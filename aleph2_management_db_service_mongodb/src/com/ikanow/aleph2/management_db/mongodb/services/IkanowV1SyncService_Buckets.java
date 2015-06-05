package com.ikanow.aleph2.management_db.mongodb.services;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;
import scala.Tuple3;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.CommonUpdateComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;

/** This service looks for changes to IKANOW sources and applies them to data bucket beans
 * @author acp
 */
public class IkanowV1SyncService_Buckets {
	private static final Logger _logger = LogManager.getLogger();	
	
	protected final MongoDbManagementDbConfigBean _config;
	protected final IServiceContext _context;
	protected final IManagementDbService _core_management_db;
	protected final IManagementDbService _underlying_management_db;
	protected final ICoreDistributedServices _core_distributed_services;
	
	protected final SetOnce<MutexMonitor> _source_mutex_monitor = new SetOnce<MutexMonitor>();
	
	protected final ScheduledExecutorService _mutex_scheduler = Executors.newScheduledThreadPool(1);		
	protected final ScheduledExecutorService _source_scheduler = Executors.newScheduledThreadPool(1);		
	protected SetOnce<ScheduledFuture<?>> _source_monitor_handle = new SetOnce<ScheduledFuture<?>>(); 
	
	protected static int _num_leader_changes = 0; // (just for debugging/testing)
	
	public final static String SOURCE_MONITOR_MUTEX = "/app/aleph2/locks/v1/sources";

	/** guice constructor
	 * @param config - the management db configuration, includes whether this service is enabled
	 * @param service_context - the service context providing all the required dependencies
	 */
	@Inject
	public IkanowV1SyncService_Buckets(final MongoDbManagementDbConfigBean config, final IServiceContext service_context) {		
		_config = config;
		_context = service_context;
		_core_management_db = _context.getCoreManagementDbService();		
		_underlying_management_db = _context.getService(IManagementDbService.class, Optional.empty()).get();
		_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		
		_core_distributed_services = _context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		
		if (Optional.ofNullable(_config.v1_enabled()).orElse(false)) {
			// Launch the synchronization service
			
			// 1) Monitor sources
			
			_source_mutex_monitor.set(new MutexMonitor(SOURCE_MONITOR_MUTEX));
			_mutex_scheduler.schedule(_source_mutex_monitor.get(), 250L, TimeUnit.MILLISECONDS);
			_source_monitor_handle.set(_source_scheduler.scheduleWithFixedDelay(new SourceMonitor(), 10L, 2L, TimeUnit.SECONDS));
				//(give it 10 seconds before starting, let everything else settle down - eg give the bucket choose handler time to register)			
		}
	}
	/** Immediately start
	 */
	public void start() {
		_source_monitor_handle.get().cancel(true);
		_source_monitor_handle.set(_source_scheduler.scheduleWithFixedDelay(new SourceMonitor(), 1, 1L, TimeUnit.SECONDS));
	}
	
	/** Stop threads (just for testing I think)
	 */
	public void stop() {
		_source_monitor_handle.get().cancel(true);
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// WORKER THREADS
	
	public class MutexMonitor implements Runnable {
		protected final String _path;
		protected final SetOnce<CuratorFramework> _curator = new SetOnce<CuratorFramework>();
		protected final SetOnce<LeaderLatch> _leader_selector = new SetOnce<LeaderLatch>();
		public MutexMonitor(final String path) {
			_path = path;
		}
		
		@Override
		public void run() {
			if (!_leader_selector.isSet()) {
				_curator.set(_core_distributed_services.getCuratorFramework());				
				try {
					final LeaderLatch Leader_latch = new LeaderLatch(_curator.get(), _path);
					Leader_latch.start();
					_leader_selector.set(Leader_latch);
				}
				catch (Exception e) {
					_logger.error(ErrorUtils.getLongForm("{0}", e));
				}
				_logger.info("SourceMonitor: joined the leadership candidate cluster");
			}			
		}
		public boolean isLeader() {
			return _leader_selector.isSet() ? _leader_selector.get().hasLeadership() : false;
		}
	}

	
	public class SourceMonitor implements Runnable {
		private final SetOnce<ICrudService<JsonNode>> _v1_db = new SetOnce<ICrudService<JsonNode>>();
		private boolean _last_state = false;
		
		/* (non-Javadoc)
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			if (!_source_mutex_monitor.get().isLeader()) {
				_last_state = false;
				return;
			}
			if (!_last_state) {
				_logger.info("SourceMonitor: now the leader");
				_num_leader_changes++;
				_last_state = true;
			}
			if (!_v1_db.isSet()) {
				@SuppressWarnings("unchecked")
				final ICrudService<JsonNode> v1_config_db = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.source")).get();				
				_v1_db.set(v1_config_db);
				
				_v1_db.get().optimizeQuery(Arrays.asList("extractType"));
			}
			
			try {
				// Synchronize
				synchronizeSources(
						_core_management_db.getDataBucketStore(), 
						_underlying_management_db.getDataBucketStatusStore(), 
						_v1_db.get())
						.get();
					// (the get at the end just ensures that you don't get two of these scheduled results colliding)
			}			
			catch (Throwable t) {
				_logger.error(ErrorUtils.getLongForm("{0}", t));
			}
		}		
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// CONTROL LOGIC
	
	/** Top level logic for source synchronization
	 * @param bucket_mgmt
	 * @param source_db
	 */
	protected CompletableFuture<?> synchronizeSources(
			final IManagementCrudService<DataBucketBean> bucket_mgmt, 
			final IManagementCrudService<DataBucketStatusBean> underlying_bucket_status_mgmt, 
			final ICrudService<JsonNode> source_db
			)
	{
		return compareSourcesToBuckets_get(bucket_mgmt, source_db)
			.thenApply(v1_v2 -> {
				return compareSourcesToBuckets_categorize(v1_v2);
			})
			.thenCompose(create_update_delete -> {
				if (create_update_delete._1().isEmpty() && create_update_delete._2().isEmpty() && create_update_delete._3().isEmpty()) {
					//(nothing to do)
					return CompletableFuture.completedFuture(null);
				}							
				_logger.info(ErrorUtils.get("Found [create={0}, delete={1}, update={2}] sources", 
						create_update_delete._1().size(),
						create_update_delete._2().size(),
						create_update_delete._3().size())
						);
				
				final List<CompletableFuture<Boolean>> l1 = 
					create_update_delete._1().stream().parallel()
						.<Tuple2<String, ManagementFuture<?>>>map(id -> 
							Tuples._2T(id, createNewBucket(id, bucket_mgmt, underlying_bucket_status_mgmt, source_db)))
						.<CompletableFuture<Boolean>>map(id_fres -> 
							updateV1SourceStatus_top(id_fres._1(), id_fres._2(), true, source_db))
						.collect(Collectors.toList());
					;
					
				final List<CompletableFuture<Boolean>> l2 = 
						create_update_delete._2().stream().parallel()
							.<Tuple2<String, ManagementFuture<?>>>map(id -> 
								Tuples._2T(id, deleteBucket(id, bucket_mgmt)))
						.<CompletableFuture<Boolean>>map(id_fres -> 
							CompletableFuture.completedFuture(true)) // (don't update source in delete case obviously)
							.collect(Collectors.toList());
						;
					
				final List<CompletableFuture<Boolean>> l3 = 
						create_update_delete._3().stream().parallel()
							.<Tuple2<String, ManagementFuture<?>>>map(id -> 
								Tuples._2T(id, updateBucket(id, bucket_mgmt, underlying_bucket_status_mgmt, source_db)))
							.<CompletableFuture<Boolean>>map(id_fres -> 
								updateV1SourceStatus_top(id_fres._1(), id_fres._2(), false, source_db))
							.collect(Collectors.toList());
						;
						
				List<CompletableFuture<?>> retval = 
						Arrays.asList(l1, l2, l3).stream().flatMap(l -> l.stream())
						.collect(Collectors.toList());
						;
						
				return CompletableFuture.allOf(retval.toArray(new CompletableFuture[0]));
			});
	}
	
	/** Top level handler for update status based on the result
	 * @param id
	 * @param fres
	 * @param disable_on_failure
	 * @param source_db
	 * @return
	 */
	protected CompletableFuture<Boolean> updateV1SourceStatus_top(final String id, 
			final ManagementFuture<?> fres, boolean disable_on_failure,
			ICrudService<JsonNode> source_db)
	{		
		return fres.getManagementResults()
				.<Boolean>thenCompose(res ->  {
					try {
						fres.get(); // (check if the DB side call has failed)
						return updateV1SourceStatus(new Date(), id, res, disable_on_failure, source_db);	
					}
					catch (Exception e) { // DB-side call has failed, create ad hoc error
						final Collection<BasicMessageBean> errs = res.isEmpty()
								? Arrays.asList(
									new BasicMessageBean(
											new Date(), 
											false, 
											"(unknown)", 
											"(unknown)", 
											null, 
											ErrorUtils.getLongForm("{0}", e), 
											null
											)
									)
								: res;
						return updateV1SourceStatus(new Date(), id, errs, disable_on_failure, source_db);											
					}
				});
	}
	
	/** Want to end up with 3 lists:
	 *  - v1 sources that don't exist in v2 (Create them)
	 *  - v2 sources that don't exist in v1 (Delete them)
	 *  - matching v1/v2 sources with different modified times (Update them)
	 * @param to_compare
	 * @returns a 3-tuple with "to create", "to delete", "to update"
	 */
	protected static Tuple3<Collection<String>, Collection<String>, Collection<String>> compareSourcesToBuckets_categorize(
			final Tuple2<Map<String, String>, Map<String, Date>> to_compare) {
		
		// Want to end up with 3 lists:
		// - v1 sources that don't exist in v2 (Create them)
		// - v2 sources that don't exist in v1 (Delete them)
		// - matching v1/v2 sources with different modified times (Update them)

		// (do delete first, then going to filter to_compare._1() on value==null)		
		final Set<String> v2_not_v1 = new HashSet<String>(to_compare._2().keySet());
		v2_not_v1.removeAll(to_compare._1().keySet());
		
		// OK not worried about deletes any more, not interested in isApproved:false
		
		final Set<String> to_compare_approved = 
				to_compare._1().entrySet().stream()
					.filter(kv -> null != kv.getValue() && !kv.getValue().isEmpty()).map(kv -> kv.getKey())
					.collect(Collectors.toSet());
		
		final Set<String> v1_and_v2 = new HashSet<String>(to_compare_approved);
		v1_and_v2.retainAll(to_compare._2().keySet());
		
		final List<String> v1_and_v2_mod = v1_and_v2.stream()
							.filter(id -> {
								try {
									final Date v1_date = parseJavaDate(to_compare._1().get(id));
									final Date v2_date = to_compare._2().get(id);
									return v1_date.getTime() > v2_date.getTime();
								}
								catch (Exception e) {
									return false; // (just ignore)
								}
							})
							.collect(Collectors.toList());
		
		final Set<String> v1_not_v2 = new HashSet<String>(to_compare_approved);
		v1_not_v2.removeAll(to_compare._2().keySet());
		
		return Tuples._3T(v1_not_v2, v2_not_v1, v1_and_v2_mod);
	}

	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// DB MANIPULATION - READ
	
	/** Gets a list of _id,modified from v1 and a list matching _id,modified from V2
	 * @param bucket_mgmt
	 * @param source_db
	 * @return tuple of id-vs-(date-or-null-if-not-approved) for v1, id-vs-date for v2
	 */
	protected static 
	CompletableFuture<Tuple2<Map<String, String>, Map<String, Date>>> compareSourcesToBuckets_get(
		final IManagementCrudService<DataBucketBean> bucket_mgmt, 
		final ICrudService<JsonNode> source_db)
	{
		// (could make this more efficient by having a regular "did something happen" query with a slower "get everything and resync)
		// (don't forget to add "modified" to the compund index though)
		CompletableFuture<Cursor<JsonNode>> f_v1_sources = 
				source_db.getObjectsBySpec(
						CrudUtils.allOf().when("extractType", "V2DataBucket"),
						Arrays.asList("key", "modified", "isApproved"), true);
		
		return f_v1_sources
			.<Map<String, String>>thenApply(v1_sources -> {
				return StreamSupport.stream(v1_sources.spliterator(), false)
					.collect(Collectors.toMap(
							j -> safeJsonGet("key", j).asText(),
							j -> safeJsonGet("isApproved", j).asBoolean() ? safeJsonGet("modified", j).asText() : ""
							));
			})
			.<Tuple2<Map<String, String>, Map<String, Date>>>
			thenCompose(v1_id_datestr_map -> {
				final SingleQueryComponent<DataBucketBean> bucket_query = CrudUtils.allOf(DataBucketBean.class)
						.rangeIn(DataBucketBean::_id, "aleph...bucket.", true, "aleph...bucket/", true)
						;						
				
				return bucket_mgmt.getObjectsBySpec(bucket_query, Arrays.asList("_id", "modified"), true)
						.<Tuple2<Map<String, String>, Map<String, Date>>>
						thenApply(c -> {							
							final Map<String, Date> v2_id_date_map = 
									StreamSupport.stream(c.spliterator(), false)
									.collect(Collectors.toMap(
											b -> b._id(),
											b -> b.modified()
											));
							
							return Tuples._2T(v1_id_datestr_map, v2_id_date_map);
						});
			});
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// DB MANIPULATION - WRITE
	
	/** Create a new bucket
	 * @param id
	 * @param bucket_mgmt
	 * @param bucket_status_mgmt
	 * @param source_db
	 * @return
	 */
	protected static ManagementFuture<Supplier<Object>> createNewBucket(final String id,
			final IManagementCrudService<DataBucketBean> bucket_mgmt, 
			final IManagementCrudService<DataBucketStatusBean> underlying_bucket_status_mgmt, 
			final ICrudService<JsonNode> source_db			
			)
	{
		_logger.info(ErrorUtils.get("Found new source {0}, creating bucket", id));
		
		// Create a status bean:

		final SingleQueryComponent<JsonNode> v1_query = CrudUtils.allOf().when("key", id);
		return FutureUtils.denestManagementFuture(source_db.getObjectBySpec(v1_query)
			.<ManagementFuture<Supplier<Object>>>thenApply(jsonopt -> {
				try {
					
					final DataBucketBean new_object = getBucketFromV1Source(jsonopt.get());
					final boolean is_now_suspended = safeJsonGet("searchCycle_secs", jsonopt.get()).asInt(1) < 0;
					
					final DataBucketStatusBean status_bean = BeanTemplateUtils.build(DataBucketStatusBean.class)
							.with(DataBucketStatusBean::_id, id)
							.with(DataBucketStatusBean::bucket_path, new_object.full_name())
							.with(DataBucketStatusBean::suspended, is_now_suspended) 
							.done().get();

					return FutureUtils.denestManagementFuture(underlying_bucket_status_mgmt.storeObject(status_bean, true)
						.thenApply(__ -> {
							final ManagementFuture<Supplier<Object>> ret = bucket_mgmt.storeObject(new_object);
							return ret;
						}));
				}			
				catch (Exception e) {
					return FutureUtils.<Supplier<Object>>createManagementFuture(
							FutureUtils.returnError(e), 
							CompletableFuture.completedFuture(Arrays.asList(new BasicMessageBean(
									new Date(),
									false, 
									"IkanowV1SyncService_Buckets", 
									"createNewBucket", 
									null, 
									ErrorUtils.getLongForm("{0}", e), 
									null
									)
									))
							);
				}				
			}))
			;
	}

	/** Delete a bucket
	 * @param id
	 * @param bucket_mgmt
	 * @return
	 */
	protected static ManagementFuture<Boolean> deleteBucket(final String id,
			final IManagementCrudService<DataBucketBean> bucket_mgmt)
	{
		_logger.info(ErrorUtils.get("Source {0} was deleted, deleting bucket", id));
		
		return bucket_mgmt.deleteObjectById(id);
	}
	
	/** Update a bucket based on a new V1 source
	 * @param id
	 * @param bucket_mgmt
	 * @param underlying_bucket_status_mgmt
	 * @param source_db
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected static ManagementFuture<Supplier<Object>> updateBucket(final String id,
			final IManagementCrudService<DataBucketBean> bucket_mgmt, 
			final IManagementCrudService<DataBucketStatusBean> underlying_bucket_status_mgmt, 
			final ICrudService<JsonNode> source_db)
	{
		_logger.info(ErrorUtils.get("Source {0} was modified, updating bucket", id));
		
		// Get the full source from V1
		// .. and from V2: the existing bucket and the existing status
		
		// OK first off, we're immediately going to update the bucket's modified time
		// since otherwise if the update fails then we'll get stuck updating it every iteration...
		// (ie this is the reason we set isApproved:false in the create case)
		//(this ugliness just handles the test case already running on the underlying service)
		final ICrudService<DataBucketBean> underlying_bucket_db = 
				bucket_mgmt.getUnderlyingPlatformDriver(ICrudService.class, Optional.empty()).orElse(bucket_mgmt);
		
		underlying_bucket_db.updateObjectById(id, CrudUtils.update(DataBucketBean.class).set(DataBucketBean::modified, new Date()));		
		
		final SingleQueryComponent<JsonNode> v1_query = CrudUtils.allOf().when("key", id);
		final CompletableFuture<Optional<JsonNode>> f_v1_source = source_db.getObjectBySpec(v1_query);		

		return FutureUtils.denestManagementFuture(f_v1_source.<ManagementFuture<Supplier<Object>>>thenApply(v1_source -> {			
			try {												
				// Once we have all the queries back, get some more information
				final boolean is_now_suspended = safeJsonGet("searchCycle_secs", v1_source.get()).asInt(1) < 0;
				final DataBucketBean new_object = getBucketFromV1Source(v1_source.get());

				// (Update status in underlying status store so don't trip a spurious harvest call)
				CompletableFuture<?> update = underlying_bucket_status_mgmt.updateObjectById(id, CrudUtils.update(DataBucketStatusBean.class)
																		.set(DataBucketStatusBean::suspended, is_now_suspended));

				// Then update the management db
				
				return FutureUtils.denestManagementFuture(update.thenApply(__ -> bucket_mgmt.storeObject(new_object, true)));
			}
			catch (Exception e) {
				return FutureUtils.<Supplier<Object>>createManagementFuture(
						FutureUtils.returnError(e), 
						CompletableFuture.completedFuture(Arrays.asList(new BasicMessageBean(
								new Date(),
								false, 
								"IkanowV1SyncService_Buckets", 
								"updateBucket", 
								null, 
								ErrorUtils.getLongForm("{0}", e), 
								null
								)
								))
						);
			}
		}));
	}	
	
	/** Takes a collection of results from the management side-channel, and uses it to update a harvest node
	 * @param key - source key / bucket id
	 * @param status_messages
	 * @param source_db
	 */
	protected static CompletableFuture<Boolean> updateV1SourceStatus(
			final Date main_date,
			final String key,
			final Collection<BasicMessageBean> status_messages,
			final boolean set_approved_state,
			final ICrudService<JsonNode> source_db
			)
	{
		final String message_block = status_messages.stream()
			.map(msg -> {
				return "[" + msg.date() + "] " + msg.source() + " (" + msg.command() + "): " + (msg.success() ? "INFO" : "ERROR") + ": " + msg.message();
			})
			.collect(Collectors.joining("\n"))
			;
		
		final boolean any_errors = status_messages.stream().anyMatch(msg -> !msg.success());
		
		@SuppressWarnings("deprecation")
		final CommonUpdateComponent<JsonNode> update_1 = CrudUtils.update()				
				.set("harvest.harvest_status", (any_errors ? "error" : "success"))
				.set("harvest.harvest_message",
						"[" + main_date.toGMTString() + "] Bucket synchronization:\n" 
						+ (message_block.isEmpty() ? "(no messages)" : message_block));
		
		final UpdateComponent<JsonNode> update = set_approved_state 
				? update_1.set("isApproved", !any_errors)
				: update_1;
		
		final SingleQueryComponent<JsonNode> v1_query = CrudUtils.allOf().when("key", key);
		final CompletableFuture<Boolean> update_res = source_db.updateObjectBySpec(v1_query, Optional.empty(), update);
		
		return update_res;
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// LOW LEVEL UTILS
	
	/** Builds a V2 bucket out of a V1 source
	 * @param src_json
	 * @return
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 * @throws ParseException
	 */
	protected static DataBucketBean getBucketFromV1Source(final JsonNode src_json) throws JsonParseException, JsonMappingException, IOException, ParseException {
		@SuppressWarnings("unused")
		final String _id = safeJsonGet("_id", src_json).asText(); // (think we'll use key instead of _id?)
		final String key = safeJsonGet("key", src_json).asText();
		final String created = safeJsonGet("created", src_json).asText();
		final String modified = safeJsonGet("modified", src_json).asText();
		final String title = safeJsonGet("title", src_json).asText();
		final String description = safeJsonGet("description", src_json).asText();
		final String owner_id = safeJsonGet("ownerId", src_json).asText();
		
		final JsonNode tags = safeJsonGet("tags", src_json); // collection of strings
		final JsonNode comm_ids = safeJsonGet("communityIds", src_json); // collection of strings
		final JsonNode px_pipeline = safeJsonGet("processingPipeline", src_json); // collection of JSON objects, first one should have data_bucket
		final JsonNode px_pipeline_first_el = px_pipeline.get(0);
		final JsonNode data_bucket = safeJsonGet("data_bucket", px_pipeline_first_el);
		
		final DataBucketBean bucket = BeanTemplateUtils.build(data_bucket, DataBucketBean.class)
													.with(DataBucketBean::_id, key)
													.with(DataBucketBean::created, parseJavaDate(created))
													.with(DataBucketBean::modified, parseJavaDate(modified))
													.with(DataBucketBean::display_name, title)
													.with(DataBucketBean::description, description)
													.with(DataBucketBean::owner_id, owner_id)
													.with(DataBucketBean::access_rights,
															new AuthorizationBean(
																	StreamSupport.stream(comm_ids.spliterator(), false)
																		.collect(Collectors.toMap(id -> id.asText(), __ -> "rw"))
																	)
															)
													.with(DataBucketBean::tags, 
															StreamSupport.stream(tags.spliterator(), false)
																			.map(jt -> jt.asText())
																			.collect(Collectors.toSet()))																	
													.done().get();
		
		return bucket;
		
	}
	
	/** Gets a JSON field that may not be present (justs an empty JsonNode if no)
	 * @param fieldname
	 * @param src
	 * @return
	 */
	protected static JsonNode safeJsonGet(String fieldname, JsonNode src) {
		final JsonNode j = Optional.ofNullable(src.get(fieldname)).orElse(JsonNodeFactory.instance.objectNode());
		//DEBUG
		//System.out.println(j);
		return j;
	}
	/** Quick utility to parse the result of Date::toString back into a date
	 * @param java_date_tostring_format
	 * @return
	 * @throws ParseException
	 */
	protected static Date parseJavaDate(String java_date_tostring_format) throws ParseException {
		try {
			return new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy").parse(java_date_tostring_format);
		}
		catch (Exception e) {			
			try {
				return new SimpleDateFormat("MMM d, yyyy hh:mm:ss a zzz").parse(java_date_tostring_format);
			}
			catch (Exception ee) {
				return new SimpleDateFormat("d MMM yyyy HH:mm:ss zzz").parse(java_date_tostring_format);				
			}
		}
	}
	
}
