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
import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

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
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;

/** This service looks for changes to IKANOW sources that  
 * @author acp
 */
public class IkanowV1SyncService {

	public static final Logger _logger = Logger.getLogger(IkanowV1SyncService.class);		
	
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
	public IkanowV1SyncService(final MongoDbManagementDbConfigBean config, final IServiceContext service_context) {		
		_config = config;
		_context = service_context;
		_core_management_db = _context.getCoreManagementDbService();
		_underlying_management_db = _context.getService(IManagementDbService.class, Optional.empty());
		_core_distributed_services = _context.getService(ICoreDistributedServices.class, Optional.empty());
		
		if (Optional.ofNullable(_config.v1_enabled()).orElse(false)) {
			// Launch the synchronization service
			
			// 1) Monitor sources
			
			_source_mutex_monitor.set(new MutexMonitor(SOURCE_MONITOR_MUTEX));
			_mutex_scheduler.schedule(_source_mutex_monitor.get(), 250L, TimeUnit.MILLISECONDS);
			_source_monitor_handle.set(_source_scheduler.scheduleWithFixedDelay(new SourceMonitor(), 1L, 1L, TimeUnit.SECONDS));
			
			// 2) Monitor files
			
			//TODO (ALEPH-19)
		}
	}
	public class MutexMonitor implements Runnable {
		protected final String _path;
		protected final SetOnce<CuratorFramework> _curator = new SetOnce<CuratorFramework>();
		protected final SetOnce<LeaderLatch> _leader_selector = new SetOnce<LeaderLatch>();
		public MutexMonitor(final @NonNull String path) {
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
				final ICrudService<JsonNode> v1_config_db = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.source"));				
				_v1_db.set(v1_config_db);
				
				_v1_db.get().optimizeQuery(Arrays.asList("extractType"));
			}
			
			// Grab the v2 sources
			
			compareSourcesToBuckets_get(_underlying_management_db.getDataBucketStore(), _v1_db.get());
			//TODO (do a big get at the end to resolve it all!)
		}		
	}
	
	/** Gets a list of _id,modified from v1 and a list matching _id,modified from V2
	 * @param bucket_mgmt
	 * @param source_db
	 * @return
	 */
	protected static 
	CompletableFuture<Tuple2<Map<String, String>, Map<String, Date>>> compareSourcesToBuckets_get(
		IManagementCrudService<DataBucketBean> bucket_mgmt, 
			ICrudService<JsonNode> source_db)
	{
		// (could make this more efficient by having a regular "did something happen" query with a slower "get everything and resync)
		// (don't forget to add "modified" to the compund index though)
		CompletableFuture<Cursor<JsonNode>> f_v1_sources = 
				source_db.getObjectsBySpec(CrudUtils.anyOf().when("extractType", "V2DataBucket"), Arrays.asList("_id", "modified"), true);
		
		return f_v1_sources
			.<Map<String, String>>thenApply(v1_sources -> {
				return StreamSupport.stream(v1_sources.spliterator(), false)
					.collect(Collectors.toMap(
							j -> safeJsonGet("_id", j).asText(),
							j -> safeJsonGet("modified", j).asText()
							));
			})
			.<Tuple2<Map<String, String>, Map<String, Date>>>
			thenCompose(v1_id_datestr_map -> {
				final QueryComponent<DataBucketBean> bucket_query = CrudUtils.anyOf(DataBucketBean.class)
						.withAny(DataBucketBean::_id, v1_id_datestr_map.keySet().stream().collect(Collectors.toList()));
				
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
	
	protected static void createNewBucket(final @NonNull String id,
			final @NonNull IManagementCrudService<DataBucketBean> bucket_mgmt, 
			final @NonNull IManagementCrudService<DataBucketStatusBean> bucket_status_mgmt, 
			final @NonNull ICrudService<JsonNode> source_db
			
			)
	{
		//TODO		
	}

	protected static void deleteBuckets(final @NonNull Collection<String> ids,
			final @NonNull IManagementCrudService<DataBucketBean> bucket_mgmt)
	{
		//TODO
		
	}
	
	protected static void updateBucket(final @NonNull String id,
			final @NonNull IManagementCrudService<DataBucketBean> bucket_mgmt, 
			final @NonNull IManagementCrudService<DataBucketStatusBean> bucket_status_mgmt, 
			final @NonNull ICrudService<JsonNode> source_db)
	{
		//TODO ugh need to handle suspend/resume
		//TODO
	}	
	
	/** Want to end up with 3 lists:
	 *  - v1 sources that don't exist in v2 (Create them)
	 *  - v2 sources that don't exist in v1 (Delete them)
	 *  - matching v1/v2 sources with different modified times (Update them)
	 * @param to_compare
	 * @returns a 3-tuple with "to create", "to delete", "to update"
	 */
	protected static Tuple3<Collection<String>, Collection<String>, Collection<String>> compareSourcesToBuckets_categorize(
			final @NonNull Tuple2<Map<String, String>, Map<String, Date>> to_compare) {
		
		// Want to end up with 3 lists:
		// - v1 sources that don't exist in v2 (Create them)
		// - v2 sources that don't exist in v1 (Delete them)
		// - matching v1/v2 sources with different modified times (Update them)
		
		final Set<String> v1_and_v2 = new HashSet<String>(to_compare._1().keySet());
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
		
		final Set<String> v1_not_v2 = new HashSet<String>(to_compare._1().keySet());
		v1_not_v2.removeAll(to_compare._2().keySet());
		
		final Set<String> v2_not_v1 = new HashSet<String>(to_compare._2().keySet());
		v2_not_v1.removeAll(to_compare._1().keySet());
		
		return Tuples._3T(v1_not_v2, v2_not_v1, v1_and_v2_mod);
	}

	/** Builds a V2 bucket out of a V1 source
	 * @param src_json
	 * @return
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 * @throws ParseException
	 */
	protected static DataBucketBean getBucketFromV1Source(final @NonNull JsonNode src_json) throws JsonParseException, JsonMappingException, IOException, ParseException {
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
	private static JsonNode safeJsonGet(String fieldname, JsonNode src) {
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
	private static Date parseJavaDate(String java_date_tostring_format) throws ParseException {
		return new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy").parse(java_date_tostring_format);
	}
	
}
