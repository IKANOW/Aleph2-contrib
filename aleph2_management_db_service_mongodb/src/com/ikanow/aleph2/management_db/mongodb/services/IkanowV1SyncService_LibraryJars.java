package com.ikanow.aleph2.management_db.mongodb.services;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
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
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import scala.Tuple2;
import scala.Tuple3;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean.LibraryType;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.CommonUpdateComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

/** This service looks for changes to IKANOW binary shares and applies them to shared library beans
 * @author acp
 */
public class IkanowV1SyncService_LibraryJars {
	private static final Logger _logger = LogManager.getLogger();	
	
	protected final MongoDbManagementDbConfigBean _config;
	protected final IServiceContext _context;
	protected final IManagementDbService _core_management_db;
	protected final IManagementDbService _underlying_management_db;
	protected final ICoreDistributedServices _core_distributed_services;
	protected final IStorageService _storage_service;
	protected SetOnce<GridFS> _mongodb_distributed_fs = new SetOnce<GridFS>();
	
	protected final SetOnce<MutexMonitor> _library_mutex_monitor = new SetOnce<MutexMonitor>();
	
	protected final ScheduledExecutorService _mutex_scheduler = Executors.newScheduledThreadPool(1);		
	protected final ScheduledExecutorService _source_scheduler = Executors.newScheduledThreadPool(1);		
	protected SetOnce<ScheduledFuture<?>> _library_monitor_handle = new SetOnce<ScheduledFuture<?>>(); 
	
	protected static int _num_leader_changes = 0; // (just for debugging/testing)
	
	public final static String LIBRARY_MONITOR_MUTEX = "/app/aleph2/locks/v1/library_jars";

	/** guice constructor
	 * @param config - the management db configuration, includes whether this service is enabled
	 * @param service_context - the service context providing all the required dependencies
	 */
	@Inject
	public IkanowV1SyncService_LibraryJars(final MongoDbManagementDbConfigBean config, final IServiceContext service_context) {		
		_config = config;
		_context = service_context;
		_core_management_db = _context.getCoreManagementDbService();
		_underlying_management_db = _context.getService(IManagementDbService.class, Optional.empty());
		_core_distributed_services = _context.getService(ICoreDistributedServices.class, Optional.empty());
		_storage_service = _context.getStorageService();
		
		if (Optional.ofNullable(_config.v1_enabled()).orElse(false)) {
			// Launch the synchronization service
			
			// 1) Monitor sources
			
			_library_mutex_monitor.set(new MutexMonitor(LIBRARY_MONITOR_MUTEX));
			_mutex_scheduler.schedule(_library_mutex_monitor.get(), 250L, TimeUnit.MILLISECONDS);
			_library_monitor_handle.set(_source_scheduler.scheduleWithFixedDelay(new LibraryMonitor(), 10L, 2L, TimeUnit.SECONDS));
				//(give it 10 seconds before starting, let everything else settle down - eg give the bucket choose handler time to register)
		}
	}
	/** Immediately start
	 */
	public void start() {
		_library_monitor_handle.get().cancel(true);
		_library_monitor_handle.set(_source_scheduler.scheduleWithFixedDelay(new LibraryMonitor(), 1, 1L, TimeUnit.SECONDS));
	}
	
	/** Stop threads (just for testing I think)
	 */
	public void stop() {
		_library_monitor_handle.get().cancel(true);
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
				_logger.info("LibraryMonitor: joined the leadership candidate cluster");
			}			
		}
		public boolean isLeader() {
			return _leader_selector.isSet() ? _leader_selector.get().hasLeadership() : false;
		}
	}

	
	public class LibraryMonitor implements Runnable {
		private final SetOnce<ICrudService<JsonNode>> _v1_db = new SetOnce<ICrudService<JsonNode>>();
		private boolean _last_state = false;
		
		/* (non-Javadoc)
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			if (!_library_mutex_monitor.get().isLeader()) {
				_last_state = false;
				return;
			}
			if (!_last_state) {
				_logger.info("LibraryMonitor: now the leader");
				_num_leader_changes++;
				_last_state = true;
			}
			if (!_v1_db.isSet()) {
				@SuppressWarnings("unchecked")
				final ICrudService<JsonNode> v1_config_db = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("social.share"));				
				_v1_db.set(v1_config_db);				
				_v1_db.get().optimizeQuery(Arrays.asList("title"));
			}
			if (!_mongodb_distributed_fs.isSet()) {
				final GridFS fs = _underlying_management_db.getUnderlyingPlatformDriver(GridFS.class, Optional.of("file.binary_shares"));
				_mongodb_distributed_fs.set(fs);
			}
			
			try {
				// Synchronize
				synchronizeLibraryJars(
						_core_management_db.getSharedLibraryStore(), 
						_storage_service,
						_v1_db.get(),
						_mongodb_distributed_fs.get())
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
	 * @param library_mgmt
	 * @param share_db
	 */
	protected CompletableFuture<?> synchronizeLibraryJars(
			final IManagementCrudService<SharedLibraryBean> library_mgmt,
			final IStorageService aleph2_fs,
			final ICrudService<JsonNode> share_db,
			final GridFS share_fs
			)
	{
		return compareJarsToLibaryBeans_get(library_mgmt, share_db)
			.thenApply(v1_v2 -> {
				return compareJarsToLibraryBeans_categorize(v1_v2);
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
							Tuples._2T(id, createLibraryBean(id, library_mgmt, aleph2_fs, true, share_db, share_fs)))
						.<CompletableFuture<Boolean>>map(id_fres -> 
							updateV1ShareErrorStatus_top(id_fres._1(), id_fres._2(), share_db))
						.collect(Collectors.toList());
					;
					
				final List<CompletableFuture<Boolean>> l2 = 
						create_update_delete._2().stream().parallel()
							.<Tuple2<String, ManagementFuture<?>>>map(id -> 
								Tuples._2T(id, deleteLibraryBean(id, library_mgmt)))
						.<CompletableFuture<Boolean>>map(id_fres -> 
							CompletableFuture.completedFuture(true)) 
							.collect(Collectors.toList());
						;
					
				final List<CompletableFuture<Boolean>> l3 = 
						create_update_delete._3().stream().parallel()
							.<Tuple2<String, ManagementFuture<?>>>map(id -> 
								Tuples._2T(id, createLibraryBean(id, library_mgmt, aleph2_fs, false, share_db, share_fs)))
							.<CompletableFuture<Boolean>>map(id_fres -> 
								CompletableFuture.completedFuture(true)) 
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
	 * @param share_db
	 * @return
	 */
	protected CompletableFuture<Boolean> updateV1ShareErrorStatus_top(final String id, 
			final ManagementFuture<?> fres, 
			ICrudService<JsonNode> share_db)
	{		
		return fres.getManagementResults()
				.<Boolean>thenCompose(res ->  {
					try {
						fres.get(); // (check if the DB side call has failed)
						return updateV1ShareErrorStatus(new Date(), id, res, share_db);	
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
						return updateV1ShareErrorStatus(new Date(), id, errs, share_db);											
					}
				});
	}
	
	/** Want to end up with 3 lists:
	 *  - v1 objects that don't exist in v2 (Create them)
	 *  - v2 objects that don't exist in v1 (Delete them)
	 *  - matching v1/v2 objects with different modified times (Update them)
	 * @param to_compare
	 * @returns a 3-tuple with "to create", "to delete", "to update" - NOTE: none of the _ids here include the "v1_"
	 */
	protected static Tuple3<Collection<String>, Collection<String>, Collection<String>> compareJarsToLibraryBeans_categorize(
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
	 * @param library_mgmt
	 * @param share_db
	 * @return tuple of id-vs-(date-or-null-if-not-approved) for v1, id-vs-date for v2
	 */
	protected static 
	CompletableFuture<Tuple2<Map<String, String>, Map<String, Date>>> compareJarsToLibaryBeans_get(
		final IManagementCrudService<SharedLibraryBean> library_mgmt, 
		final ICrudService<JsonNode> share_db)
	{
		// (could make this more efficient by having a regular "did something happen" query with a slower "get everything and resync)
		// (don't forget to add "modified" to the compound index though)
		CompletableFuture<Cursor<JsonNode>> f_v1_jars = 
				share_db.getObjectsBySpec(
						CrudUtils.allOf().when("type", "binary")
							.rangeIn("title", "/app/aleph2/library/", true, "/app/aleph2/library0", true),
							Arrays.asList("_id", "modified"), true
						);
		
		return f_v1_jars
			.<Map<String, String>>thenApply(v1_jars -> {
				return StreamSupport.stream(v1_jars.spliterator(), false)
					.collect(Collectors.toMap(
							j -> safeJsonGet("_id", j).asText(),
							j -> safeJsonGet("modified", j).asText()
							));
			})
			.<Tuple2<Map<String, String>, Map<String, Date>>>
			thenCompose(v1_id_datestr_map -> {
				final SingleQueryComponent<SharedLibraryBean> library_query = CrudUtils.allOf(SharedLibraryBean.class)
						.rangeIn(SharedLibraryBean::_id, "v1_", true, "v1a", true)
						;						
				
				return library_mgmt.getObjectsBySpec(library_query, Arrays.asList("_id", "modified"), true)
						.<Tuple2<Map<String, String>, Map<String, Date>>>
						thenApply(c -> {							
							final Map<String, Date> v2_id_date_map = 
									StreamSupport.stream(c.spliterator(), false)
									.collect(Collectors.toMap(
											b -> b._id().substring(3), //(ie remove the "v1_")
											b -> b.modified()
											));
							
							return Tuples._2T(v1_id_datestr_map, v2_id_date_map);
						});
			});
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// FS - WRITE
	
	protected static void copyFile(final String binary_id, final String path, 
			final IStorageService aleph2_fs,
			final GridFS share_fs
			) throws IOException
	{
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			final GridFSDBFile file = share_fs.find(new ObjectId(binary_id));						
			file.writeTo(out);		
			final FileContext fs = aleph2_fs.getUnderlyingPlatformDriver(FileContext.class, Optional.empty());
			final Path file_path = fs.makeQualified(new Path(path));
			try (FSDataOutputStream outer = fs.create(file_path, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), 
					org.apache.hadoop.fs.Options.CreateOpts.createParent()))
			{
				outer.write(out.toByteArray());
			}
		}		
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// DB MANIPULATION - WRITE
	
	/** Create a new library bean
	 * @param id
	 * @param bucket_mgmt
	 * @param create_not_update - true if create, false if update
	 * @param share_db
	 * @return
	 */
	protected static ManagementFuture<Supplier<Object>> createLibraryBean(final String id,
			final IManagementCrudService<SharedLibraryBean> library_mgmt, 
			final IStorageService aleph2_fs,
			final boolean create_not_update,
			final ICrudService<JsonNode> share_db,
			final GridFS share_fs
			)
	{
		if (create_not_update) {
			_logger.info(ErrorUtils.get("Found new share {0}, creating library bean", id));
		}
		else {
			_logger.info(ErrorUtils.get("Share {0} was modified, updating library bean", id));
		}
		
		// Create a status bean:

		final SingleQueryComponent<JsonNode> v1_query = CrudUtils.allOf().when("_id", new ObjectId(id));
		return FutureUtils.denestManagementFuture(share_db.getObjectBySpec(v1_query)
			.<ManagementFuture<Supplier<Object>>>thenApply(jsonopt -> {
				try {
					final SharedLibraryBean new_object = getLibraryBeanFromV1Share(jsonopt.get());
					
					// Try to copy the file across before going crazy (going to leave this as single threaded for now, we'll live)
					final String binary_id = safeJsonGet("binaryId", jsonopt.get()).asText();					
					copyFile(binary_id, new_object.path_name(), aleph2_fs, share_fs);
					
					final ManagementFuture<Supplier<Object>> ret = library_mgmt.storeObject(new_object, !create_not_update);
					return ret;
				}			
				catch (Exception e) {
					return FutureUtils.<Supplier<Object>>createManagementFuture(
							FutureUtils.returnError(e), 
							CompletableFuture.completedFuture(Arrays.asList(new BasicMessageBean(
									new Date(),
									false, 
									"IkanowV1SyncService_LibraryJars", 
									"createLibraryBean", 
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

	/** Delete a library bean
	 * @param id
	 * @param library_mgmt
	 * @return
	 */
	protected static ManagementFuture<Boolean> deleteLibraryBean(final String id,
			final IManagementCrudService<SharedLibraryBean> library_mgmt)
	{
		_logger.info(ErrorUtils.get("Share {0} was deleted, deleting libary bean", id));

		//TODO: make it delete the JAR file in deleteObjectById
		
		return library_mgmt.deleteObjectById("v1_" + id);
	}
	
	/** Takes a collection of results from the management side-channel, and uses it to update a harvest node
	 * @param key - source key / bucket id
	 * @param status_messages
	 * @param source_db
	 */
	protected static CompletableFuture<Boolean> updateV1ShareErrorStatus(
			final Date main_date,
			final String id,
			final Collection<BasicMessageBean> status_messages,
			final ICrudService<JsonNode> share_db
			)
	{
		final String message_block = status_messages.stream()
			.map(msg -> {
				return "[" + msg.date() + "] " + msg.source() + " (" + msg.command() + "): " + (msg.success() ? "INFO" : "ERROR") + ": " + msg.message();
			})
			.collect(Collectors.joining("\n"))
			;
		
		final boolean any_errors = status_messages.stream().anyMatch(msg -> !msg.success());
		
		// Only going to do something if we have errors:
		
		if (any_errors) {
			return share_db.getObjectById(id, Arrays.asList("title", "description"), true).thenCompose(jsonopt -> {
				if (jsonopt.isPresent()) { // (else share has vanished, nothing to do)
					final CommonUpdateComponent<JsonNode> update = CrudUtils.update()
							.set("title", "ERROR:" + safeJsonGet("title", jsonopt.get()))
							.set("description", safeJsonGet("title", jsonopt.get()) + "\n" + message_block)
							;
					final SingleQueryComponent<JsonNode> v1_query = CrudUtils.allOf().when("_id", new ObjectId(id));					
					final CompletableFuture<Boolean> update_res = share_db.updateObjectBySpec(v1_query, Optional.empty(), update);
					return update_res;
				}
				else {
					return CompletableFuture.completedFuture(false);
				}
			});
		}
		else {
			return CompletableFuture.completedFuture(false);			
		}
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// LOW LEVEL UTILS
	
	/** Builds a V2 library bean out of a V1 share
	 * @param src_json
	 * @return
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 * @throws ParseException
	 */
	protected static SharedLibraryBean getLibraryBeanFromV1Share(final JsonNode src_json) throws JsonParseException, JsonMappingException, IOException, ParseException {

		final String[] description_lines = Optional.ofNullable(safeJsonGet("description", src_json).asText())
											.orElse("unknown").split("\r\n?|\n");
		
		final String _id = "v1_" + safeJsonGet("_id", src_json).asText(); // (think we'll use key instead of _id?)
		final String created = safeJsonGet("created", src_json).asText();
		final String modified = safeJsonGet("modified", src_json).asText();
		final String display_name = safeJsonGet("title", src_json).asText();
		final String path_name = display_name;
		final String description = Arrays.asList(description_lines).stream().skip(1).collect(Collectors.joining("\n"));
		final LibraryType type = LibraryType.misc_archive;
		final String owner_id = safeJsonGet("_id", safeJsonGet("owner", src_json)).asText();
		final Set<String> tags = description_lines[description_lines.length - 1].substring(0, 5).toLowerCase().startsWith("tags:")
								? new HashSet<String>(Arrays.asList(
										description_lines[description_lines.length - 1]
												.replaceFirst("(?i)tags:\\s*", "")
												.split("\\s*,\\s*")))
								: Collections.emptySet(); 
		final JsonNode comm_objs = safeJsonGet("communities", src_json); // collection of { _id: $oid } types
		final String misc_entry_point = description_lines[0];
		
		final SharedLibraryBean bean = BeanTemplateUtils.build(SharedLibraryBean.class)
													.with(SharedLibraryBean::_id, _id)
													.with(SharedLibraryBean::created, parseJavaDate(created))
													.with(SharedLibraryBean::modified, parseJavaDate(modified))
													.with(SharedLibraryBean::display_name, display_name)
													.with(SharedLibraryBean::path_name, path_name)
													.with(SharedLibraryBean::description, description)
													.with(SharedLibraryBean::tags, tags)
													.with(SharedLibraryBean::type, type)
													.with(SharedLibraryBean::misc_entry_point, misc_entry_point)
													.with(SharedLibraryBean::owner_id, owner_id)
													.with(SharedLibraryBean::access_rights,
															new AuthorizationBean(
																	StreamSupport.stream(comm_objs.spliterator(), false)
																		.collect(Collectors.toMap(obj -> safeJsonGet("_id", obj).asText(), __ -> "rw"))
																	)
															)
													.done().get();
		
		return bean;		
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
