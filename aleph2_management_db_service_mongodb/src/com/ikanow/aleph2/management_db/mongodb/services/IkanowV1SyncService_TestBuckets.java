/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.ikanow.aleph2.management_db.mongodb.services;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.BeanUpdateComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.data_model.TestQueueBean;
import com.ikanow.aleph2.management_db.mongodb.module.MongoDbManagementDbModule.BucketTestService;

/** This service looks for changes to IKANOW test db entries and kicks off test jobs
 * @author cmb
 */
public class IkanowV1SyncService_TestBuckets {
	private static final Logger _logger = LogManager.getLogger();
	protected static final ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	protected final MongoDbManagementDbConfigBean _config;
	protected final IServiceContext _context;
	protected final IManagementDbService _core_management_db;
	protected final IManagementDbService _underlying_management_db;
	protected final ICoreDistributedServices _core_distributed_services;
	
	protected final SetOnce<MutexMonitor> _source_test_mutex_monitor = new SetOnce<MutexMonitor>();
	
	protected final ScheduledExecutorService _mutex_scheduler = Executors.newScheduledThreadPool(1);		
	protected final ScheduledExecutorService _source_test_scheduler = Executors.newScheduledThreadPool(1);		
	protected SetOnce<ScheduledFuture<?>> _source_test_monitor_handle = new SetOnce<ScheduledFuture<?>>(); 
	
	protected static int _num_leader_changes = 0; // (just for debugging/testing)
	
	public final static String SOURCE_TEST_MONITOR_MUTEX = "/app/aleph2/locks/v1/sources_test";

	/** guice constructor
	 * @param config - the management db configuration, includes whether this service is enabled
	 * @param service_context - the service context providing all the required dependencies
	 */
	@Inject
	public IkanowV1SyncService_TestBuckets(final MongoDbManagementDbConfigBean config, final IServiceContext service_context) {		
		_config = config;
		_context = service_context;
		_core_management_db = _context.getCoreManagementDbService();		
		_underlying_management_db = _context.getService(IManagementDbService.class, Optional.empty()).get();
		_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		_core_distributed_services = _context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		
		if (Optional.ofNullable(_config.v1_enabled()).orElse(false)) {
			// Launch the synchronization service
			
			// 1) Monitor sources
			
			_source_test_mutex_monitor.set(new MutexMonitor(SOURCE_TEST_MONITOR_MUTEX));
			_mutex_scheduler.schedule(_source_test_mutex_monitor.get(), 250L, TimeUnit.MILLISECONDS);
			_source_test_monitor_handle.set(_source_test_scheduler.scheduleWithFixedDelay(new SourceTestMonitor(), 10L, 2L, TimeUnit.SECONDS));
				//(give it 10 seconds before starting, let everything else settle down - eg give the bucket choose handler time to register)			
		}
	}
	/** Immediately start (for testing)
	 */
	@SuppressWarnings("deprecation")
	public void start() {
		_logger.info("IkanowV1SyncService_TestBuckets started, running on a schedule (typically every 1s)");
		_source_test_monitor_handle.get().cancel(true);
		_source_test_monitor_handle.forceSet(_source_test_scheduler.scheduleWithFixedDelay(new SourceTestMonitor(), 1, 1L, TimeUnit.SECONDS));
	}
	
	/** Stop threads (just for testing I think)
	 */
	public void stop() {
		_source_test_monitor_handle.get().cancel(true);
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
				catch (Throwable e) {
					_logger.error(ErrorUtils.getLongForm("{0}", e));
				}
				_logger.info("SourceTestMonitor: joined the leadership candidate cluster");
			}			
		}
		public boolean isLeader() {
			return _leader_selector.isSet() ? _leader_selector.get().hasLeadership() : false;
		}
	}

	
	public class SourceTestMonitor implements Runnable {
		private final SetOnce<ICrudService<TestQueueBean>> _v1_db = new SetOnce<ICrudService<TestQueueBean>>();
		private boolean _last_state = false;
		
		/* (non-Javadoc)
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			if (!_source_test_mutex_monitor.get().isLeader()) {
				_last_state = false;
				return;
			}
			if (!_last_state) {
				_logger.info("SourceTestMonitor: now the leader");
				_num_leader_changes++;
				_last_state = true;
			}
			if (!_v1_db.isSet()) {
				@SuppressWarnings("unchecked")
				final ICrudService<TestQueueBean> v1_config_db = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.v2_test_q/" + TestQueueBean.class.getName())).get();				
				_v1_db.set(v1_config_db);
				
				_v1_db.get().optimizeQuery(Arrays.asList(BeanTemplateUtils.from(TestQueueBean.class).field(TestQueueBean::status)));
			}
			
			try {
				// Synchronize
				synchronizeTestSources(
						_core_management_db.getDataBucketStore(), 
						_underlying_management_db.getDataBucketStatusStore(), 
						_v1_db.get(),
						new BucketTestService())
						.get();
				//(note this only waits for completions to finish, which should be super fast)
				
				//(note you can have multiple instances of the called code running even though the main thread pool is one,
				// because of the futures - the code handles it so that it doesn't try to start the code multiple times)
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
	protected CompletableFuture<Void> synchronizeTestSources(
			final IManagementCrudService<DataBucketBean> bucket_mgmt, 
			final IManagementCrudService<DataBucketStatusBean> underlying_bucket_status_mgmt, 
			final ICrudService<TestQueueBean> source_test_db,
			final BucketTestService bucket_test_service
			)
	{
		//_logger.debug("Starting a sync test sources cycle");
		final List<CompletableFuture<?>> new_results = new ArrayList<CompletableFuture<?>>(); // (not used for synchronization - in a single)
		final List<CompletableFuture<?>> existing_results = new ArrayList<CompletableFuture<?>>();		
		
		final CompletableFuture<List<TestQueueBean>> future_test_sources = getAllTestSources(source_test_db);
		
		//check for entries in test db
		return future_test_sources.thenCompose( test_sources -> {
			//_logger.debug("Got test sources successfully, looping over any results");
			test_sources.forEach(Lambdas.wrap_consumer_u(test_source -> {		
				_logger.debug("Looking at test source: " + test_source._id());

				final DataBucketBean to_test = Lambdas.wrap_u(() -> getBucketFromV1Source(test_source.source())).get();
				if ( test_source.status() != null &&
						(test_source.status().equals("in_progress") || test_source.status().equals("completed") || test_source.status().equals("error"))
						)
				{
					existing_results.add(handleExistingTestSource(to_test, test_source, source_test_db));
				} else { // in progress...
					
					_logger.debug("Found a new entry, setting up test");
					new_results.add(handleNewTestSource(to_test, test_source, bucket_test_service, source_test_db));								
				}
			}));
			if (existing_results.isEmpty()) { // Make sure at least that we don't start a new thread until we've got all the tests from the previous sources
				existing_results.add(future_test_sources);
			}
			//_logger.debug("done looping over test sources");
			//combine response of new and old entries, return
			List<CompletableFuture<?>> retval = 
					Arrays.asList(existing_results).stream() // potentially block on existing results but not new tests 'cos that can take ages
					.flatMap(l -> l.stream())
					.collect(Collectors.toList());			
			return CompletableFuture.allOf(retval.toArray(new CompletableFuture[0]));
		});				
	}
	
	/**
	 * Logic that runs when we come across an old test object
	 * Check if source is done:
	 * A. Has timed out
	 * B. Has created enough results
	 * If either are true, copy over what test results there are (if any), mark as done
	 * 
	 * @param data_bucket
	 * @param old_test_source
	 * @param source_test_db
	 * @return
	 */
	private CompletableFuture<Boolean> handleExistingTestSource(
			final DataBucketBean data_bucket,
			final TestQueueBean old_test_source,
			final ICrudService<TestQueueBean> source_test_db) {			
		
		// if null==started_processing_on, then source is still being started in a different thread, so just ignore it:
		if (null == old_test_source.started_processing_on()) {
			return CompletableFuture.completedFuture(true);
		}
		
		//ENTRY: is old		
		final ProcessingTestSpecBean test_spec = old_test_source.test_params();
		//get v1 bucket
		return getTestOutputCrudService(data_bucket).map(v2_output_db -> {
			//got the output crud, check if time is up or we have enough test results
			//1: time is up by checking started_on+test_spec vs now
			final long time_expires_on = old_test_source.started_processing_on().getTime()+(test_spec.max_run_time_secs()*1000);
			if ( new Date().getTime() > time_expires_on ) {
				_logger.debug("Test job: " + data_bucket.full_name() + " expired, need to retire");
				return retireTestJob(data_bucket, old_test_source, source_test_db, v2_output_db);
			}
			//2: test results, if we've hit the requested num results
			return checkTestHitRequestedNumResults(v2_output_db, data_bucket, test_spec, old_test_source, source_test_db);
		}).orElseGet(()->{			
			//we couldn't get the output crud, need to exit out
			//complete exceptionally so sync will throw an error
			_logger.error("Error getting test output crud");
			CompletableFuture<Boolean> db_error_future = new CompletableFuture<Boolean>();
			db_error_future.completeExceptionally(new Exception("Error retrieving output db for test job: " + data_bucket._id()));
			return db_error_future;
		});
	}
	
	/**
	 * Checks if the number of objects in v2_output_db is at least
	 * as many as we are looking for in test_spec.requested_num_objects
	 * 
	 * if so: retire job
	 * else: return back CF that we haven't reached our limit yet
	 * 
	 * @param v2_output_db
	 * @param data_bucket
	 * @param test_spec
	 * @param old_test_source
	 * @param source_test_db
	 * @return
	 */
	private CompletableFuture<Boolean> checkTestHitRequestedNumResults(final ICrudService<JsonNode> v2_output_db,
			final DataBucketBean data_bucket,
			final ProcessingTestSpecBean test_spec,
			final TestQueueBean old_test_source,
			final ICrudService<TestQueueBean> source_test_db) {
		return v2_output_db.countObjects().thenCompose( num_results -> {
			_logger.debug("Found: " + num_results + "("+test_spec.requested_num_objects()+") for this test.");
			if ( num_results >= test_spec.requested_num_objects()) {
				_logger.debug("Test job: " + data_bucket.full_name() + " reached requested num results, need to retire");
				return retireTestJob(data_bucket, old_test_source, source_test_db, v2_output_db);								
			} else {
				_logger.debug("Test job: " + data_bucket.full_name() + " haven't reached requested num results yet, let it continue");
				return CompletableFuture.completedFuture(true);
			}
		});
	}
	/**
	 * Returns an ICrudService for this buckets output.
	 * Currently only returns the searchIndexService, need to add in checks for other services.
	 * @param data_bucket
	 * @return
	 */
	private Optional<ICrudService<JsonNode>> getTestOutputCrudService(final DataBucketBean data_bucket) {
		//first need to change bucket to test bucket, otherwise we will overwrite any actual data
		final DataBucketBean test_data_bucket = BucketUtils.convertDataBucketBeanToTest(data_bucket, data_bucket.owner_id());	
		//store it in the correct location, depending on what is spec'd in the data_bucket
		//the results are being stored where the bucket output location is
		//if (test_config.service == "search_index_schema") service_context.getSearchIndexService().get().getCrudService(test_bucket).blah()
		//if (test_config.service == "storage_service") { /* TODO read the file or something */ }
		//longer term if (test_config_service == "graph_service") service_context.getGraphService().get().getTinkerpopService().blah
		return _context.getSearchIndexService().flatMap(IDataServiceProvider::getDataService)
				.flatMap(s -> s.getWritableDataService(JsonNode.class, test_data_bucket, Optional.empty(), Optional.empty()))
				.flatMap(IDataWriteService::getCrudService);
	}
	
	/**
	 * Moves up to test_spec.requested_num_objects into v2_output_db
	 * Then marks the TestQueueBean as completed.
	 * 
	 * @param data_bucket
	 * @param test_source
	 * @param source_test_db
	 * @param v2_output_db
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private CompletableFuture<Boolean> retireTestJob(
			final DataBucketBean data_bucket,
			final TestQueueBean test_source,
			final ICrudService<TestQueueBean> source_test_db,
			final ICrudService<JsonNode> v2_output_db) {
		_logger.debug("retiring job");
		return v2_output_db.getObjectsBySpec(CrudUtils.allOf().limit(test_source.test_params().requested_num_objects())).thenCompose( results -> {
			_logger.debug("returned: " + results.count() + " items in output collection (with limit, there could be more)");
			_logger.debug("moving data to mongo collection: ingest." + data_bucket._id());
			final ICrudService<JsonNode> v1_output_db = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest." + data_bucket._id())).get();		
			List<JsonNode> objects_to_store = new ArrayList<JsonNode>();
			results.forEach(objects_to_store::add);
			return objects_to_store.isEmpty()
					? CompletableFuture.completedFuture(true)
					: v1_output_db.deleteDatastore() // (should have already been deleted at test startup _and_ on completion, so this is to be on the safe side!)
						.thenCompose(__ -> v1_output_db.storeObjects(objects_to_store).thenCompose(x->CompletableFuture.completedFuture(true)))
						;
		}).thenCompose(x -> {
			_logger.debug("Marking job completed");
			//do final step for exists/not exists 
			final String output_collection = data_bucket._id();		
			//mark job as complete, point to v1 collection				
			return updateTestSourceStatus(test_source._id(), "completed", source_test_db, Optional.empty(), Optional.ofNullable(output_collection), Optional.empty());
		})
		.exceptionally(t -> {
			_logger.debug("Marking job completed with error");
			return updateTestSourceStatus(test_source._id(), ErrorUtils.get("error: {0}", t.getMessage()), source_test_db, Optional.empty(), Optional.empty(), Optional.empty())
					.join();
		})
		;
	}
	
	/**
	 * Logic that runs when we come across a new test object.
	 * Kicks off a test by calling BucketTestService.test_bucket -> this typically calls CoreManagementService.test_bucket
	 * Updates the status based on if the test_bucket started successfully
	 * 
	 * @param data_bucket
	 * @param new_test_source
	 * @param bucket_test_service
	 * @param source_test_db
	 * @return
	 */
	private CompletableFuture<Boolean> handleNewTestSource(
			final DataBucketBean data_bucket,
			final TestQueueBean new_test_source, 
			final BucketTestService bucket_test_service,
			final ICrudService<TestQueueBean> source_test_db) {
		//get the test params
		final ProcessingTestSpecBean test_spec = new_test_source.test_params();
		
		//try to test the bucket
		_logger.debug("Running bucket test");
		@SuppressWarnings("unchecked")
		final ICrudService<JsonNode> v1_output_db = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest." + data_bucket._id())).get();
		final CompletableFuture<Boolean> delete_datastore = v1_output_db.deleteDatastore(); //(this is done in a few other places, so just to be on the safe side here)
		final ManagementFuture<Boolean> test_res_future = bucket_test_service.test_bucket(_core_management_db, data_bucket, test_spec);
		
		return delete_datastore.exceptionally(ex->{
			_logger.error("Error trying to clear v1 output db before test run: ingest." + data_bucket._id(),ex);
			return false;
		}).thenCompose(y -> test_res_future.thenCompose(res -> {
			return test_res_future.getManagementResults().<Boolean>thenCompose(man_res -> {
				return updateTestSourceStatus(new_test_source._id(), (res ? "in_progress" : "error"), source_test_db, Optional.of(new Date()), Optional.empty(), Optional.of(man_res.stream().map(
					msg -> {
					return "[" + msg.date() + "] " + msg.source() + " (" + msg.command() + "): " + (msg.success() ? "INFO" : "ERROR") + ": " + msg.message();}
				).collect(Collectors.joining("\n"))));	
			});
		}).exceptionally(t -> {
			updateTestSourceStatus(new_test_source._id(), ("error"), source_test_db, Optional.of(new Date()), Optional.empty(), Optional.of("Error during test_bucket: " + t.getMessage()))
			.thenCompose(x -> {
				if ( !x )
					_logger.error("Had an error trying to update status of test object after having an error during test bucket, somethings gone horribly wrong");
				return CompletableFuture.completedFuture(x); //this return doesn't matter
			});
			return false;
		}));
	}
	
	/**
	 * Updates a test object with the given status and updates the last_processed_on date, 
	 * optionally also updates it's started_on date and/or the output_collection
	 * 
	 * @param id
	 * @param status
	 * @param source_test_db
	 * @param started_on
	 * @param output_collection
	 * @return
	 */
	private CompletableFuture<Boolean> updateTestSourceStatus(
			final String id, 
			final String status, 			
			final ICrudService<TestQueueBean> source_test_db, 
			Optional<Date> started_on,
			Optional<String> output_collection,
			Optional<String> message) {		
		_logger.debug("Setting test q: " + id + " status to: " + status + " with message: " + (message.isPresent() ? message.get() : "(no message)"));
		//create update with status, add started date if it was passed in
		BeanUpdateComponent<TestQueueBean> update_component = CrudUtils.update(TestQueueBean.class)
				.set(TestQueueBean::status, status)
				.set(TestQueueBean::last_processed_on, new Date());
		if ( started_on.isPresent() )
			update_component.set(TestQueueBean::started_processing_on, started_on.get());
		if ( output_collection.isPresent() )
			update_component.set(TestQueueBean::result, output_collection.get());		
		if ( message.isPresent() )
			update_component.set(TestQueueBean::message, message.get());
										
		final SingleQueryComponent<TestQueueBean> v1_query = CrudUtils.allOf(TestQueueBean.class).when("_id", id);
		return source_test_db.updateObjectBySpec(v1_query, Optional.empty(), update_component);		
	}
	
	/**
	 * Returns back all test sources that aren't marked as complete or errored
	 * 
	 * @param source_test_db
	 * @return
	 */
	protected CompletableFuture<List<TestQueueBean>> getAllTestSources(final ICrudService<TestQueueBean> source_test_db) {
		final QueryComponent<TestQueueBean> get_query =
				CrudUtils.allOf(TestQueueBean.class)
				.whenNot(TestQueueBean::status, "complete")
				.whenNot(TestQueueBean::status, "error"); //can be complete | error | in_progress | submitted | {unset/anything else}
				
		final QueryComponent<TestQueueBean> update_query =
				CrudUtils.allOf(TestQueueBean.class)
				.whenNot(TestQueueBean::status, "in_progress")
				.whenNot(TestQueueBean::status, "complete")
				.whenNot(TestQueueBean::status, "error"); //can be complete | error | in_progress | submitted | {unset/anything else}

		final UpdateComponent<TestQueueBean> update_command = CrudUtils.update(TestQueueBean.class)
				.set(TestQueueBean::status, "in_progress")
				// (don't set started_processing_on - only set that once the job has been launched)
				;		
		
		final CompletableFuture<List<TestQueueBean>> get_command = 
				source_test_db.getObjectsBySpec(get_query)
				.thenApply(c -> StreamSupport.stream(c.spliterator(), false).collect(Collectors.toList()));
		
		return get_command.thenCompose(__ -> {
			return source_test_db.updateObjectsBySpec(update_query, Optional.of(false), update_command);
		})
		.thenApply(__ -> get_command.join()); // (ie return the original command but only once the update has completed)
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	protected static String getBucketIdFromV1SourceKey(final String key) {
		return key + ';';
	}
	protected static String getV1SourceKeyFromBucketId(final String _id) {
		return _id.endsWith(";") ? _id.substring(0, _id.length()-1) : _id;
	}
	
	// LOW LEVEL UTILS
	
	/** Builds a V2 bucket out of a V1 source
	 * @param src_json
	 * @return
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 * @throws ParseException
	 */
	public static DataBucketBean getBucketFromV1Source(final JsonNode src_json) throws JsonParseException, JsonMappingException, IOException, ParseException {

		final DataBucketBean not_test_version = IkanowV1SyncService_Buckets.getBucketFromV1Source(src_json);
		return BeanTemplateUtils.clone(not_test_version)
					.with(DataBucketBean::_id, not_test_version.owner_id() + not_test_version._id())
				.done();
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
