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
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.UnaryOperator;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.codepoetics.protonpack.StreamUtils;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.BeanUpdateComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.data_model.TestQueueBean;
import com.ikanow.aleph2.management_db.mongodb.module.MongoDbManagementDbModule.BucketTestService;

/** This service looks for changes to IKANOW test db entries and kicks off test jobs
 * @author cmb
 */
public class IkanowV1SyncService_TestBuckets {
	private static final Logger _logger = LogManager.getLogger();
	private static final ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
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
	/** Immediately start
	 */
	public void start() {
		_logger.info("IkanowV1SyncService_TestBuckets started, running on a schedule (typically every 1s)");
		_source_test_monitor_handle.get().cancel(true);
		_source_test_monitor_handle.set(_source_test_scheduler.scheduleWithFixedDelay(new SourceTestMonitor(), 1, 1L, TimeUnit.SECONDS));
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
				
				//_v1_db.get().optimizeQuery(Arrays.asList("extractType"));
			}
			
			try {
				// Synchronize
				synchronizeTestSources(
						_core_management_db.getDataBucketStore(), 
						_underlying_management_db.getDataBucketStatusStore(), 
						_v1_db.get(),
						new BucketTestService())
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
	protected CompletableFuture<Void> synchronizeTestSources(
			final IManagementCrudService<DataBucketBean> bucket_mgmt, 
			final IManagementCrudService<DataBucketStatusBean> underlying_bucket_status_mgmt, 
			final ICrudService<TestQueueBean> source_test_db,
			final BucketTestService bucket_test_service
			)
	{
		//_logger.debug("Starting a sync test sources cycle");
		final List<CompletableFuture<Boolean>> new_results = new ArrayList<CompletableFuture<Boolean>>();
		final List<CompletableFuture<Boolean>> existing_results = new ArrayList<CompletableFuture<Boolean>>();		
		
		//check for entries in test db
		return getAllTestSources(source_test_db).thenCompose( test_sources -> {
			//_logger.debug("Got test sources successfully, looping over any results");
			test_sources.forEach(Lambdas.wrap_consumer_u(test_source -> {		
				_logger.debug("Looking at test source: " + test_source._id());
				//TODO if the getBucketFromV1Source throws an exeption, it gets tossed the entire way up to whatever called synchronizeTestSources (e.g. on the CF)
				final DataBucketBean to_test = Lambdas.wrap_u(() -> getBucketFromV1Source(test_source.source())).get();
				if ( test_source.status() != null &&
						test_source.status().equals("in_progress") ) {
					_logger.debug("Found an old entry, checking if test is done");
					existing_results.add(handleExistingTestSource(to_test, test_source, source_test_db));					
				} else {
					_logger.debug("Found a new entry, setting up test");
					new_results.add(handleNewTestSource(to_test, test_source, bucket_test_service, source_test_db));								
				}
			}));
			//_logger.debug("done looping over test sources");
			//combine response of new and old entries, return
			List<CompletableFuture<?>> retval = 
					Arrays.asList(new_results, existing_results).stream()
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
			return v1_output_db.storeObjects(objects_to_store).thenCompose(x->CompletableFuture.completedFuture(true));
		}).thenCompose(x -> {
			_logger.debug("Marking job completed");
			//do final step for exists/not exists 
			final String output_collection = data_bucket._id();		
			//mark job as complete, point to v1 collection				
			return updateTestSourceStatus(test_source._id(), "completed", source_test_db, Optional.of(new Date()), Optional.ofNullable(output_collection), Optional.empty());
		});
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
		final ManagementFuture<Boolean> test_res_future = bucket_test_service.test_bucket(_core_management_db, data_bucket, test_spec);
		return test_res_future.thenApply(res -> {					
			try {
				_logger.debug("finished test_bucket, about to get any messages and update status");
				Collection<BasicMessageBean> man_res = test_res_future.getManagementResults().get();
				return updateTestSourceStatus(new_test_source._id(), (res ? "in_progress" : "error"), source_test_db, Optional.of(new Date()), Optional.empty(), Optional.of(man_res.stream().map(
						msg -> {
							return "[" + msg.date() + "] " + msg.source() + " (" + msg.command() + "): " + (msg.success() ? "INFO" : "ERROR") + ": " + msg.message();}
						).collect(Collectors.joining("\n"))));	
			} catch (Exception e) {		
				_logger.error("Had an exception in test_bucket: ", e);
				return updateTestSourceStatus(new_test_source._id(), "error", source_test_db, Optional.of(new Date()), Optional.empty(), Optional.of(ErrorUtils.getLongForm("{0}", e)));
			}
		}).exceptionally(t-> {
			_logger.error("Had an error trying to test_bucket: ", t);
			//threw an exception when trying to run test_bucket, return exception
			//had an error running test, update status and return
			return updateTestSourceStatus(new_test_source._id(), "error", source_test_db, Optional.of(new Date()), Optional.empty(), Optional.of("Error during test bucket: " + t.getMessage()));			
			})
			.thenCompose(x->x); //let an exception in updateTestSourceStatus throw
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
	protected CompletableFuture<Cursor<TestQueueBean>> getAllTestSources(final ICrudService<TestQueueBean> source_test_db) {
		return source_test_db.getObjectsBySpec(
						CrudUtils.allOf(TestQueueBean.class)
						.whenNot("status", "complete")
						.whenNot("status", "error")); //can be complete | error | in_progress | submitted | {unset/anything else}
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
		// (think we'll use key instead of _id):
		//final String _id = safeJsonGet("_id", src_json).asText(); 
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
		final JsonNode data_bucket_tmp = safeJsonGet("data_bucket", px_pipeline_first_el);// (WARNING: mutable, see below)
		final JsonNode scripting = safeJsonGet("scripting", data_bucket_tmp);
		
		// HANDLE SUBSTITUTION
		final String sub_prefix = Optional.ofNullable(scripting.get("sub_prefix")).map(x -> x.asText()).orElse("$$SCRIPT_");
		final String sub_suffix = Optional.ofNullable(scripting.get("sub_suffix")).map(x -> x.asText()).orElse("$$");
		final List<UnaryOperator<String>> search_replace = 
				StreamSupport.stream(Spliterators.spliteratorUnknownSize(scripting.fieldNames(), Spliterator.ORDERED), false)
						.filter(f -> !f.equals("sub_prefix") && !f.equals("sub_suffix")) // (remove non language fields)
						.map(lang -> Tuples._2T(scripting.get(lang), lang))
						// Get (separator regex, entire script, sub prefix)
						.map(scriptobj_lang -> Tuples._3T(safeJsonGet("separator_regex", scriptobj_lang._1()).asText(""), 
															safeJsonGet("script", scriptobj_lang._1()).asText(""), 
																sub_prefix + scriptobj_lang._2()))
						// Split each "entire script" up into blocks of format (bloc, lang)
						.<Stream<Tuple2<String,String>>>
							map(regex_script_lang -> Stream.concat(
														Stream.of(Tuples._2T(regex_script_lang._2(), regex_script_lang._3()))
														, 
														regex_script_lang._1().isEmpty() 
															? 
															Stream.of(Tuples._2T(regex_script_lang._2(), regex_script_lang._3()))																	
															:
															Arrays.stream(regex_script_lang._2().split(regex_script_lang._1()))
																.<Tuple2<String, String>>map(s -> Tuples._2T(s, regex_script_lang._3()))
													))
						// Associate a per-lang index with each  script block -> (replacement, string_sub)
						.<Tuple2<String,String>>
							flatMap(stream -> StreamUtils.zip(stream, 
														Stream.iterate(0, i -> i+1), (script_lang, i) -> 
															Tuples._2T(script_lang._1().replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r"), 
																	i == 0
																	? script_lang._2() + sub_suffix // (entire thing)
																	: script_lang._2() + "_" + i + sub_suffix))) //(broken down components)

						.<UnaryOperator<String>>map(t2 -> (String s) -> s.replace(t2._2(), t2._1()))
							//(need to escape "s and newlines)
						.collect(Collectors.toList())
						;
		
		// Apply the list of transforms to the string
		((ObjectNode) data_bucket_tmp).remove("scripting"); // (WARNING: mutable)
		final String data_bucket_str = search_replace.stream()
											.reduce(
												data_bucket_tmp.toString(), 
												(acc, s) -> s.apply(acc), 
												(acc1, acc2) -> acc1);
		
		// Convert back to the bucket JSON
		final JsonNode data_bucket = _mapper.readTree(data_bucket_str);
		
		final DataBucketBean bucket = BeanTemplateUtils.build(data_bucket, DataBucketBean.class)
													.with(DataBucketBean::_id, getBucketIdFromV1SourceKey(key))
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
