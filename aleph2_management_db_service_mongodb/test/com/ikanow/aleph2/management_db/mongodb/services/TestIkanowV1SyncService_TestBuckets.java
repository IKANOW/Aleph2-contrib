/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.management_db.mongodb.services;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.data_model.TestQueueBean;
import com.ikanow.aleph2.management_db.mongodb.data_model.TestQueueBean.TestStatus;
import com.ikanow.aleph2.management_db.mongodb.module.MockMongoDbManagementDbModule;
import com.ikanow.aleph2.management_db.mongodb.module.MongoDbManagementDbModule.BucketTestService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestIkanowV1SyncService_TestBuckets {
	final static protected Logger _logger = LogManager.getLogger();	
	protected ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// TEST SETUP
	@Inject 
	protected IServiceContext _service_context = null;
	
	@Inject 
	protected IkanowV1SyncService_TestBuckets sync_service; 

	@Inject 
	protected MongoDbManagementDbConfigBean _service_config; 

	@Before
	public void setupDependencies() throws Exception {
		try {
			//IMPORTANT NOTE: WE USE CORE MANAGEMENT DB == UNDERLYING MANAGEMENT DB (==mongodb) HERE SO JUST USE getCoreManagementDbService() ANYWHERE 
			//YOU WANT AN IManagementDbService, NOT SURE fongo ALWAYS WORKS IF YOU GRAB getService(IManagementDbService.class, ...) 
			
			final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
			
			// OK we're going to use guice, it was too painful doing this by hand...				
			Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("test_v1_sync_service.properties")))
								.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
								.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
								.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
								.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
			
			Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(new MockMongoDbManagementDbModule()), Optional.of(config));			
			app_injector.injectMembers(this);
		}
		catch (Throwable t) {
			System.out.println(ErrorUtils.getLongForm("{0}", t));
			throw t;
		}
	}
	
	@Test
	public void test_setup() {
		_logger.info("Starting test_Setup");
		
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		assertTrue("setup completed - service context", _service_context != null);
		assertTrue("setup completed - services", _service_context.getCoreManagementDbService() != null);
		assertTrue("setup completed - services", sync_service != null);
		assertEquals(temp_dir, _service_context.getGlobalProperties().local_root_dir());
		
		if (File.separator.equals("\\")) { // windows mode!
			assertTrue("WINDOWS MODE: hadoop home needs to be set (use -Dhadoop.home.dir={HADOOP_HOME} in JAVA_OPTS)", null != System.getProperty("hadoop.home.dir"));
			assertTrue("WINDOWS MODE: hadoop home needs to exist: " + System.getProperty("hadoop.home.dir"), null != System.getProperty("hadoop.home.dir"));
		}
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// LOW LEVEL UTILS - PART 1
	
	@SuppressWarnings("deprecation")
	@Test
	public void test_sourceToBucketConversion() throws JsonProcessingException, IOException, ParseException {
		_logger.info("Starting test_SourceToBucketConversion");

		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());		
		final JsonNode v1_source = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		
		final DataBucketBean bucket = IkanowV1SyncService_TestBuckets.getBucketFromV1Source(v1_source);
		
		assertEquals("506dc16dfbf042893dd6b8f2aleph...bucket.Template_V2_data_bucket.;", bucket._id());
		assertEquals(Collections.unmodifiableSet(new HashSet<String>()), bucket.aliases());
		assertEquals(1, bucket.batch_enrichment_configs().size());
		assertEquals(false, bucket.batch_enrichment_configs().get(0).enabled());
		assertEquals(1, bucket.batch_enrichment_configs().get(0).config().get("key1"));
		assertEquals("value2", bucket.batch_enrichment_configs().get(0).config().get("key2"));
		assertEquals(null, bucket.batch_enrichment_topology());
		assertEquals("21 May 2015 02:37:23 GMT", bucket.created().toGMTString());
		assertEquals(null, bucket.data_locations());
		assertEquals(true,bucket.data_schema().columnar_schema().enabled());
		assertEquals(null,bucket.data_schema().data_warehouse_schema());
		assertEquals(false,bucket.data_schema().document_schema().enabled());
		assertEquals(null,bucket.data_schema().geospatial_schema());
		assertEquals(null,bucket.data_schema().graph_schema());
		assertEquals(true,bucket.data_schema().search_index_schema().enabled());
		assertEquals("week",bucket.data_schema().storage_schema().raw().grouping_time_period());
		assertEquals(true,bucket.data_schema().temporal_schema().enabled());
		assertEquals("DESCRIPTION HERE.", bucket.description());
		assertEquals("Template V2 data bucket", bucket.display_name());
		assertEquals("/bucket/path/here", bucket.full_name());
		assertEquals(1, bucket.harvest_configs().size());
		assertEquals(true, bucket.harvest_configs().get(0).enabled());
		assertEquals("value1_harvest", bucket.harvest_configs().get(0).config().get("key1"));
		assertEquals("/app/aleph2/library/import/harvest/tech/XXX", bucket.harvest_technology_name_or_id());
		assertEquals("streaming", bucket.master_enrichment_type().toString());
		assertEquals("25 May 2015 13:52:01 GMT", bucket.modified().toGMTString());
		assertEquals(null, bucket.multi_bucket_children());
		assertEquals(false, bucket.multi_node_enabled());
		assertEquals(Arrays.asList(), bucket.node_list_rules());
		assertEquals("506dc16dfbf042893dd6b8f2", bucket.owner_id());
		assertEquals(null, bucket.poll_frequency());
		assertEquals(null, bucket.streaming_enrichment_configs());
		assertEquals(true, bucket.streaming_enrichment_topology().enabled());
		assertEquals("value1_streaming", bucket.streaming_enrichment_topology().config().get("key1"));
		assertEquals(Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("test"))), bucket.tags());
	}

	@SuppressWarnings("deprecation")
	@Test 
	public void test_SourceToBucketConversion_scripting() throws JsonProcessingException, IOException, ParseException {		
		_logger.info("Starting test_SourceToBucketConversion_scripting");
		
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		
		{
			final JsonNode v1_source = mapper.readTree(this.getClass().getResourceAsStream("test_scripting_1.json"));
			final DataBucketBean bucket = IkanowV1SyncService_TestBuckets.getBucketFromV1Source(v1_source);
			
			// (all the existing stuff)
			assertEquals("506dc16dfbf042893dd6b8f2aleph...bucket.Template_V2_data_bucket.;", bucket._id());
			assertEquals("21 May 2015 02:37:23 GMT", bucket.created().toGMTString());
			assertEquals(null, bucket.data_locations());
			assertEquals("DESCRIPTION HERE.", bucket.description());
			assertEquals("Template V2 data bucket", bucket.display_name());
			assertEquals(1, bucket.harvest_configs().size());
			assertEquals(true, bucket.harvest_configs().get(0).enabled());
			assertEquals("/app/aleph2/library/import/harvest/tech/XXX", bucket.harvest_technology_name_or_id());
			assertEquals("none", bucket.master_enrichment_type().toString());
			assertEquals("25 May 2015 13:52:01 GMT", bucket.modified().toGMTString());
			assertEquals(null, bucket.multi_bucket_children());
			assertEquals(false, bucket.multi_node_enabled());
			assertEquals("506dc16dfbf042893dd6b8f2", bucket.owner_id());
			assertEquals(null, bucket.poll_frequency());
			assertEquals(null, bucket.streaming_enrichment_configs());
			assertEquals(null, bucket.streaming_enrichment_topology());
			assertEquals(Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("test"))), bucket.tags());
			
			//Plus check on the subvariables:
			assertEquals(3, bucket.harvest_configs().get(0).config().size());
			assertEquals("test1:string1\n", bucket.harvest_configs().get(0).config().get("test1"));
			assertEquals("test2:\n\"string2\"\r:test2", bucket.harvest_configs().get(0).config().get("test2"));
			assertEquals("string1\n//ALEPH2_MODULE------------------\n\"string2\"\r:test_all", bucket.harvest_configs().get(0).config().get("test_all"));			
		}
		
		{
			final JsonNode v1_source = mapper.readTree(this.getClass().getResourceAsStream("test_scripting_2.json"));
			final DataBucketBean bucket = IkanowV1SyncService_TestBuckets.getBucketFromV1Source(v1_source);
			
			// (all the existing stuff)
			assertEquals("506dc16dfbf042893dd6b8f2aleph...bucket.Template_V2_data_bucket.;", bucket._id());
			assertEquals("21 May 2015 02:37:23 GMT", bucket.created().toGMTString());
			assertEquals(null, bucket.data_locations());
			assertEquals("DESCRIPTION HERE.", bucket.description());
			assertEquals("Template V2 data bucket", bucket.display_name());
			assertEquals(1, bucket.harvest_configs().size());
			assertEquals(true, bucket.harvest_configs().get(0).enabled());
			assertEquals("/app/aleph2/library/import/harvest/tech/a", bucket.harvest_technology_name_or_id());
			assertEquals("none", bucket.master_enrichment_type().toString());
			assertEquals("25 May 2015 13:52:01 GMT", bucket.modified().toGMTString());
			assertEquals(null, bucket.multi_bucket_children());
			assertEquals(false, bucket.multi_node_enabled());
			assertEquals("506dc16dfbf042893dd6b8f2", bucket.owner_id());
			assertEquals(null, bucket.poll_frequency());
			assertEquals(null, bucket.streaming_enrichment_configs());
			assertEquals(null, bucket.streaming_enrichment_topology());
			assertEquals(Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("test"))), bucket.tags());
			
			//Plus check on the subvariables:
			assertEquals(1, bucket.harvest_configs().get(0).config().size());
			assertEquals("string1\n//ALEPH2_MODULE------------------\n\"string2\"", bucket.harvest_configs().get(0).config().get("test_all"));			
		}
	}		

	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// DB INTEGRATION - READ	
	@SuppressWarnings("unchecked")
	@Test
	public void test_HitNumResultsSynchronizeSources() throws JsonProcessingException, IOException, ParseException, InterruptedException, ExecutionException {
		_logger.info("Starting test_synchronizeSources");
		final ICrudService<TestQueueBean> v2_test_q = this._service_context.getCoreManagementDbService().getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.v2_test_q/" + TestQueueBean.class.getName())).get();
		
		//clear test_q
		v2_test_q.deleteDatastore().get();
		
		//put an entry in the test_q
		final TestQueueBean test_entry_1 = createTestQEntry(20L);
		assertEquals(v2_test_q.countObjects().get().longValue(), 0);
		v2_test_q.storeObject(test_entry_1).get();
		assertEquals(v2_test_q.countObjects().get().longValue(), 1);
		
		//ensure that object is in the db
		Cursor<TestQueueBean> test_objects = v2_test_q.getObjectsBySpec(
				CrudUtils.allOf(TestQueueBean.class)
				.whenNot("status", TestStatus.completed)).get(); //can be complete | in_progress | {unset/anything else}
		test_objects.forEach( test_object -> assertTrue(test_object._id().equals(test_entry_1._id())));
		
		//run test cycle, this should have:
		//1. picked up the test queue bean
		//2. sent it through our fake test success service
		//3. marked it as in progress
		sync_service.synchronizeTestSources(sync_service._core_management_db.getDataBucketStore(), 
				sync_service._underlying_management_db.getDataBucketStatusStore(), 
				v2_test_q, new SuccessBucketTestService()).get();				
		
		//ensure its status gets updated to in_progress
		assertEquals(v2_test_q.getObjectById(test_entry_1._id()).get().get().status(), TestStatus.in_progress); //status should no longer be submitted		

		//lets throw some fake output in the db (more than the requested 10 items)
		final DataBucketBean data_bucket = IkanowV1SyncService_TestBuckets.getBucketFromV1Source(test_entry_1.source());
		final ICrudService<JsonNode> v2_output_index = getTestOutputCrudService(data_bucket).get();
		insertFakeOutputData(v2_output_index, data_bucket, 15);
		assertEquals(v2_output_index.countObjects().get().longValue(), 15);
		
		
		//run a second time, it should:
		//1. find the in_progress source and check its status
		//2. find we have more than enough results
		//3. copy the requested number of results into our test db
		//4. set the team beans results field to the collection we moved the data to
		//5. mark the test bean as completed
		sync_service.synchronizeTestSources(sync_service._core_management_db.getDataBucketStore(), 
				sync_service._underlying_management_db.getDataBucketStatusStore(), 
				v2_test_q, new SuccessBucketTestService()).get();		
		
		//should have maxed out it's results, check it copied them into output dir
		final TestQueueBean test_bean = v2_test_q.getObjectById(test_entry_1._id()).get().get();
		_logger.info("TestBean: status:" + test_bean.status() + " result: " + test_bean.result());
		assertEquals(test_bean.status(), TestStatus.completed); //status should no longer be submitted
		final String output_collection = test_bean.result();
		//check the output db
		final ICrudService<TestQueueBean> v2_output_db = this._service_context.getCoreManagementDbService().getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest." + output_collection)).get();
		assertEquals(10, v2_output_db.countObjects().get().longValue());
		
		//cleanup
		v2_output_db.deleteDatastore().get();
		v2_output_index.deleteDatastore().get();
		v2_test_q.deleteDatastore().get();										
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test_TimeoutSomeResultsSynchronizeSources() throws JsonProcessingException, IOException, ParseException, InterruptedException, ExecutionException {
		_logger.info("Starting test_synchronizeSources");
		final ICrudService<TestQueueBean> v2_test_q = this._service_context.getCoreManagementDbService().getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.v2_test_q/" + TestQueueBean.class.getName())).get();
		
		//clear test_q
		v2_test_q.deleteDatastore().get();
		
		//put an entry in the test_q
		final TestQueueBean test_entry_1 = createTestQEntry(0L);
		assertEquals(v2_test_q.countObjects().get().longValue(), 0);
		v2_test_q.storeObject(test_entry_1).get();
		assertEquals(v2_test_q.countObjects().get().longValue(), 1);
		
		//ensure that object is in the db
		Cursor<TestQueueBean> test_objects = v2_test_q.getObjectsBySpec(
				CrudUtils.allOf(TestQueueBean.class)
				.whenNot("status", TestStatus.completed)).get(); //can be complete | in_progress | {unset/anything else}
		test_objects.forEach( test_object -> assertTrue(test_object._id().equals(test_entry_1._id())));
		
		//run test cycle, this should have:
		//1. picked up the test queue bean
		//2. sent it through our fake test success service
		//3. marked it as in progress
		sync_service.synchronizeTestSources(sync_service._core_management_db.getDataBucketStore(), 
				sync_service._underlying_management_db.getDataBucketStatusStore(), 
				v2_test_q, new SuccessBucketTestService()).get();				
		
		//ensure its status gets updated to in_progress
		assertEquals(v2_test_q.getObjectById(test_entry_1._id()).get().get().status(), TestStatus.in_progress); //status should no longer be submitted		

		//lets throw some fake output in the db (less than the requested 10 items)
		final DataBucketBean data_bucket = IkanowV1SyncService_TestBuckets.getBucketFromV1Source(test_entry_1.source());
		final ICrudService<JsonNode> v2_output_index = getTestOutputCrudService(data_bucket).get();
		insertFakeOutputData(v2_output_index, data_bucket, 5);		
		assertEquals(v2_output_index.countObjects().get().longValue(), 5);
		
		
		//run a second time, it should:
		//1. find the in_progress source and check its status
		//2. find we have more than enough results
		//3. copy the requested number of results into our test db
		//4. set the team beans results field to the collection we moved the data to
		//5. mark the test bean as completed
		sync_service.synchronizeTestSources(sync_service._core_management_db.getDataBucketStore(), 
				sync_service._underlying_management_db.getDataBucketStatusStore(), 
				v2_test_q, new SuccessBucketTestService()).get();	
		
		//should have maxed out it's results, check it copied them into output dir
		final TestQueueBean test_bean = v2_test_q.getObjectById(test_entry_1._id()).get().get();
		_logger.info("TestBean: status:" + test_bean.status() + " result: " + test_bean.result());
		assertEquals(test_bean.status(), TestStatus.completed); //status should no longer be submitted
		final String output_collection = test_bean.result();
		//check the output db
		final ICrudService<TestQueueBean> v2_output_db = this._service_context.getCoreManagementDbService().getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest." + output_collection)).get();
		assertEquals(5, v2_output_db.countObjects().get().longValue());
		
		//cleanup
		v2_output_db.deleteDatastore().get();
		v2_output_index.deleteDatastore().get();
		v2_test_q.deleteDatastore().get();										
	}
	
	private void insertFakeOutputData(ICrudService<JsonNode> v2_output_index, final DataBucketBean data_bucket, int num_objects_to_insert) throws InterruptedException, ExecutionException {		
		List<JsonNode> test_objects = IntStream.range(0, num_objects_to_insert).boxed().map(i -> _mapper.createObjectNode().put("test", "test" + i).put("_id", "a"+i)).collect(Collectors.toList());
		v2_output_index.storeObjects(test_objects).get();
		Thread.sleep(10000);
		_logger.debug("Inserted: " + num_objects_to_insert + " into test output db");		
	}
	
	private Optional<ICrudService<JsonNode>> getTestOutputCrudService(final DataBucketBean data_bucket) {
		final DataBucketBean test_data_bucket = BucketUtils.convertDataBucketBeanToTest(data_bucket, data_bucket.owner_id());
		return _service_context.getSearchIndexService().flatMap(IDataServiceProvider::getDataService)
				.flatMap(s -> s.getWritableDataService(JsonNode.class, test_data_bucket, Optional.empty(), Optional.empty()))
				.flatMap(IDataWriteService::getCrudService);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void test_TimeoutSynchronizeSources() throws JsonProcessingException, IOException, ParseException, InterruptedException, ExecutionException {
		_logger.info("Starting test_synchronizeSources");
		final ICrudService<TestQueueBean> v2_test_q = this._service_context.getCoreManagementDbService().getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.v2_test_q/" + TestQueueBean.class.getName())).get();
		
		//clear test_q
		v2_test_q.deleteDatastore().get();
		
		//put an entry in the test_q
		final TestQueueBean test_entry_1 = createTestQEntry(0L);
		_logger.info("ENTRY: " + test_entry_1.toString());
		assertEquals(v2_test_q.countObjects().get().longValue(), 0);
		v2_test_q.storeObject(test_entry_1).get();
		assertEquals(v2_test_q.countObjects().get().longValue(), 1);
		
		//ensure that object is in the db
		Cursor<TestQueueBean> test_objects = v2_test_q.getObjectsBySpec(
				CrudUtils.allOf(TestQueueBean.class)
				.whenNot("status", TestStatus.completed)).get(); //can be complete | in_progress | {unset/anything else}
		test_objects.forEach( test_object -> {
			System.out.println(test_object.toString());
			assertTrue(test_object._id().equals(test_entry_1._id()));
		} );
		
		//run test cycle
		sync_service.synchronizeTestSources(sync_service._core_management_db.getDataBucketStore(), 
				sync_service._underlying_management_db.getDataBucketStatusStore(), 
				v2_test_q, new SuccessBucketTestService()).get();				
		
		//ensure its status gets updated to in_progress
		assertEquals(v2_test_q.getObjectById(test_entry_1._id()).get().get().status(), TestStatus.in_progress); //status should no longer be submitted		
		System.out.println("here");
		//run a second time, it should find the in_progress source and check its status
		sync_service.synchronizeTestSources(sync_service._core_management_db.getDataBucketStore(), 
				sync_service._underlying_management_db.getDataBucketStatusStore(), 
				v2_test_q, new SuccessBucketTestService()).get();	
		
		//should have timed out, been marked as completed 
		assertEquals(v2_test_q.getObjectById(test_entry_1._id()).get().get().status(), TestStatus.completed); //status should no longer be submitted
		
		//cleanup
		v2_test_q.deleteDatastore().get();										
	}
	
	
	@SuppressWarnings("unchecked")
	@Test
	public void test_FailsynchronizeSources() throws JsonProcessingException, IOException, ParseException, InterruptedException, ExecutionException {
		_logger.info("Starting test_synchronizeSources");
		final ICrudService<TestQueueBean> v2_test_q = this._service_context.getCoreManagementDbService().getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.v2_test_q/" + TestQueueBean.class.getName())).get();
		
		//clear test_q
		v2_test_q.deleteDatastore().get();
		
		//put an entry in the test_q
		final TestQueueBean test_entry_1 = createTestQEntry(10L);
		_logger.info("ENTRY: " + test_entry_1.toString());
		assertEquals(v2_test_q.countObjects().get().longValue(), 0);
		v2_test_q.storeObject(test_entry_1).get();
		assertEquals(v2_test_q.countObjects().get().longValue(), 1);
		
		//ensure that object is in the db
		Cursor<TestQueueBean> test_objects = v2_test_q.getObjectsBySpec(
				CrudUtils.allOf(TestQueueBean.class)
				.whenNot("status", TestStatus.completed)).get(); //can be complete | in_progress | {unset/anything else}
		test_objects.forEach( test_object -> {
			System.out.println(test_object.toString());
			assertTrue(test_object._id().equals(test_entry_1._id()));
		} );
		
		
		
		//run test cycle
		sync_service.synchronizeTestSources(sync_service._core_management_db.getDataBucketStore(), 
				sync_service._underlying_management_db.getDataBucketStatusStore(), 
				v2_test_q, new FailBucketTestService()).get();				
		
		//ensure its status gets updated to in_progress		
		assertEquals(v2_test_q.getObjectById(test_entry_1._id()).get().get().status(), TestStatus.error); //status should no longer be submitted			
		
		//cleanup
		v2_test_q.deleteDatastore().get();										
	}
		
	@SuppressWarnings("unchecked")
	@Test
	public void test_ErrorSynchronizeSources() throws JsonProcessingException, IOException, ParseException, InterruptedException, ExecutionException {
		_logger.info("Starting test_synchronizeSources");
		final ICrudService<TestQueueBean> v2_test_q = this._service_context.getCoreManagementDbService().getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.v2_test_q/" + TestQueueBean.class.getName())).get();
		
		//clear test_q
		v2_test_q.deleteDatastore().get();
		
		//put an entry in the test_q
		final TestQueueBean test_entry_1 = createTestQEntry(10L);
		_logger.info("ENTRY: " + test_entry_1.toString());
		assertEquals(v2_test_q.countObjects().get().longValue(), 0);
		v2_test_q.storeObject(test_entry_1).get();
		assertEquals(v2_test_q.countObjects().get().longValue(), 1);
		
		//ensure that object is in the db
		Cursor<TestQueueBean> test_objects = v2_test_q.getObjectsBySpec(
				CrudUtils.allOf(TestQueueBean.class)
				.whenNot("status", TestStatus.completed)).get(); //can be complete | in_progress | {unset/anything else}
		test_objects.forEach( test_object -> {
			System.out.println(test_object.toString());
			assertTrue(test_object._id().equals(test_entry_1._id()));
		} );
		
		
		
		//run test cycle
		sync_service.synchronizeTestSources(sync_service._core_management_db.getDataBucketStore(), 
				sync_service._underlying_management_db.getDataBucketStatusStore(), 
				v2_test_q, new ErrorBucketTestService()).get();				
		
		//ensure its status gets updated to in_progress		
		TestQueueBean t = v2_test_q.getObjectById(test_entry_1._id()).get().get();
		_logger.info("Got item: " + t._id());
		assertEquals(t.status(), TestStatus.error); //status should no longer be submitted		
		
		//as a bonus, run again, make sure this error source isn't attempted again?
		sync_service.synchronizeTestSources(sync_service._core_management_db.getDataBucketStore(), 
				sync_service._underlying_management_db.getDataBucketStatusStore(), 
				v2_test_q, new ErrorBucketTestService()).get();
		
		//cleanup
		v2_test_q.deleteDatastore().get();										
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test_ErrorThenPassSynchronizeSources() throws JsonProcessingException, IOException, ParseException, InterruptedException, ExecutionException {
		_logger.info("Starting test_synchronizeSources");
		final ICrudService<TestQueueBean> v2_test_q = this._service_context.getCoreManagementDbService().getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.v2_test_q/" + TestQueueBean.class.getName())).get();
		
		//clear test_q
		v2_test_q.deleteDatastore().get();
		
		//put an entry in the test_q
		final TestQueueBean test_entry_1 = createTestQEntry(10L);
		_logger.info("ENTRY: " + test_entry_1.toString());
		assertEquals(v2_test_q.countObjects().get().longValue(), 0);
		v2_test_q.storeObject(test_entry_1).get();
		assertEquals(v2_test_q.countObjects().get().longValue(), 1);
		
		//ensure that object is in the db
		Cursor<TestQueueBean> test_objects = v2_test_q.getObjectsBySpec(
				CrudUtils.allOf(TestQueueBean.class)
				.whenNot("status", TestStatus.completed)).get(); //can be complete | in_progress | {unset/anything else}
		test_objects.forEach( test_object -> {
			System.out.println(test_object.toString());
			assertTrue(test_object._id().equals(test_entry_1._id()));
		} );
		
		//run test cycle
		sync_service.synchronizeTestSources(sync_service._core_management_db.getDataBucketStore(), 
				sync_service._underlying_management_db.getDataBucketStatusStore(), 
				v2_test_q, new ErrorBucketTestService()).get();				
		
		//ensure its status gets updated to in_progress		
		TestQueueBean t = v2_test_q.getObjectById(test_entry_1._id()).get().get();
		_logger.info("Got item: " + t._id());
		assertEquals(t.status(), TestStatus.error); //status should no longer be submitted		
		
		//as a bonus, run again, make sure this error source isn't attempted again?
		//NOTE: changed the BucketTestService to a passing one, so if it does get picked up it will run
		sync_service.synchronizeTestSources(sync_service._core_management_db.getDataBucketStore(), 
				sync_service._underlying_management_db.getDataBucketStatusStore(), 
				v2_test_q, new SuccessBucketTestService()).get();
		
		//make sure its still set to error
		TestQueueBean t1 = v2_test_q.getObjectById(test_entry_1._id()).get().get();
		_logger.info("Got item: " + t1._id());
		assertEquals(t1.status(), TestStatus.error); //status should no longer be submitted	
		
		//cleanup
		v2_test_q.deleteDatastore().get();										
	}

	private TestQueueBean createTestQEntry(long max_secs_to_run) throws JsonProcessingException, IOException {		
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		final JsonNode v1_source = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		
		return BeanTemplateUtils.build(TestQueueBean.class)
				.with(TestQueueBean::_id, new ObjectId().toString())
				.with(TestQueueBean::source, v1_source)
				.with(TestQueueBean::test_params, new ProcessingTestSpecBean(10L, max_secs_to_run))
				.with(TestQueueBean::status, TestStatus.submitted)
				.done().get();
	}
	
	public static class SuccessBucketTestService extends BucketTestService {
		@Override
		public ManagementFuture<Boolean> test_bucket(
				IManagementDbService core_management_db,
				DataBucketBean to_test, ProcessingTestSpecBean test_spec) {
			System.out.println("Im in SuccessBucketTestService, returning success message");
			final CompletableFuture<Boolean> future = new CompletableFuture<Boolean>();
			future.complete(true);
			return FutureUtils.createManagementFuture(future);
		}
	}
	
	public static class FailBucketTestService extends BucketTestService {
		@Override
		public ManagementFuture<Boolean> test_bucket(
				IManagementDbService core_management_db,
				DataBucketBean to_test, ProcessingTestSpecBean test_spec) {
			System.out.println("Im in FailBucketTestService, returning success message");
			final CompletableFuture<Boolean> future = new CompletableFuture<Boolean>();
			future.complete(false);
			return FutureUtils.createManagementFuture(future);
		}
	}
	
	public static class ErrorBucketTestService extends BucketTestService {
		@Override
		public ManagementFuture<Boolean> test_bucket(
				IManagementDbService core_management_db,
				DataBucketBean to_test, ProcessingTestSpecBean test_spec) {
			System.out.println("Im in ErrorBucketTestService, returning exception");
			final CompletableFuture<Boolean> future = new CompletableFuture<Boolean>();
			future.completeExceptionally(new Exception("some random error"));
			return FutureUtils.createManagementFuture(future);
		}
	}
}
