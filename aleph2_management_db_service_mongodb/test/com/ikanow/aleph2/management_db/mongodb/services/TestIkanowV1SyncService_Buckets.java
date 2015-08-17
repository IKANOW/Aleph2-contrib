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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;
import scala.Tuple3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.module.MockMongoDbManagementDbModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestIkanowV1SyncService_Buckets {
	final static protected Logger _logger = LogManager.getLogger();	
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// TEST SETUP

	@Inject 
	protected IServiceContext _service_context = null;
	
	@Inject 
	protected IkanowV1SyncService_Buckets sync_service; 

	@Inject 
	protected MongoDbManagementDbConfigBean _service_config; 

	@Before
	public void setupDependencies() throws Exception {
		try {
			final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
			
			// OK we're going to use guice, it was too painful doing this by hand...				
			Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("test_v1_sync_service.properties")))
								.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
								.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
								.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
								.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
			
			Injector app_injector = ModuleUtils.createInjector(Arrays.asList(new MockMongoDbManagementDbModule()), Optional.of(config));	
			app_injector.injectMembers(this);
		}
		catch (Throwable t) {
			System.out.println(ErrorUtils.getLongForm("{0}", t));
			throw t;
		}
	}
	
	@Test
	public void testSetup() {
		_logger.info("Starting testSetup");
		
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

	// WORKER THREADS
	
	@Test
	public void testSynchronization() throws InterruptedException, ExecutionException {
		_logger.info("Starting testSynchronization");
		
		IkanowV1SyncService_Buckets s1 = new IkanowV1SyncService_Buckets(BeanTemplateUtils.clone(_service_config).with("v1_enabled", true).done(), 
				_service_context);
		IkanowV1SyncService_Buckets s2 = new IkanowV1SyncService_Buckets(BeanTemplateUtils.clone(_service_config).with("v1_enabled", true).done(), 
				_service_context);
		IkanowV1SyncService_Buckets s3 = new IkanowV1SyncService_Buckets(BeanTemplateUtils.clone(_service_config).with("v1_enabled", true).done(), 
				_service_context);
		
		int old = IkanowV1SyncService_Buckets._num_leader_changes;
		
		s1.start(); s2.start(); s3.start();
		for (int i = 0; i < 4; ++i) {
			try { Thread.sleep(1000); } catch (Exception e) {}
		}
		s1.stop(); s2.stop(); s3.stop();
		
		assertEquals(old + 1, IkanowV1SyncService_Buckets._num_leader_changes);
		
		@SuppressWarnings("unchecked")
		final ICrudService<JsonNode> v1_config_db = _service_context.getCoreManagementDbService().getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.source")).get();				
		
		assertTrue("Query optimized", v1_config_db.deregisterOptimizedQuery(Arrays.asList("extractType")));
		
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// LOW LEVEL UTILS - PART 1
	
	@SuppressWarnings("deprecation")
	@Test
	public void testSourceToBucketConversion() throws JsonProcessingException, IOException, ParseException {
		_logger.info("Starting testSourceToBucketConversion");

		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());		
		final JsonNode v1_source = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		
		final DataBucketBean bucket = IkanowV1SyncService_Buckets.getBucketFromV1Source(v1_source);
		
		assertEquals("aleph...bucket.Template_V2_data_bucket.;", bucket._id());
		assertEquals(ImmutableMap.<String, String>builder().put("50bcd6fffbf0fd0b27875a7c", "rw").build(), bucket.access_rights().auth_token());
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
		assertEquals("week",bucket.data_schema().storage_schema().raw_grouping_time_period());
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
	public void testSourceToBucketConversion_scripting() throws JsonProcessingException, IOException, ParseException {		
		_logger.info("Starting testSourceToBucketConversion_scripting");
		
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		
		{
			final JsonNode v1_source = mapper.readTree(this.getClass().getResourceAsStream("test_scripting_1.json"));
			final DataBucketBean bucket = IkanowV1SyncService_Buckets.getBucketFromV1Source(v1_source);
			
			// (all the existing stuff)
			assertEquals("aleph...bucket.Template_V2_data_bucket.;", bucket._id());
			assertEquals(ImmutableMap.<String, String>builder().put("50bcd6fffbf0fd0b27875a7c", "rw").build(), bucket.access_rights().auth_token());
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
			final DataBucketBean bucket = IkanowV1SyncService_Buckets.getBucketFromV1Source(v1_source);
			
			// (all the existing stuff)
			assertEquals("aleph...bucket.Template_V2_data_bucket.;", bucket._id());
			assertEquals(ImmutableMap.<String, String>builder().put("50bcd6fffbf0fd0b27875a7c", "rw").build(), bucket.access_rights().auth_token());
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

	// CONTROL LOGIC
	
	@SuppressWarnings("deprecation")
	@Test
	public void test_compareSourcesToBuckets_categorize() throws ParseException {
		_logger.info("Starting test_compareSourcesToBuckets_categorize");
		
		final String same_date = "21 May 2015 02:37:23 GMT";
		
		//Map<String, String>
		final Map<String, String> v1_side = ImmutableMap.<String, String>builder()
										.put("v1_not_v2_1", new Date().toGMTString())
										.put("v1_not_v2_2", new Date().toGMTString())
										.put("v1_not_v2_3", "") //(ignored because null ie notApproved)
										.put("v1_and_v2_same_1", same_date)
										.put("v1_and_v2_same_2", same_date)
										.put("v1_and_v2_mod_1", new Date().toGMTString())
										.put("v1_and_v2_mod_2", new Date().toGMTString())
										.put("v1_and_v2_ignore", "") //(ignored because null ie notApproved)
										.build();

		final Map<String, Date> v2_side = ImmutableMap.<String, Date>builder() 
										.put("v2_not_v1_1", new Date())
										.put("v2_not_v1_2", new Date())
										.put("v1_and_v2_same_1", IkanowV1SyncService_Buckets.parseJavaDate(same_date))
										.put("v1_and_v2_same_2", IkanowV1SyncService_Buckets.parseJavaDate(same_date))
										.put("v1_and_v2_mod_1", IkanowV1SyncService_Buckets.parseJavaDate(same_date))
										.put("v1_and_v2_mod_2", IkanowV1SyncService_Buckets.parseJavaDate(same_date))
										.put("v1_and_v2_ignore", IkanowV1SyncService_Buckets.parseJavaDate(same_date))
										.build();
		
		final Tuple3<Collection<String>, Collection<String>, Collection<String>> result = 
				IkanowV1SyncService_Buckets.compareSourcesToBuckets_categorize(Tuples._2T(v1_side, v2_side));
		
		final List<String> expected_create = Arrays.asList("v1_not_v2_1", "v1_not_v2_2"); 
		final List<String> expected_delete = Arrays.asList("v2_not_v1_2", "v2_not_v1_1");
		final List<String> expected_update = Arrays.asList("v1_and_v2_mod_2", "v1_and_v2_mod_1"); // (order is wonky because hashset..)
		
		assertEquals(expected_create, Arrays.asList(result._1().toArray()));
		assertEquals(expected_delete, Arrays.asList(result._2().toArray()));
		assertEquals(expected_update, Arrays.asList(result._3().toArray()));
	}

	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// DB INTEGRATION - READ
	
	@Test
	public void test_compareSourcesToBuckets_get() throws JsonProcessingException, IOException, ParseException, InterruptedException, ExecutionException {
		_logger.info("Starting test_compareSourcesToBuckets_get");
				
		@SuppressWarnings("unchecked")
		ICrudService<JsonNode> v1_source_db = this._service_context.getCoreManagementDbService()
																	.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.source")).get();
		
		v1_source_db.deleteDatastore().get();
		
		IManagementCrudService<DataBucketBean> bucket_db = this._service_context.getCoreManagementDbService().getDataBucketStore();
		
		bucket_db.deleteDatastore().get();
		
		// Create 2 V1 sources
		
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());		
		
		final JsonNode v1_source_1 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		final JsonNode v1_source_2 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		final JsonNode v1_source_3 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		
		((ObjectNode)v1_source_2).set("_id", null);
		((ObjectNode)v1_source_2).set("key", new TextNode("aleph...bucket.Template_V2_data_bucket.2"));

		((ObjectNode)v1_source_3).set("_id", null);
		((ObjectNode)v1_source_3).set("key", new TextNode("aleph...bucket.Template_V2_data_bucket.3"));
		((ObjectNode)v1_source_3).set("isApproved", BooleanNode.FALSE);
		
		assertEquals(0L, (long)v1_source_db.countObjects().get());
		v1_source_db.storeObjects(Arrays.asList(v1_source_1, v1_source_2, v1_source_3)).get();
		assertEquals(3L, (long)v1_source_db.countObjects().get());
		
		// Create 2 buckets
		
		final DataBucketBean bucket1 = IkanowV1SyncService_Buckets.getBucketFromV1Source(v1_source_1);
		final DataBucketBean bucket2 = IkanowV1SyncService_Buckets.getBucketFromV1Source(v1_source_2);

		assertEquals(0L, (long)bucket_db.countObjects().get());
		bucket_db.storeObjects(Arrays.asList(bucket1, bucket2)).get();
		assertEquals(2L, (long)bucket_db.countObjects().get());

		// Run the function under test
		
		final Tuple2<Map<String, String>, Map<String, Date>> f_res = 
				IkanowV1SyncService_Buckets.compareSourcesToBuckets_get(bucket_db, v1_source_db).get();
				
		assertEquals("{aleph...bucket.Template_V2_data_bucket.=May 25, 2015 01:52:01 PM UTC, aleph...bucket.Template_V2_data_bucket.3=, aleph...bucket.Template_V2_data_bucket.2=May 25, 2015 01:52:01 PM UTC}", f_res._1().toString());

		assertEquals(2, f_res._2().size());
		assertEquals(true, f_res._1().containsKey("aleph...bucket.Template_V2_data_bucket."));
		assertEquals(true, f_res._1().containsKey("aleph...bucket.Template_V2_data_bucket.2"));		
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// DB INTEGRATION - WRITE
	
	private BasicMessageBean buildMessage(String source, String command, boolean success, String message) throws ParseException {
		final String some_date = "21 May 2015 02:37:23 GMT";
		return new BasicMessageBean(
				IkanowV1SyncService_Buckets.parseJavaDate(some_date), success, source, command, null, message, null
				);
	}
	
	@Test
	public void test_updateV1SourceStatus() throws JsonProcessingException, IOException, InterruptedException, ExecutionException, ParseException {
		_logger.info("Starting test_updateV1SourceStatus");
		
		final Date some_date_str = IkanowV1SyncService_Buckets.parseJavaDate("21 May 2015 02:38:23 GMT");
		
		@SuppressWarnings("unchecked")
		ICrudService<JsonNode> v1_source_db = this._service_context.getCoreManagementDbService()
												.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.source")).get();
		
		v1_source_db.deleteDatastore().get();
		
		// Create 2 V1 sources
		
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());		
		
		final JsonNode v1_source_1 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		
		assertEquals(0L, (long)v1_source_db.countObjects().get());
		v1_source_db.storeObjects(Arrays.asList(v1_source_1)).get();
		assertEquals(1L, (long)v1_source_db.countObjects().get());

		// Test1 failure - !disable on failure
		
		final Collection<BasicMessageBean> test1 = Arrays.asList(
				buildMessage("test_src1", "test_cmd1", true, "test_msg1"),
				buildMessage("test_src2", "test_cmd2", false, "test_msg2")
				);
		
		
		IkanowV1SyncService_Buckets.updateV1SourceStatus(some_date_str, "aleph...bucket.Template_V2_data_bucket.", test1, false, v1_source_db).get();
		
		assertEquals(1L, (long)v1_source_db.countObjects().get());
		
		final Optional<JsonNode> res1 = v1_source_db.getObjectBySpec(CrudUtils.anyOf().when("key", "aleph...bucket.Template_V2_data_bucket.")).get();
		
		assertTrue("Got source", res1.isPresent());
		assertEquals(true, res1.get().get("isApproved").asBoolean());
		assertEquals("{'harvest_status':'error','harvest_message':'[DATE] Bucket synchronization:\\n[DATE] test_src1 (test_cmd1): INFO: test_msg1\\n[DATE] test_src2 (test_cmd2): ERROR: test_msg2'}", 
				res1.get().get("harvest").toString().replace("\"", "'").replaceAll("\\[.*?\\]", "[DATE]"));

		// Test1b failure - disable on failure
		
		final Optional<JsonNode> res1b = v1_source_db.getObjectBySpec(CrudUtils.anyOf().when("key", "aleph...bucket.Template_V2_data_bucket.")).get();
		
		assertTrue("Got source", res1b.isPresent());
		assertEquals(true, res1b.get().get("isApproved").asBoolean());
		assertEquals("{'harvest_status':'error','harvest_message':'[DATE] Bucket synchronization:\\n[DATE] test_src1 (test_cmd1): INFO: test_msg1\\n[DATE] test_src2 (test_cmd2): ERROR: test_msg2'}", 
				res1b.get().get("harvest").toString().replace("\"", "'").replaceAll("\\[.*?\\]", "[DATE]"));

		// Test2 success
		
		final Collection<BasicMessageBean> test2 = Arrays.asList(
				buildMessage("test_src1", "test_cmd1", true, "test_msg1"),
				buildMessage("test_src2", "test_cmd2", true, "test_msg2")
				);
		
		
		IkanowV1SyncService_Buckets.updateV1SourceStatus(some_date_str, "aleph...bucket.Template_V2_data_bucket.", test2, true, v1_source_db).get();
		
		assertEquals(1L, (long)v1_source_db.countObjects().get());
		
		final Optional<JsonNode> res2 = v1_source_db.getObjectBySpec(CrudUtils.anyOf().when("key", "aleph...bucket.Template_V2_data_bucket.")).get();
		
		assertTrue("Got source", res2.isPresent());
		assertEquals(true, res2.get().get("isApproved").asBoolean());
		assertEquals("{'harvest_status':'success','harvest_message':'[DATE] Bucket synchronization:\\n[DATE] test_src1 (test_cmd1): INFO: test_msg1\\n[DATE] test_src2 (test_cmd2): INFO: test_msg2'}", 
				res2.get().get("harvest").toString().replace("\"", "'").replaceAll("\\[.*?\\]", "[DATE]"));		

		// Test 3 empty message
		
		final Collection<BasicMessageBean> test3 = Arrays.asList();
		
		IkanowV1SyncService_Buckets.updateV1SourceStatus(some_date_str, "aleph...bucket.Template_V2_data_bucket.", test3, false, v1_source_db).get();
		
		assertEquals(1L, (long)v1_source_db.countObjects().get());
		
		final Optional<JsonNode> res3 = v1_source_db.getObjectBySpec(CrudUtils.anyOf().when("key", "aleph...bucket.Template_V2_data_bucket.")).get();
		
		assertTrue("Got source", res3.isPresent());
		assertEquals(true, res3.get().get("isApproved").asBoolean());
		assertEquals("{'harvest_status':'success','harvest_message':'[21 May 2015 02:38:23 GMT] Bucket synchronization:\\n(no messages)'}", 
				res3.get().get("harvest").toString().replace("\"", "'"));		
	}
		
	@Test
	public void test_updateBucket() throws JsonProcessingException, IOException, InterruptedException, ExecutionException, ParseException {
		_logger.info("Starting test_updateBucket");
				
		@SuppressWarnings("unchecked")
		ICrudService<JsonNode> v1_source_db = this._service_context.getService(IManagementDbService.class, Optional.empty()).get()
																	.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.source")).get();
		
		v1_source_db.deleteDatastore().get();
		
		IManagementCrudService<DataBucketBean> bucket_db = this._service_context.getCoreManagementDbService().getDataBucketStore();		
		bucket_db.deleteDatastore().get();
		
		IManagementCrudService<DataBucketStatusBean> bucket_status_db = this._service_context.getCoreManagementDbService().getDataBucketStatusStore();		
		bucket_status_db.deleteDatastore().get();
		
		// Create 2 V1 sources
		
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());		
		
		final JsonNode v1_source_1 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		final JsonNode v1_source_2 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		
		((ObjectNode)v1_source_2).set("_id", null);
		((ObjectNode)v1_source_2).set("key", new TextNode("aleph...bucket.Template_V2_data_bucket.2"));

		// Create 2 buckets
		
		final DataBucketBean bucket1 = IkanowV1SyncService_Buckets.getBucketFromV1Source(v1_source_1);
		final DataBucketBean bucket2 = IkanowV1SyncService_Buckets.getBucketFromV1Source(v1_source_2);

		assertEquals(0L, (long)bucket_db.countObjects().get());
		bucket_db.storeObjects(Arrays.asList(bucket1, bucket2)).get();
		assertEquals(2L, (long)bucket_db.countObjects().get());

		//(store status)
		
		final DataBucketStatusBean bucket_status1 = BeanTemplateUtils.build(DataBucketStatusBean.class)
														.with(DataBucketStatusBean::_id, bucket1._id())
														.with(DataBucketStatusBean::suspended, false)
														.with(DataBucketStatusBean::bucket_path, bucket1.full_name())
														.done().get();
		
		final DataBucketStatusBean bucket_status2 = BeanTemplateUtils.build(DataBucketStatusBean.class)
				.with(DataBucketStatusBean::_id, bucket2._id())
				.with(DataBucketStatusBean::suspended, true)
				.with(DataBucketStatusBean::bucket_path, bucket2.full_name())
				.done().get();

		assertEquals(0L, (long)bucket_status_db.countObjects().get());
		bucket_status_db.storeObjects(Arrays.asList(bucket_status1, bucket_status2)).get();
		assertEquals(2L, (long)bucket_status_db.countObjects().get());		
		
		// Mod + save sources
		
		((ObjectNode)v1_source_1).set("searchCycle_secs", new IntNode(-1));
		((ObjectNode)v1_source_1).set("description", new TextNode("NEW DESCRIPTION"));
		
		assertEquals(0L, (long)v1_source_db.countObjects().get());
		v1_source_db.storeObjects(Arrays.asList(v1_source_1)).get(); // (onyl source 1, source 2 used to demo error)
		assertEquals(1L, (long)v1_source_db.countObjects().get());
		
		// Run the function under test

		// Test1 - succeeds
		
		final ManagementFuture<Supplier<Object>> res_1 =
				IkanowV1SyncService_Buckets.updateBucket("aleph...bucket.Template_V2_data_bucket.", bucket_db, bucket_status_db, v1_source_db);
		
		assertEquals(bucket1._id(), res_1.get().get());
		assertEquals(0, res_1.getManagementResults().get().size());
		
		assertEquals(2L, (long)bucket_db.countObjects().get());		
		assertEquals(2L, (long)bucket_status_db.countObjects().get());				
		
		final Optional<DataBucketStatusBean> status = bucket_status_db.getObjectById(bucket1._id()).get();
		assertEquals(true, status.get().suspended());

		final Optional<DataBucketBean> updated_bucket = bucket_db.getObjectById(bucket1._id()).get();
		assertEquals("NEW DESCRIPTION", updated_bucket.get().description());
		assertEquals(bucket1.display_name(), updated_bucket.get().display_name());
		assertEquals(bucket1.tags(), updated_bucket.get().tags());
		assertEquals(bucket1.full_name(), updated_bucket.get().full_name());
		
		// Test 2 - error because source_2 not in DB any more
		
		final ManagementFuture<Supplier<Object>> res_2 =
				IkanowV1SyncService_Buckets.updateBucket("aleph...bucket.Template_V2_data_bucket.2", bucket_db, bucket_status_db, v1_source_db);
		
		try {
			res_2.get();
			fail("Should have errored");
		}
		catch (Exception e) {}
		assertEquals(1, res_2.getManagementResults().get().size());
		assertEquals(false, res_2.getManagementResults().get().iterator().next().success());		
	}
	
	@Test
	public void deleteBucket() throws JsonProcessingException, IOException, InterruptedException, ExecutionException, ParseException {
		_logger.info("Starting deleteBucket");
				
		@SuppressWarnings("unchecked")
		ICrudService<JsonNode> v1_source_db = this._service_context.getCoreManagementDbService()
																	.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.source")).get();
		
		v1_source_db.deleteDatastore().get();
		
		IManagementCrudService<DataBucketBean> bucket_db = this._service_context.getCoreManagementDbService().getDataBucketStore();		
		bucket_db.deleteDatastore().get();
		
		IManagementCrudService<DataBucketStatusBean> bucket_status_db = this._service_context.getCoreManagementDbService().getDataBucketStatusStore();		
		bucket_status_db.deleteDatastore().get();
		
		// Create 2 V1 sources
		
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());		
		
		final JsonNode v1_source_1 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		final JsonNode v1_source_2 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		
		((ObjectNode)v1_source_2).set("_id", null);
		((ObjectNode)v1_source_2).set("key", new TextNode("aleph...bucket.Template_V2_data_bucket.2"));

		// Create 2 buckets
		
		final DataBucketBean bucket1 = IkanowV1SyncService_Buckets.getBucketFromV1Source(v1_source_1);
		final DataBucketBean bucket2 = IkanowV1SyncService_Buckets.getBucketFromV1Source(v1_source_2);

		assertEquals(0L, (long)bucket_db.countObjects().get());
		bucket_db.storeObjects(Arrays.asList(bucket1, bucket2)).get();
		assertEquals(2L, (long)bucket_db.countObjects().get());

		//(store status)
		
		final DataBucketStatusBean bucket_status1 = BeanTemplateUtils.build(DataBucketStatusBean.class)
														.with(DataBucketStatusBean::_id, bucket1._id())
														.with(DataBucketStatusBean::suspended, false)
														.with(DataBucketStatusBean::bucket_path, bucket1.full_name())
														.done().get();
		
		final DataBucketStatusBean bucket_status2 = BeanTemplateUtils.build(DataBucketStatusBean.class)
				.with(DataBucketStatusBean::_id, bucket2._id())
				.with(DataBucketStatusBean::suspended, true)
				.with(DataBucketStatusBean::bucket_path, bucket2.full_name())
				.done().get();

		assertEquals(0L, (long)bucket_status_db.countObjects().get());
		bucket_status_db.storeObjects(Arrays.asList(bucket_status1, bucket_status2)).get();
		assertEquals(2L, (long)bucket_status_db.countObjects().get());		
		
		final ManagementFuture<Boolean> f_res = IkanowV1SyncService_Buckets.deleteBucket("aleph...bucket.Template_V2_data_bucket.", bucket_db);
		
		assertEquals(true, f_res.get());
		assertEquals(0, f_res.getManagementResults().get().size());
		
		// Check if got deleted....
		
		assertEquals(false, bucket_db.getObjectById(IkanowV1SyncService_Buckets.getBucketIdFromV1SourceKey("aleph...bucket.Template_V2_data_bucket.")).get().isPresent());
		// (would normally test bucket status here - but it won't be changed because test uses underlying_mgmt_db as core_mgmt_db for circular dep issues in maven)
	}	
	
	@Test
	public void test_createNewBucket() throws JsonProcessingException, IOException, InterruptedException, ExecutionException, ParseException {
		_logger.info("Starting test_createNewBucket");		
		
		@SuppressWarnings("unchecked")
		ICrudService<JsonNode> v1_source_db = this._service_context.getCoreManagementDbService()
																	.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.source")).get();
		
		v1_source_db.deleteDatastore().get();
		
		IManagementCrudService<DataBucketBean> bucket_db = this._service_context.getCoreManagementDbService().getDataBucketStore();		
		bucket_db.deleteDatastore().get();
		
		IManagementCrudService<DataBucketStatusBean> bucket_status_db = this._service_context.getCoreManagementDbService().getDataBucketStatusStore();		
		bucket_status_db.deleteDatastore().get();
		
		// Create 2 V1 sources
		
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());		
		
		final JsonNode v1_source_1 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		final JsonNode v1_source_2 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		
		((ObjectNode)v1_source_2).set("_id", null);
		((ObjectNode)v1_source_2).set("key", new TextNode("aleph...bucket.Template_V2_data_bucket.2"));

		// Create 2 buckets
		
		assertEquals(0L, (long)bucket_db.countObjects().get());
		assertEquals(0L, (long)bucket_status_db.countObjects().get());
		
		// Save sources
		
		((ObjectNode)v1_source_1).set("searchCycle_secs", new IntNode(-1));
		((ObjectNode)v1_source_1).set("description", new TextNode("NEW DESCRIPTION"));
		
		assertEquals(0L, (long)v1_source_db.countObjects().get());
		v1_source_db.storeObjects(Arrays.asList(v1_source_1, v1_source_2)).get(); 
		assertEquals(2L, (long)v1_source_db.countObjects().get());
		
		final ManagementFuture<Supplier<Object>> f_res = IkanowV1SyncService_Buckets.createNewBucket("aleph...bucket.Template_V2_data_bucket.", 
																			bucket_db, bucket_status_db,
																			v1_source_db);

		assertEquals(IkanowV1SyncService_Buckets.getBucketIdFromV1SourceKey("aleph...bucket.Template_V2_data_bucket."), f_res.get().get());
		assertEquals(0, f_res.getManagementResults().get().size());
		
		assertEquals("Should have only 1 bucket", 1L, 
				(long)bucket_db.countObjectsBySpec(CrudUtils.allOf(DataBucketBean.class).when("_id", f_res.get().get())).get());
		assertEquals("Should have only 1 bucket status", 1L, 
				(long)bucket_status_db.countObjectsBySpec(CrudUtils.allOf(DataBucketStatusBean.class).when("_id", f_res.get().get())).get());
		
		final Optional<DataBucketStatusBean> status = bucket_status_db.getObjectById(IkanowV1SyncService_Buckets.getBucketIdFromV1SourceKey("aleph...bucket.Template_V2_data_bucket.")).get();
		assertEquals(true, status.get().suspended());

		final Optional<DataBucketBean> bucket = bucket_db.getObjectById(IkanowV1SyncService_Buckets.getBucketIdFromV1SourceKey("aleph...bucket.Template_V2_data_bucket.")).get();

		final DataBucketBean exp_bucket = IkanowV1SyncService_Buckets.getBucketFromV1Source(v1_source_1);
		//(check a couple of fields)
		assertEquals(exp_bucket.description(), bucket.get().description());
		assertEquals(exp_bucket.full_name(), bucket.get().full_name());
		
		// Error case
		
		final ManagementFuture<Supplier<Object>> res_2 = IkanowV1SyncService_Buckets.createNewBucket("aleph...bucket.Template_V2_data_bucket.X", 
				bucket_db, bucket_status_db,
				v1_source_db
		);
		try {
			res_2.get();
			fail("Should have errored");
		}
		catch (Exception e) {}
		assertEquals("Should only have 1 management result: " + 
				res_2.getManagementResults().get().stream().map(BasicMessageBean::message).collect(Collectors.joining()), 
				1, res_2.getManagementResults().get().size());
		assertEquals(false, res_2.getManagementResults().get().iterator().next().success());		
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// CONTROL CODE - PART 2
	
	@SuppressWarnings("deprecation")
	@Test
	public void test_puttingItAllTogether() throws JsonProcessingException, IOException, ParseException, InterruptedException, ExecutionException {
		_logger.info("Starting test_puttingItAllTogether");		
		
		// Set up 3 different scenarios:
		// 1 - doc to be deleted
		// 1 - doc to be updated (+1 that would be updated if it was non-approveD)
		// 1 - doc to be created (+1 that would be created if it was non-approveD)
		
		
		@SuppressWarnings("unchecked")
		ICrudService<JsonNode> v1_source_db = this._service_context.getService(IManagementDbService.class, Optional.empty()).get()
																	.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.source")).get();
		
		v1_source_db.deleteDatastore().get();
		
		IManagementCrudService<DataBucketBean> bucket_db = this._service_context.getCoreManagementDbService().getDataBucketStore();		
		bucket_db.deleteDatastore().get();
		
		IManagementCrudService<DataBucketStatusBean> bucket_status_db = this._service_context.getCoreManagementDbService().getDataBucketStatusStore();		
		bucket_status_db.deleteDatastore().get();
		
		// Create 3 V1 sources (only going to save 1 of them)
		
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());		
		
		final JsonNode v1_source_1 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		final JsonNode v1_source_2 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		final JsonNode v1_source_3 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		final JsonNode v1_source_4 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		final JsonNode v1_source_5 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_source.json"));
		
		((ObjectNode)v1_source_2).set("_id", null);
		((ObjectNode)v1_source_2).set("key", new TextNode("aleph...bucket.Template_V2_data_bucket.2"));

		// (not saving this one it's just a template)
		((ObjectNode)v1_source_3).set("_id", null);
		((ObjectNode)v1_source_3).set("key", new TextNode("aleph...bucket.Template_V2_data_bucket.3"));
		
		// (disabled one)
		((ObjectNode)v1_source_4).set("_id", null);
		((ObjectNode)v1_source_4).set("key", new TextNode("aleph...bucket.Template_V2_data_bucket.4"));
		((ObjectNode)v1_source_4).set("isApproved", BooleanNode.FALSE);

		// (disabled one with matching bucket)
		((ObjectNode)v1_source_5).set("_id", null);
		((ObjectNode)v1_source_5).set("key", new TextNode("aleph...bucket.Template_V2_data_bucket.5"));
		((ObjectNode)v1_source_5).set("isApproved", BooleanNode.FALSE);
		
		// Create 3 buckets
		
		final DataBucketBean bucket1 = IkanowV1SyncService_Buckets.getBucketFromV1Source(v1_source_1);
		final DataBucketBean bucket3 = IkanowV1SyncService_Buckets.getBucketFromV1Source(v1_source_3);
		final DataBucketBean bucket5 = IkanowV1SyncService_Buckets.getBucketFromV1Source(v1_source_5);

		assertEquals(0L, (long)bucket_db.countObjects().get());
		bucket_db.storeObjects(Arrays.asList(bucket1, bucket3, bucket5)).get();
		assertEquals(3L, (long)bucket_db.countObjects().get());

		//(store status)
		
		final DataBucketStatusBean bucket_status1 = BeanTemplateUtils.build(DataBucketStatusBean.class)
														.with(DataBucketStatusBean::_id, bucket1._id())
														.with(DataBucketStatusBean::suspended, false)
														.with(DataBucketStatusBean::bucket_path, bucket1.full_name())
														.done().get();
		
		final DataBucketStatusBean bucket_status3 = BeanTemplateUtils.build(DataBucketStatusBean.class)
				.with(DataBucketStatusBean::_id, bucket3._id())
				.with(DataBucketStatusBean::suspended, true)
				.with(DataBucketStatusBean::bucket_path, bucket3.full_name())
				.done().get();

		final DataBucketStatusBean bucket_status5 = BeanTemplateUtils.build(DataBucketStatusBean.class)
				.with(DataBucketStatusBean::_id, bucket5._id())
				.with(DataBucketStatusBean::suspended, true)
				.with(DataBucketStatusBean::bucket_path, bucket5.full_name())
				.done().get();
		
		assertEquals(0L, (long)bucket_status_db.countObjects().get());
		bucket_status_db.storeObjects(Arrays.asList(bucket_status1, bucket_status3, bucket_status5)).get();
		assertEquals(3L, (long)bucket_status_db.countObjects().get());		
		
		// Mod + save sources
		
		((ObjectNode)v1_source_1).set("modified", new TextNode(new Date().toGMTString()));
		((ObjectNode)v1_source_1).set("searchCycle_secs", new IntNode(-1));
		((ObjectNode)v1_source_1).set("description", new TextNode("NEW DESCRIPTION"));

		((ObjectNode)v1_source_5).set("modified", new TextNode(new Date().toGMTString()));
		
		
		assertEquals(0L, (long)v1_source_db.countObjects().get());
		v1_source_db.storeObjects(Arrays.asList(v1_source_1, v1_source_2, v1_source_4, v1_source_5)).get(); 
		assertEquals(4L, (long)v1_source_db.countObjects().get());		

		// OK now fire off an instance of the runner
		
		IkanowV1SyncService_Buckets s1 = new IkanowV1SyncService_Buckets(BeanTemplateUtils.clone(_service_config).with("v1_enabled", true).done(), 
				_service_context);
		
		int old = IkanowV1SyncService_Buckets._num_leader_changes;
		s1.start();
		for (int i = 0; i < 4; ++i) {
			try { Thread.sleep(1000); } catch (Exception e) {}
		}
		s1.stop();

		assertEquals(old + 1, IkanowV1SyncService_Buckets._num_leader_changes);
		
		// Check a few things have happened:
		
		// 1) bucket3 has been deleted
		
		assertEquals(false, bucket_db.getObjectById(IkanowV1SyncService_Buckets.getBucketIdFromV1SourceKey("aleph...bucket.Template_V2_data_bucket.3")).get().isPresent());
		
		// 2) bucket2 has been created
		
		assertEquals(true, bucket_db.getObjectById(IkanowV1SyncService_Buckets.getBucketIdFromV1SourceKey("aleph...bucket.Template_V2_data_bucket.2")).get().isPresent());
		
		// 3) bucket1 has been updated
		
		final Optional<DataBucketStatusBean> status = bucket_status_db.getObjectById(bucket1._id()).get();
		assertEquals(true, status.get().suspended());

		final Optional<DataBucketBean> updated_bucket = bucket_db.getObjectById(bucket1._id()).get();
		assertEquals("NEW DESCRIPTION", updated_bucket.get().description());
		assertEquals(bucket1.display_name(), updated_bucket.get().display_name());
		assertEquals(bucket1.tags(), updated_bucket.get().tags());
		assertEquals(bucket1.full_name(), updated_bucket.get().full_name());
		
		// 4) Check counts quickly
		
		assertEquals(4L, (long)bucket_status_db.countObjects().get());
		//(this should be 3 but we're using the wrong db for maven reasons so the proxy doesn't occur)
		assertEquals(3L, (long)bucket_db.countObjects().get());		
		assertEquals(4L, (long)v1_source_db.countObjects().get());
		
		// 5) Check v1 statuses have been updated...
		final Optional<JsonNode> res1 = v1_source_db.getObjectBySpec(CrudUtils.anyOf().when("key", "aleph...bucket.Template_V2_data_bucket.")).get();
		assertEquals("{'harvest_status':'success','harvest_message':'[DATE] Bucket synchronization:\\n(no messages)'}", 
				res1.get().get("harvest").toString().replace("\"", "'").replaceAll("\\[.*?\\]", "[DATE]"));		
		
		final Optional<JsonNode> res2 = v1_source_db.getObjectBySpec(CrudUtils.anyOf().when("key", "aleph...bucket.Template_V2_data_bucket.2")).get();
		assertEquals("{'harvest_status':'success','harvest_message':'[DATE] Bucket synchronization:\\n(no messages)'}", 
				res2.get().get("harvest").toString().replace("\"", "'").replaceAll("\\[.*?\\]", "[DATE]"));		
		
	}	
}
