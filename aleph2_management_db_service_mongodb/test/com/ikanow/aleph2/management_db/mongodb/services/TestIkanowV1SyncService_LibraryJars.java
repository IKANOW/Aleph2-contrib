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

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import scala.Tuple2;
import scala.Tuple3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.module.MockMongoDbManagementDbModule;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.util.JSON;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import fj.Unit;

public class TestIkanowV1SyncService_LibraryJars {

	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// TEST SETUP

	protected ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
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
	public void test_synchronization() throws InterruptedException, ExecutionException {
		
		IkanowV1SyncService_LibraryJars s1 = new IkanowV1SyncService_LibraryJars(BeanTemplateUtils.clone(_service_config).with("v1_enabled", true).done(), 
				_service_context);
		IkanowV1SyncService_LibraryJars s2 = new IkanowV1SyncService_LibraryJars(BeanTemplateUtils.clone(_service_config).with("v1_enabled", true).done(), 
				_service_context);
		IkanowV1SyncService_LibraryJars s3 = new IkanowV1SyncService_LibraryJars(BeanTemplateUtils.clone(_service_config).with("v1_enabled", true).done(), 
				_service_context);
		
		int old = IkanowV1SyncService_LibraryJars._num_leader_changes;
		
		s1.start(); s2.start(); s3.start();
		for (int i = 0; i < 4; ++i) {
			try { Thread.sleep(1000); } catch (Exception e) {}
		}
		s1.stop(); s2.stop(); s3.stop();
		
		assertEquals(old + 1, IkanowV1SyncService_LibraryJars._num_leader_changes);
		
		@SuppressWarnings("unchecked")
		final ICrudService<JsonNode> v1_config_db = _service_context.getCoreManagementDbService().getUnderlyingPlatformDriver(ICrudService.class, Optional.of("social.share")).get();				
		
		assertTrue("Query optimized", v1_config_db.deregisterOptimizedQuery(Arrays.asList("title")));
		
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// LOW LEVEL UTILS - PART 1
	
	public static JsonNode getLibraryMetadata(Object resource, List<String> desc) throws Exception {
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());		
		final JsonNode v1_bean = 
				((ObjectNode) mapper.readTree(resource.getClass().getResourceAsStream("test_v1_sync_sample_share.json")))
				.put("description", desc.stream().collect(Collectors.joining("\n")));
				
		return v1_bean;
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void test_shareToLibraryConversion() throws Exception {
		// FIRST OFF CHECK ALL THE FIELDS
		
		{
			final JsonNode share1 = getLibraryMetadata(this, Arrays.asList(
					"com.ikanow.aleph2.test.EntryPoint",
					"This is a description.",
					"More description."
					//(no tags)
					));
			
			SharedLibraryBean lib1 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(share1);
			assertEquals("v1_555d44e3347d336b3e8c4cbe", lib1._id());
			assertEquals("21 May 2015 02:37:23 GMT", lib1.created().toGMTString());
			assertEquals("21 May 2015 02:37:24 GMT", lib1.modified().toGMTString());
			assertEquals(null, lib1.batch_enrichment_entry_point());
			assertEquals("This is a description.\nMore description.", lib1.description());
			assertEquals("/app/aleph2/library/misc/library.jar", lib1.display_name());
			assertEquals(null, lib1.library_config());
			assertEquals("com.ikanow.aleph2.test.EntryPoint", lib1.misc_entry_point());
			assertEquals("455d44e3347d336b3e8c4cbe", lib1.owner_id());
			assertEquals("/app/aleph2/library/misc/library.jar", lib1.path_name());
			assertEquals(null, lib1.streaming_enrichment_entry_point());
			assertEquals(null, lib1.subtype());
			assertEquals(ImmutableSet.builder().build(), lib1.tags());
		}
		
		// NOW A BUNCH OF ONES WHERE WE'LL JUST CHECK THE DESC/TAGS/ENTRY POINT

		//(desc, tags)
		{
			final JsonNode share2 = getLibraryMetadata(this, Arrays.asList(
					"com.ikanow.aleph2.test.EntryPoint",
					"This is a description.",
					"More description.",
					"tags:tag1,tag2"
					));
			
			SharedLibraryBean lib2 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(share2);
			assertEquals("This is a description.\nMore description.", lib2.description());
			assertEquals(null, lib2.library_config());
			assertEquals(ImmutableSet.builder().add("tag1").add("tag2").build(), lib2.tags());
		}		

		//(desc, tags, multi-line JSON)
		{
			final JsonNode share2 = getLibraryMetadata(this, Arrays.asList(
					"com.ikanow.aleph2.test.EntryPoint",
					"{",
					"   \"test\": {",
					"      \"test2\": \"test_val\"",
					"   }",
					"}",
					"This is a description.",
					"More description.",
					"tags:tag1,tag2"
					));
			
			SharedLibraryBean lib2 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(share2);
			assertEquals("This is a description.\nMore description.", lib2.description());
			assertEquals("{\"test\":{\"test2\":\"test_val\"}}", _mapper.convertValue(lib2.library_config(), JsonNode.class).toString());
			assertEquals(ImmutableSet.builder().add("tag1").add("tag2").build(), lib2.tags());
		}		

		//(desc, no tags, multi-line JSON)
		{
			final JsonNode share2 = getLibraryMetadata(this, Arrays.asList(
					"com.ikanow.aleph2.test.EntryPoint",
					"{",
					"   \"test\": {",
					"      \"test2\": \"test_val\"",
					"   }",
					"}",
					"This is a description.",
					"More description."
					));
			
			SharedLibraryBean lib2 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(share2);
			assertEquals("This is a description.\nMore description.", lib2.description());
			assertEquals("{\"test\":{\"test2\":\"test_val\"}}", _mapper.convertValue(lib2.library_config(), JsonNode.class).toString());
			assertEquals(ImmutableSet.builder().build(), lib2.tags());
		}		

		//(NO desc, no tags, multi-line JSON)
		{
			final JsonNode share2 = getLibraryMetadata(this, Arrays.asList(
					"com.ikanow.aleph2.test.EntryPoint",
					"{",
					"   \"test\": {",
					"      \"test2\": \"test_val\"",
					"   }",
					"}"
					));
			
			SharedLibraryBean lib2 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(share2);
			assertEquals("", lib2.description());
			assertEquals("{\"test\":{\"test2\":\"test_val\"}}", _mapper.convertValue(lib2.library_config(), JsonNode.class).toString());
			assertEquals(ImmutableSet.builder().build(), lib2.tags());
		}		
		
		//(desc, tags, single-line JSON)
		{
			final JsonNode share2 = getLibraryMetadata(this, Arrays.asList(
					"com.ikanow.aleph2.test.EntryPoint",
					"{\"test\":{\"test2\":\"test_val\"}}",
					"This is a description.",
					"More description.",
					"tags:tag1,tag2"
					));
			
			SharedLibraryBean lib2 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(share2);
			assertEquals("This is a description.\nMore description.", lib2.description());
			assertEquals("{\"test\":{\"test2\":\"test_val\"}}", _mapper.convertValue(lib2.library_config(), JsonNode.class).toString());
			assertEquals(ImmutableSet.builder().add("tag1").add("tag2").build(), lib2.tags());
		}		

		//(NO desc, tags, single-line JSON)
		{
			final JsonNode share2 = getLibraryMetadata(this, Arrays.asList(
					"com.ikanow.aleph2.test.EntryPoint",
					"{\"test\":{\"test2\":\"test_val\"}}",
					"tags:tag1,tag2"
					));
			
			SharedLibraryBean lib2 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(share2);
			assertEquals("", lib2.description());
			assertEquals("{\"test\":{\"test2\":\"test_val\"}}", _mapper.convertValue(lib2.library_config(), JsonNode.class).toString());
			assertEquals(ImmutableSet.builder().add("tag1").add("tag2").build(), lib2.tags());
		}		

		//(desc, no tags, single-line JSON)
		{
			final JsonNode share2 = getLibraryMetadata(this, Arrays.asList(
					"com.ikanow.aleph2.test.EntryPoint",
					"{\"test\":{\"test2\":\"test_val\"}}",
					"This is a description.",
					"More description."
					));
			
			SharedLibraryBean lib2 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(share2);
			assertEquals("This is a description.\nMore description.", lib2.description());
			assertEquals("{\"test\":{\"test2\":\"test_val\"}}", _mapper.convertValue(lib2.library_config(), JsonNode.class).toString());
			assertEquals(ImmutableSet.builder().build(), lib2.tags());
		}		
		
//		final JsonNode share1 = getLibraryMetadata(this, Arrays.asList(
//				"",
//				""
//				));

		// SOME FAILURE CASES
		
		//TODO (ALEPH-19): some exception cases
	}

	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// CONTROL LOGIC
	
	@SuppressWarnings("deprecation")
	@Test
	public void test_compareSharesToLibraries_categorize() throws ParseException {
		
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
				IkanowV1SyncService_LibraryJars.compareJarsToLibraryBeans_categorize(Tuples._2T(v1_side, v2_side));
		
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
	public void test_compareSharesToLibraryBeans_get() throws JsonProcessingException, IOException, ParseException, InterruptedException, ExecutionException {
		@SuppressWarnings("unchecked")
		ICrudService<JsonNode> v1_share_db = this._service_context.getCoreManagementDbService()
																	.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("social.share")).get();
		
		v1_share_db.deleteDatastore().get();
		
		IManagementCrudService<SharedLibraryBean> library_db = this._service_context.getCoreManagementDbService().getSharedLibraryStore();
		
		library_db.deleteDatastore().get();
		
		// Create 2 V1 sources
		
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());		
		
		final JsonNode v1_share_1 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_share.json"));
		final JsonNode v1_share_2 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_share.json"));
		final JsonNode v1_share_3 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_share.json"));
		
		((ObjectNode)v1_share_2).set("_id", new TextNode("655d44e3347d336b3e8c4cbe"));
		((ObjectNode)v1_share_2).set("title", new TextNode("/app/aleph2/library/misc/library2.jar"));

		((ObjectNode)v1_share_3).set("_id", new TextNode("755d44e3347d336b3e8c4cbe"));
		((ObjectNode)v1_share_3).set("title", new TextNode("/app/aleph2/library/misc/library3.jar"));
		
		assertEquals(0L, (long)v1_share_db.countObjects().get());
		v1_share_db.storeObjects(Arrays.asList(v1_share_1, v1_share_2, v1_share_3)).get();
		assertEquals(3L, (long)v1_share_db.countObjects().get());
		
		// Create 2 buckets
		
		final SharedLibraryBean share1 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(v1_share_1);
		final SharedLibraryBean share2 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(v1_share_2);

		assertEquals(0L, (long)library_db.countObjects().get());
		library_db.storeObjects(Arrays.asList(share1, share2)).get();
		assertEquals(2L, (long)library_db.countObjects().get());

		// Run the function under test
		
		final Tuple2<Map<String, String>, Map<String, Date>> f_res = 
				IkanowV1SyncService_LibraryJars.compareJarsToLibaryBeans_get(library_db, v1_share_db).get();
				
		assertEquals("{755d44e3347d336b3e8c4cbe=May 21, 2015 02:37:24 AM UTC, 555d44e3347d336b3e8c4cbe=May 21, 2015 02:37:24 AM UTC, 655d44e3347d336b3e8c4cbe=May 21, 2015 02:37:24 AM UTC}", f_res._1().toString());

		assertEquals(2, f_res._2().size());
		//(times are sys dependent here so just check the keys)		
		assertEquals(true, f_res._2().containsKey("555d44e3347d336b3e8c4cbe"));
		assertEquals(true, f_res._2().containsKey("655d44e3347d336b3e8c4cbe"));						
	}	
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// DB INTEGRATION - WRITE
	
	@Test
	public void test_updateV1SourceStatus() throws JsonProcessingException, IOException, InterruptedException, ExecutionException, ParseException {
		@SuppressWarnings("unchecked")
		ICrudService<JsonNode> v1_share_db = this._service_context.getCoreManagementDbService()
																	.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("social.share")).get();
		
		final DBCollection dbc = v1_share_db.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
		
		v1_share_db.deleteDatastore().get();
		
		IManagementCrudService<SharedLibraryBean> library_db = this._service_context.getCoreManagementDbService().getSharedLibraryStore();
		
		library_db.deleteDatastore().get();
		
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());		
		
		final ObjectNode v1_share_1 = (ObjectNode) mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_share.json"));
		final DBObject v1_share_1_dbo = (DBObject) JSON.parse(v1_share_1.toString());
		v1_share_1_dbo.put("_id", new ObjectId(v1_share_1.get("_id").asText()));
		
		assertEquals(0L, (long)v1_share_db.countObjects().get());
		dbc.save(v1_share_1_dbo);
		//v1_share_db.storeObjects(Arrays.asList(v1_share_1)).get();
		assertEquals(1L, (long)v1_share_db.countObjects().get());
		
		final SharedLibraryBean share1 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(v1_share_1);

		assertEquals(0L, (long)library_db.countObjects().get());
		library_db.storeObjects(Arrays.asList(share1)).get();
		assertEquals(1L, (long)library_db.countObjects().get());
				
		// No error - create
		{
			final ManagementFuture<?> test_1 = FutureUtils.createManagementFuture(
													CompletableFuture.completedFuture(Unit.unit()),
													CompletableFuture.completedFuture(Arrays.asList(ErrorUtils.buildSuccessMessage("", "", "", ""))) // (single non error)													
													);
					
			final CompletableFuture<Boolean> res = IkanowV1SyncService_LibraryJars.updateV1ShareErrorStatus_top("555d44e3347d336b3e8c4cbe", test_1, library_db, v1_share_db, true);
			
			assertEquals(false, res.get());
			
			ObjectNode unchanged = (ObjectNode)v1_share_db.getRawService().getObjectById(new ObjectId("555d44e3347d336b3e8c4cbe")).get().get();
			
			assertEquals(v1_share_1.without("_id").toString(), unchanged.without("_id").toString());
		}
	
		// DB call throws exception
		{
			final CompletableFuture<?> error_out = new CompletableFuture<>();
			error_out.completeExceptionally(new RuntimeException("test"));
			
			final ManagementFuture<?> test_1 = FutureUtils.createManagementFuture(
					error_out													
					);
			
			final CompletableFuture<Boolean> res = IkanowV1SyncService_LibraryJars.updateV1ShareErrorStatus_top("555d44e3347d336b3e8c4cbe", test_1, library_db, v1_share_db, true);

			assertEquals(true, res.get());
			
			JsonNode changed = v1_share_db.getRawService().getObjectById(new ObjectId("555d44e3347d336b3e8c4cbe")).get().get();
			
			assertTrue(changed.get("description").asText().contains("] (unknown) ((unknown)): ERROR: [java.lang.RuntimeException: test"));
			// This shouldn't yet pe present
			assertFalse("Description error time travels: " + changed.get("description").asText(), changed.get("description").asText().contains("] (test) (unknown): ERROR: test"));
		}
		
		// db call throws exception, object doesn't exist (code coverage!)
		{
			final CompletableFuture<?> error_out = new CompletableFuture<>();
			error_out.completeExceptionally(new RuntimeException("test"));
			
			final ManagementFuture<?> test_1 = FutureUtils.createManagementFuture(
					error_out													
					);
			
			final CompletableFuture<Boolean> res = IkanowV1SyncService_LibraryJars.updateV1ShareErrorStatus_top("555d44e3347d336b3e8c4cbf", test_1, library_db, v1_share_db, true);

			assertEquals(false, res.get());
		}
		
		// User errors (+update not create)
		{
			final ManagementFuture<?> test_1 = FutureUtils.createManagementFuture(
													CompletableFuture.completedFuture(Unit.unit()),
													CompletableFuture.completedFuture(Arrays.asList(ErrorUtils.buildErrorMessage("test", "test", "test", "test"))) // (single non error)													
													);
					
			final CompletableFuture<Boolean> res = IkanowV1SyncService_LibraryJars.updateV1ShareErrorStatus_top("555d44e3347d336b3e8c4cbe", test_1, library_db, v1_share_db, false);
			
			assertEquals(true, res.get());
			
			JsonNode changed = v1_share_db.getRawService().getObjectById(new ObjectId("555d44e3347d336b3e8c4cbe")).get().get();
			
			SharedLibraryBean v2_version = library_db.getObjectById("v1_555d44e3347d336b3e8c4cbe").get().get();
			assertTrue("v2 lib bean needed updating: " + v2_version.modified(), new Date().getTime() - v2_version.modified().getTime() < 5000L);
			
			// Still has the old error
			assertTrue("Description missing errors: " + changed.get("description").asText(), changed.get("description").asText().contains("] (unknown) ((unknown)): ERROR: [java.lang.RuntimeException: test"));
			// Now has the new error
			assertTrue("Description missing errors: " + changed.get("description").asText(), changed.get("description").asText().contains("] test (test): ERROR: test"));
		}
		
	}
	
	@Test
	public void test_createDeleteLibraryBean() throws InterruptedException, ExecutionException, JsonProcessingException, IOException, ParseException {
		
		@SuppressWarnings("unchecked")
		ICrudService<JsonNode> v1_share_db = this._service_context.getCoreManagementDbService()
																	.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("social.share")).get();
		
		final DBCollection dbc = v1_share_db.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
		
		v1_share_db.deleteDatastore().get();
		
		IManagementCrudService<SharedLibraryBean> library_db = this._service_context.getCoreManagementDbService().getSharedLibraryStore();
		
		library_db.deleteDatastore().get();
		
		
		
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());		
		
		final ObjectNode v1_share_1 = (ObjectNode) mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_share.json"));
		final DBObject v1_share_1_dbo = (DBObject) JSON.parse(v1_share_1.toString());
		v1_share_1_dbo.put("_id", new ObjectId(v1_share_1.get("_id").asText()));
		
		assertEquals(0L, (long)v1_share_db.countObjects().get());
		dbc.save(v1_share_1_dbo);
		//v1_share_db.storeObjects(Arrays.asList(v1_share_1)).get();
		assertEquals(1L, (long)v1_share_db.countObjects().get());
		
		final ObjectNode v1_share_2 = (ObjectNode) mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_share.json"));
		v1_share_2.set("_id", new TextNode("655d44e3347d336b3e8c4cbe"));		
		final SharedLibraryBean share2 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(v1_share_2);
		library_db.storeObject(share2).get();
		assertEquals(1L, (long)library_db.countObjects().get());

		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		// Create directory
		FileUtils.forceMkdir(new File(temp_dir + "/library/"));
		FileUtils.deleteQuietly(new File(temp_dir + "/library/misc"));
		assertFalse(new File(temp_dir + "/library/misc").exists());
		
		final GridFS share_fs = Mockito.mock(GridFS.class);
		final GridFSDBFile share_file = Mockito.mock(GridFSDBFile.class, new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				if (invocation.getMethod().getName().equals("writeTo")) {
					ByteArrayOutputStream baos = (ByteArrayOutputStream) invocation.getArguments()[0];
					if (null != baos) {
						try {
							baos.write("test123".getBytes());
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
				return null;				
			}			
		});
		Mockito.when(share_file.writeTo(Mockito.<OutputStream>any())).thenReturn(0L);
		Mockito.when(share_fs.find(Mockito.<ObjectId>any())).thenReturn(share_file);
		
		// Create
		
		Date modified_test = null;
		{
			final ManagementFuture<Supplier<Object>> res = IkanowV1SyncService_LibraryJars.createLibraryBean(v1_share_1.get("_id").asText(), 
					library_db, _service_context.getStorageService(), true, v1_share_db, share_fs, _service_context);
			
			assertEquals("v1_" + v1_share_1.get("_id").asText(), res.get().get().toString());
			
			// Wrote DB entry
			assertTrue(library_db.getObjectById(res.get().get().toString()).get().isPresent());
			
			modified_test = library_db.getObjectById(res.get().get().toString()).get().get().modified();
			
			// Created file:
			final File f = new File(temp_dir + "/library/misc/library.jar");
			assertTrue(f.exists());
			assertEquals("test123", FileUtils.readFileToString(f));
		}
		
		// Create duplicate
		
		{
			final ManagementFuture<Supplier<Object>> res = IkanowV1SyncService_LibraryJars.createLibraryBean(v1_share_1.get("_id").asText(), 
					library_db, _service_context.getStorageService(), true, v1_share_db, share_fs, _service_context);
			
			try {
				res.get();
				fail("Should have thrown dup error");
			}
			catch (Exception e) {} // good
		}
		
		// Update

		{
			v1_share_1_dbo.put("modified", new Date());
			dbc.save(v1_share_1_dbo);

			final GridFSDBFile share_file2 = Mockito.mock(GridFSDBFile.class, new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					if (invocation.getMethod().getName().equals("writeTo")) {
						ByteArrayOutputStream baos = (ByteArrayOutputStream) invocation.getArguments()[0];
						if (null != baos) {
							try {
								baos.write("test1234".getBytes());
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
					return null;				
				}			
			});
			Mockito.when(share_file2.writeTo(Mockito.<OutputStream>any())).thenReturn(0L);
			Mockito.when(share_fs.find(Mockito.<ObjectId>any())).thenReturn(share_file2);

			final ManagementFuture<Supplier<Object>> res = IkanowV1SyncService_LibraryJars.createLibraryBean(v1_share_1.get("_id").asText(), 
					library_db, _service_context.getStorageService(), false, v1_share_db, share_fs, _service_context);
			
			assertEquals("v1_" + v1_share_1.get("_id").asText(), res.get().get().toString());
			
			// Wrote DB entry
			assertTrue(library_db.getObjectById(res.get().get().toString()).get().isPresent());
			
			// Created file:
			final File f = new File(temp_dir + "/library/misc/library.jar");
			assertTrue(f.exists());
			assertEquals("test1234", FileUtils.readFileToString(f));
			
			final Date modified_2 = library_db.getObjectById(res.get().get().toString()).get().get().modified();

			assertTrue("Mod time should change " + modified_test + " < " + modified_2, modified_2.getTime() > modified_test.getTime());
		}
		
		// Delete
		
		{
			IkanowV1SyncService_LibraryJars.deleteLibraryBean(v1_share_1.get("_id").asText(), library_db, _service_context.getStorageService());
			
			assertFalse(library_db.getObjectById("v1_" + v1_share_1.get("_id").asText()).get().isPresent());
			
			final File f = new File(temp_dir + "/library/misc/library.jar");
			assertTrue(f.exists());

			IkanowV1SyncService_LibraryJars.deleteLibraryBean(v1_share_2.get("_id").asText(), library_db, _service_context.getStorageService());
			assertFalse(f.exists());			
		}
	}
		
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// CONTROL CODE - PART 2
	
	@Test
	public void test_puttingItAllTogether() throws JsonProcessingException, IOException, ParseException, InterruptedException, ExecutionException {
		
		@SuppressWarnings("unchecked")
		ICrudService<JsonNode> v1_share_db = this._service_context.getCoreManagementDbService()
																	.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("social.share")).get();
		
		v1_share_db.deleteDatastore().get();
		
		IManagementCrudService<SharedLibraryBean> library_db = this._service_context.getCoreManagementDbService().getSharedLibraryStore();
		
		library_db.deleteDatastore().get();
		
		// Create 2 V1 sources
		
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());		
		
		final JsonNode v1_share_1 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_share.json"));
		final JsonNode v1_share_2 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_share.json"));
		final JsonNode v1_share_3 = mapper.readTree(this.getClass().getResourceAsStream("test_v1_sync_sample_share.json"));
		
		((ObjectNode)v1_share_2).set("_id", new TextNode("655d44e3347d336b3e8c4cbe"));
		((ObjectNode)v1_share_2).set("title", new TextNode("/app/aleph2/library/misc/library2.jar"));

		((ObjectNode)v1_share_3).set("_id", new TextNode("755d44e3347d336b3e8c4cbe"));
		((ObjectNode)v1_share_3).set("title", new TextNode("/app/aleph2/library/misc/library3.jar"));
		
		//final SharedLibraryBean share1 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(v1_share_1);
		final SharedLibraryBean share2 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(v1_share_2);
		final SharedLibraryBean share3 = IkanowV1SyncService_LibraryJars.getLibraryBeanFromV1Share(v1_share_3);		
		
		final DBCollection dbc = v1_share_db.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
		final DBObject v1_share_1_dbo = (DBObject) JSON.parse(v1_share_1.toString());
		v1_share_1_dbo.put("_id", new ObjectId(v1_share_1.get("_id").asText()));
		v1_share_1_dbo.removeField("binaryId");
		final DBObject v1_share_2_dbo = (DBObject) JSON.parse(v1_share_2.toString());
		v1_share_2_dbo.put("_id", new ObjectId(v1_share_2.get("_id").asText()));
		v1_share_2_dbo.put("modified", new Date());
		v1_share_2_dbo.removeField("binaryId");
		final DBObject v1_share_3_dbo = (DBObject) JSON.parse(v1_share_3.toString());
		v1_share_3_dbo.put("_id", new ObjectId(v1_share_3.get("_id").asText()));
		v1_share_3_dbo.removeField("binaryId");
		
		
		assertEquals(0L, (long)v1_share_db.countObjects().get());
		dbc.save(v1_share_1_dbo);
		dbc.save(v1_share_2_dbo);
		assertEquals(2L, (long)v1_share_db.countObjects().get());

		 // Store the buckets
		
		assertEquals(0L, (long)library_db.countObjects().get());
		library_db.storeObjects(Arrays.asList(share2, share3)).get();
		assertEquals(2L, (long)library_db.countObjects().get());

		// OK now fire off an instance of the runner
		
		IkanowV1SyncService_LibraryJars s1 = new IkanowV1SyncService_LibraryJars(BeanTemplateUtils.clone(_service_config).with("v1_enabled", true).done(), 
				_service_context);
		
		int old = IkanowV1SyncService_LibraryJars._num_leader_changes;
		s1.start();
		for (int i = 0; i < 20; ++i) {
			try { Thread.sleep(1000); } catch (Exception e) {}
			
			if ((old + 1) == IkanowV1SyncService_LibraryJars._num_leader_changes) break;
		}
		s1.stop();

		assertEquals(old + 1, IkanowV1SyncService_LibraryJars._num_leader_changes);
		
		// Now sleep a bit more to let the monitor have time to finish:
		Thread.sleep(3000L);

		// Check a few things have happened:

		// 1) share 1 was created
		
		assertTrue("share 1 should be created", library_db.getObjectById("v1_" + v1_share_1.get("_id").asText()).get().isPresent());
		
		// 2) share 2 should have been updated
		
		SharedLibraryBean updated_share_2 = library_db.getObjectById("v1_" + v1_share_2.get("_id").asText()).get().get();
		assertTrue("share 2 should have been updated: ", updated_share_2.modified().getTime() > share2.modified().getTime());
		
		// 3) share 3 was deleted:
		
		assertFalse("share 3 should be deleted", library_db.getObjectById("v1_" + v1_share_3.get("_id").asText()).get().isPresent());
	}	
}
