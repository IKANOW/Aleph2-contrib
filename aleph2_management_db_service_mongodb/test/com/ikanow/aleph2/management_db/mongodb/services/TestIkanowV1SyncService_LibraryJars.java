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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import scala.Tuple3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.module.MockMongoDbManagementDbModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

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
			
			Injector app_injector = ModuleUtils.createInjector(Arrays.asList(new MockMongoDbManagementDbModule()), Optional.of(config));	
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
			assertEquals(ImmutableMap.builder()
							.put("50bcd6fffbf0fd0b27875a7c", "rw")
							.put("50bcd6fffbf0fd0b27875a7d", "rw").build(),
					lib1.access_rights().auth_token());
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
		
		//TODO: some exception cases
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
		//TODO (ALEPH-19)
	}	
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// DB INTEGRATION - WRITE
	
	//TODO (ALEPH-19)
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// CONTROL CODE - PART 2
	
	//TODO (ALEPH-19): "Putting it all together"
}
