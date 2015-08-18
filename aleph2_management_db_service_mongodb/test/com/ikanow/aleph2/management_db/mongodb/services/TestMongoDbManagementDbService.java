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
 ******************************************************************************/
package com.ikanow.aleph2.management_db.mongodb.services;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class TestMongoDbManagementDbService {

	@SuppressWarnings("unchecked")
	@Test
	public void test_CrudAccess() {
		
		MockMongoDbCrudServiceFactory mock_crud_service_factory = new MockMongoDbCrudServiceFactory();

		MongoDbManagementDbService management_db_service = new MongoDbManagementDbService(mock_crud_service_factory, new MongoDbManagementDbConfigBean(false), null, null);
		
		assertEquals(MongoDbManagementDbService.DATA_BUCKET_STATUS_STORE,
				management_db_service.getDataBucketStatusStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals(MongoDbManagementDbService.DATA_BUCKET_STORE,
				management_db_service.getDataBucketStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals(MongoDbManagementDbService.BUCKET_DELETION_STORE,
				management_db_service.getBucketDeletionQueue(String.class).getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals(MongoDbManagementDbService.BUCKET_TEST_STORE,
				management_db_service.getBucketTestQueue(String.class).getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals(MongoDbManagementDbService.SHARED_LIBRARY_STORE,
				management_db_service.getSharedLibraryStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());

		assertEquals(MongoDbManagementDbService.STATE_DIRECTORY_STORE,
				management_db_service.getStateDirectory(Optional.empty(), Optional.empty()).getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		// Check that the wrapped version also works
		
		MongoDbManagementDbService management_db_service2 = management_db_service.getFilteredDb(Optional.empty(), Optional.empty());
		
		assertEquals(MongoDbManagementDbService.DATA_BUCKET_STATUS_STORE,
				management_db_service2.getDataBucketStatusStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals(MongoDbManagementDbService.DATA_BUCKET_STORE,
				management_db_service2.getDataBucketStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals(MongoDbManagementDbService.BUCKET_DELETION_STORE,
				management_db_service2.getBucketDeletionQueue(String.class).getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals(MongoDbManagementDbService.BUCKET_TEST_STORE,
				management_db_service2.getBucketTestQueue(String.class).getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals(MongoDbManagementDbService.SHARED_LIBRARY_STORE,
				management_db_service2.getSharedLibraryStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());


		assertEquals(MongoDbManagementDbService.STATE_DIRECTORY_STORE,
				management_db_service2.getStateDirectory(Optional.empty(), Optional.empty()).getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals("test", management_db_service.getUnderlyingPlatformDriver(DB.class, Optional.of("test")).get().getName());
		assertEquals("test1.test2", management_db_service.getUnderlyingPlatformDriver(DBCollection.class, Optional.of("test1.test2")).get().getFullName());
		
		assertEquals("test3.test4", 
				((DBCollection)management_db_service.getUnderlyingPlatformDriver(ICrudService.class, 
						Optional.of("test3.test4")).get().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get()).getFullName());
		
		assertEquals("test2.test5", 
				((DBCollection)management_db_service.getUnderlyingPlatformDriver(ICrudService.class, 
						Optional.of("test2.test5/com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean")).get().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get())
						.getFullName());
		
		try {
			assertEquals(Optional.empty(), management_db_service.getUnderlyingPlatformDriver(String.class, Optional.empty()));
		}
		catch (Exception e) {}
		
		try {
			management_db_service.getUnderlyingPlatformDriver(DB.class, Optional.empty());
			fail("Should have thrown an exception");
		}
		catch (Exception e) {}

		try {
			management_db_service.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty());
			fail("Should have thrown an exception");
		}
		catch (Exception e) {}

		try {
			management_db_service.getUnderlyingPlatformDriver(DBCollection.class, Optional.of("test"));
			fail("Should have thrown an exception");
		}
		catch (Exception e) {}
		
		try {
			management_db_service.getUnderlyingPlatformDriver(ICrudService.class, Optional.empty());
			fail("Should have thrown an exception");
		}
		catch (Exception e) {}
		
		try {
			management_db_service.getUnderlyingPlatformDriver(ICrudService.class, Optional.of("test"));
			fail("Should have thrown an exception");
		}
		catch (Exception e) {}
		
		// TEST READ-ONLY MODE:
		
		final IManagementDbService read_only_management_db_service = management_db_service.readOnlyVersion();
		
		// Bucket
		{
			ICrudService<DataBucketBean> bucket_service = read_only_management_db_service.getDataBucketStore();
			assertTrue("Is read only", IManagementCrudService.IReadOnlyManagementCrudService.class.isAssignableFrom(bucket_service.getClass()));
			try {
				bucket_service.deleteDatastore();
				fail("Should have thrown error");
			}
			catch (Exception e) {
				assertEquals("Correct error message", ErrorUtils.READ_ONLY_CRUD_SERVICE, e.getMessage());
			}
			bucket_service.countObjects(); // (just check doesn't thrown)
		}
		// Bucket status
		{
			ICrudService<DataBucketStatusBean> bucket_status_service = read_only_management_db_service.getDataBucketStatusStore();
			assertTrue("Is read only", IManagementCrudService.IReadOnlyManagementCrudService.class.isAssignableFrom(bucket_status_service.getClass()));
			try {
				bucket_status_service.deleteDatastore();
				fail("Should have thrown error");
			}
			catch (Exception e) {
				assertEquals("Correct error message", ErrorUtils.READ_ONLY_CRUD_SERVICE, e.getMessage());
			}
			bucket_status_service.countObjects(); // (just check doesn't thrown)
		}
		// Shared Library Store
		{
			ICrudService<SharedLibraryBean> shared_lib_service = read_only_management_db_service.getSharedLibraryStore();
			assertTrue("Is read only", IManagementCrudService.IReadOnlyManagementCrudService.class.isAssignableFrom(shared_lib_service.getClass()));
			try {
				shared_lib_service.deleteDatastore();
				fail("Should have thrown error");
			}
			catch (Exception e) {
				assertEquals("Correct error message", ErrorUtils.READ_ONLY_CRUD_SERVICE, e.getMessage());
			}
			shared_lib_service.countObjects(); // (just check doesn't thrown)
		}
		// State Directory Store
		{
			ICrudService<AssetStateDirectoryBean> state_dir_service = read_only_management_db_service.getStateDirectory(Optional.empty(), Optional.empty());
			assertTrue("Is read only", ICrudService.IReadOnlyCrudService.class.isAssignableFrom(state_dir_service.getClass()));
			try {
				state_dir_service.deleteDatastore();
				fail("Should have thrown error");
			}
			catch (Exception e) {
				assertEquals("Correct error message", ErrorUtils.READ_ONLY_CRUD_SERVICE, e.getMessage());
			}
			state_dir_service.countObjects(); // (just check doesn't thrown)
		}
		// Retry Store
		{
			ICrudService<String> retry_service = read_only_management_db_service.getRetryStore(String.class);
			assertTrue("Is read only", ICrudService.IReadOnlyCrudService.class.isAssignableFrom(retry_service.getClass()));
			try {
				retry_service.deleteDatastore();
				fail("Should have thrown error");
			}
			catch (Exception e) {
				assertEquals("Correct error message", ErrorUtils.READ_ONLY_CRUD_SERVICE, e.getMessage());
			}
			retry_service.countObjects(); // (just check doesn't thrown)
		}
		// Deletion queue
		{
			ICrudService<String> deletion_service = read_only_management_db_service.getBucketDeletionQueue(String.class);
			assertTrue("Is read only", ICrudService.IReadOnlyCrudService.class.isAssignableFrom(deletion_service.getClass()));
			try {
				deletion_service.deleteDatastore();
				fail("Should have thrown error");
			}
			catch (Exception e) {
				assertEquals("Correct error message", ErrorUtils.READ_ONLY_CRUD_SERVICE, e.getMessage());
			}
			deletion_service.countObjects(); // (just check doesn't thrown)
		}		
		// Test queue
		{
			ICrudService<String> test_service = read_only_management_db_service.getBucketTestQueue(String.class);
			assertTrue("Is read only", ICrudService.IReadOnlyCrudService.class.isAssignableFrom(test_service.getClass()));
			try {
				test_service.deleteDatastore();
				fail("Should have thrown error");
			}
			catch (Exception e) {
				assertEquals("Correct error message", ErrorUtils.READ_ONLY_CRUD_SERVICE, e.getMessage());
			}
			test_service.countObjects(); // (just check doesn't thrown)
		}		
	}
	
	public static class StateTester {
		public String _id;
		public String test_val;
	}
	
	@Test
	public void test_getPerBucketState() throws InterruptedException, ExecutionException {
		
		// Set up:
		MockMongoDbCrudServiceFactory mock_crud_service_factory = new MockMongoDbCrudServiceFactory();
		MongoDbManagementDbService management_db_service = new MongoDbManagementDbService(mock_crud_service_factory, new MongoDbManagementDbConfigBean(false), null, null);
		IManagementDbService management_db_service_ro = management_db_service.readOnlyVersion();
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test+extra/4354____42").done().get();
		
		// No id, "top level collection"
		{
			// (check fails if use wrong clazz)
			try {
				management_db_service.getBucketHarvestState(StateTester.class, bucket, Optional.empty());
				fail("Must return correct clazz");
			}
			catch (Exception e) {}
			
			final ICrudService<AssetStateDirectoryBean> test = management_db_service.getBucketHarvestState(AssetStateDirectoryBean.class, bucket, Optional.empty());
			final ICrudService<AssetStateDirectoryBean> test_ro = management_db_service_ro.getBucketHarvestState(AssetStateDirectoryBean.class, bucket, Optional.empty());
			
			final DBCollection dbc1 = test.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
			final DBCollection dbc2 = test_ro.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
			
			assertEquals("aleph2_shared.state_directory_store", dbc1.getFullName());
			assertEquals("aleph2_shared.state_directory_store", dbc2.getFullName());
			
			dbc1.drop();
			assertEquals(0, dbc1.count());
			
			final AssetStateDirectoryBean test_obj = BeanTemplateUtils.build(AssetStateDirectoryBean.class).with("_id", "test").done().get();
			
			try {
				test_ro.storeObject(test_obj);
			}
			catch (Exception e) {
				// check didn't add:
				assertEquals(0, dbc2.count());
			}
			try {
				test.storeObject(test_obj);
			}
			catch (Exception e) {
				// check didn't add:
				assertEquals(0, dbc2.count());
			}			
		}		
		// Id, collection (harvest)
		String harvest_name;
		{
			final ICrudService<StateTester> test = management_db_service.getBucketHarvestState(StateTester.class, bucket, Optional.of("t"));
			final ICrudService<StateTester> test_ro = management_db_service_ro.getBucketHarvestState(StateTester.class, bucket, Optional.of("t"));
			
			final DBCollection dbc1 = test.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
			final DBCollection dbc2 = test_ro.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
			harvest_name = dbc1.getFullName();
			
			assertEquals("aleph2_harvest_state_1.test_ext_4354_t_3b7ae2550a2e", dbc1.getFullName());
			assertEquals("aleph2_harvest_state_1.test_ext_4354_t_3b7ae2550a2e", dbc2.getFullName());
			
			dbc1.drop();
			assertEquals(0, dbc1.count());
			
			final StateTester test_obj = BeanTemplateUtils.build(StateTester.class).with("_id", "test_id").with("test_val", "test").done().get();
			
			try {
				test_ro.storeObject(test_obj);
			}
			catch (Exception e) {
				// check didn't add:
				assertEquals(0, dbc2.count());
			}
			test.storeObject(test_obj);
			assertEquals(1, dbc1.count());
			BasicDBObject test_dbo = (BasicDBObject) dbc1.findOne(new BasicDBObject("test_val", "test"));
			assertEquals("Populated _id", "test_id", test_dbo.get("_id"));
			
			// Check it's been added (filtered on bucket + type)
			final ICrudService<AssetStateDirectoryBean> dir_test = management_db_service.getBucketHarvestState(AssetStateDirectoryBean.class, bucket, Optional.empty());
			assertEquals(1, dir_test.countObjects().get().intValue());
			
			assertTrue("Object present (harvest)", dir_test.getObjectById(harvest_name).get().isPresent());			
		}		
		// Id, collection (enrichment)
		String enrich_name;
		{
			final ICrudService<StateTester> test = management_db_service.getBucketEnrichmentState(StateTester.class, bucket, Optional.of("t"));
			final ICrudService<StateTester> test_ro = management_db_service_ro.getBucketEnrichmentState(StateTester.class, bucket, Optional.of("t"));
			
			final DBCollection dbc1 = test.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
			final DBCollection dbc2 = test_ro.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
			enrich_name = dbc1.getFullName();
			
			assertEquals("aleph2_enrich_state_1.test_ext_4354_t_3b7ae2550a2e", dbc1.getFullName());
			assertEquals("aleph2_enrich_state_1.test_ext_4354_t_3b7ae2550a2e", dbc2.getFullName());
			
			dbc1.drop();
			assertEquals(0, dbc1.count());
			
			final StateTester test_obj = BeanTemplateUtils.build(StateTester.class).with("_id", "test_id").with("test_val", "test").done().get();
			
			try {
				test_ro.storeObject(test_obj);
			}
			catch (Exception e) {
				// check didn't add:
				assertEquals(0, dbc2.count());
			}
			test.storeObject(test_obj);
			assertEquals(1, dbc1.count());
			BasicDBObject test_dbo = (BasicDBObject) dbc1.findOne(new BasicDBObject("test_val", "test"));
			assertEquals("Populated _id", "test_id", test_dbo.get("_id"));
			
			// Check it's been added (filtered on bucket + type)
			final ICrudService<AssetStateDirectoryBean> dir_test = management_db_service.getBucketEnrichmentState(AssetStateDirectoryBean.class, bucket, Optional.empty());
			assertEquals(1, dir_test.countObjects().get().intValue());
			
			assertTrue("Object present (enrich)", dir_test.getObjectBySpec(CrudUtils.allOf(AssetStateDirectoryBean.class).when("_id", enrich_name)).get().isPresent());
			assertTrue("Object not present (harvest)", !dir_test.getObjectBySpec(CrudUtils.allOf(AssetStateDirectoryBean.class).when("_id", harvest_name)).get().isPresent());
		}		
		// Id, collection (analytics)		
		final ICrudService<StateTester> test = management_db_service.getBucketAnalyticThreadState(StateTester.class, bucket, Optional.of("t"));
		final DBCollection dbc1 = test.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
		// (leave these out so we can delete them...)
		
		{
			final ICrudService<StateTester> test_ro = management_db_service_ro.getBucketAnalyticThreadState(StateTester.class, bucket, Optional.of("t"));
			
			final DBCollection dbc2 = test_ro.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
			
			assertEquals("aleph2_analytics_state_1.test_ext_4354_t_3b7ae2550a2e", dbc1.getFullName());
			assertEquals("aleph2_analytics_state_1.test_ext_4354_t_3b7ae2550a2e", dbc2.getFullName());
			
			dbc1.drop();
			assertEquals(0, dbc1.count());
			
			final StateTester test_obj = BeanTemplateUtils.build(StateTester.class).with("_id", "test_id").with("test_val", "test").done().get();
			
			try {
				test_ro.storeObject(test_obj);
			}
			catch (Exception e) {
				// check didn't add:
				assertEquals(0, dbc2.count());
			}
			test.storeObject(test_obj);
			assertEquals(1, dbc1.count());
			BasicDBObject test_dbo = (BasicDBObject) dbc1.findOne(new BasicDBObject("test_val", "test"));
			assertEquals("Populated _id", "test_id", test_dbo.get("_id"));
			
			// Check it's been added (filtered on bucket + type)
			final ICrudService<AssetStateDirectoryBean> dir_test = management_db_service.getBucketAnalyticThreadState(AssetStateDirectoryBean.class, bucket, Optional.empty());
			assertEquals(1, dir_test.countObjects().get().intValue());				
			
			// (test both types of update by _id with extra query terms)
			assertTrue("Object present (analytics)", dir_test.getObjectById(dbc1.getFullName()).get().isPresent());
			assertTrue("Object not present (enrich)", !dir_test.getObjectById(enrich_name, Arrays.asList("_id"), true).get().isPresent());
			assertTrue("Object not present (harvest)", !dir_test.getObjectById(harvest_name).get().isPresent());
		}				
		{
			final ICrudService<AssetStateDirectoryBean> dir_test_all = management_db_service.getStateDirectory(Optional.empty(), Optional.empty());
			assertEquals(3, dir_test_all.countObjects().get().intValue());
			
			final ICrudService<AssetStateDirectoryBean> dir_test_analytics = management_db_service.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.analytic_thread));
			assertEquals(1, dir_test_analytics.countObjects().get().intValue());			
			
			dir_test_analytics.deleteDatastore();
			
			assertEquals(0, dir_test_analytics.countObjects().get().intValue());			
			assertEquals(2, dir_test_all.countObjects().get().intValue());

			final ICrudService<AssetStateDirectoryBean> dir_test_enrich = management_db_service.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.enrichment));
			assertTrue("Deleted enriched object", dir_test_enrich.deleteObjectById(enrich_name).get());
			assertEquals(1, dir_test_all.countObjects().get().intValue());
		}
	}
	
	@Test
	public void test_getPerLibraryState() throws InterruptedException, ExecutionException {
		
		// Set up:
		MockMongoDbCrudServiceFactory mock_crud_service_factory = new MockMongoDbCrudServiceFactory();
		MongoDbManagementDbService management_db_service = new MongoDbManagementDbService(mock_crud_service_factory, new MongoDbManagementDbConfigBean(false), null, null);
		IManagementDbService management_db_service_ro = management_db_service.readOnlyVersion();
		
		final SharedLibraryBean library = BeanTemplateUtils.build(SharedLibraryBean.class).with(SharedLibraryBean::path_name, "/test+extra/4354____42").done().get();
		
		// No id, "top level collection"
		{
			final ICrudService<AssetStateDirectoryBean> test = management_db_service.getPerLibraryState(AssetStateDirectoryBean.class, library, Optional.empty());
			final ICrudService<AssetStateDirectoryBean> test_ro = management_db_service_ro.getPerLibraryState(AssetStateDirectoryBean.class, library, Optional.empty());
			
			final DBCollection dbc1 = test.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
			final DBCollection dbc2 = test_ro.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
			
			assertEquals("aleph2_shared.state_directory_store", dbc1.getFullName());
			assertEquals("aleph2_shared.state_directory_store", dbc2.getFullName());
			
			dbc1.drop();
			assertEquals(0, dbc1.count());
			
			final AssetStateDirectoryBean test_obj = BeanTemplateUtils.build(AssetStateDirectoryBean.class).with("_id", "test").done().get();
			
			try {
				test_ro.storeObject(test_obj);
			}
			catch (Exception e) {
				// check didn't add:
				assertEquals(0, dbc2.count());
			}
			try {
				test.storeObject(test_obj);
			}
			catch (Exception e) {
				// check didn't add:
				assertEquals(0, dbc2.count());
			}			
		}		
		// Id, sub-collection
		final ICrudService<StateTester> test = management_db_service.getPerLibraryState(StateTester.class, library, Optional.of("t"));
		final DBCollection dbc1 = test.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
		{
			final ICrudService<StateTester> test_ro = management_db_service_ro.getPerLibraryState(StateTester.class, library, Optional.of("t"));
			
			final DBCollection dbc2 = test_ro.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get();
			
			assertEquals("aleph2_library_state_1.test_ext_4354_t_3b7ae2550a2e", dbc1.getFullName());
			assertEquals("aleph2_library_state_1.test_ext_4354_t_3b7ae2550a2e", dbc2.getFullName());
			
			dbc1.drop();
			assertEquals(0, dbc1.count());
			
			final StateTester test_obj = BeanTemplateUtils.build(StateTester.class).with("_id", "test_id").with("test_val", "test").done().get();
			
			try {
				test_ro.storeObject(test_obj);
			}
			catch (Exception e) {
				// check didn't add:
				assertEquals(0, dbc2.count());
			}
			test.storeObject(test_obj);
			assertEquals(1, dbc1.count());
			BasicDBObject test_dbo = (BasicDBObject) dbc1.findOne(new BasicDBObject("test_val", "test"));
			assertEquals("Populated _id", "test_id", test_dbo.get("_id"));
			
			// Check it's been added (filtered on bucket + type)
			final ICrudService<AssetStateDirectoryBean> dir_test = management_db_service.getPerLibraryState(AssetStateDirectoryBean.class, library, Optional.empty());
			assertEquals(1, dir_test.countObjects().get().intValue());
			
			assertTrue("Object present (harvest)", dir_test.getObjectById(dbc1.getFullName()).get().isPresent());			
			
		}		
		{
			final ICrudService<AssetStateDirectoryBean> dir_test_all = management_db_service.getStateDirectory(Optional.empty(), Optional.empty());
			assertEquals(1, dir_test_all.countObjects().get().intValue());
			
			final ICrudService<AssetStateDirectoryBean> dir_test_lib = management_db_service.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.library));
			assertEquals(1, dir_test_lib.countObjects().get().intValue());			
			
			test.deleteDatastore().get();
			
			assertEquals(0, dir_test_lib.countObjects().get().intValue());			
			assertEquals(0, dir_test_all.countObjects().get().intValue());
		}
	}
	
}
