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

import java.util.Optional;

import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class TestMongoDbManagementDbService {

	@SuppressWarnings("unchecked")
	@Test
	public void testCrudAccess() {
		
		MockMongoDbCrudServiceFactory mock_crud_service_factory = new MockMongoDbCrudServiceFactory();

		MongoDbManagementDbService management_db_service = new MongoDbManagementDbService(mock_crud_service_factory, new MongoDbManagementDbConfigBean(false), null, null);
		
		assertEquals(MongoDbManagementDbService.DATA_ANALYTIC_THREAD_STORE,
				management_db_service.getAnalyticThreadStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals(MongoDbManagementDbService.DATA_BUCKET_STATUS_STORE,
				management_db_service.getDataBucketStatusStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals(MongoDbManagementDbService.DATA_BUCKET_STORE,
				management_db_service.getDataBucketStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals(MongoDbManagementDbService.SHARED_LIBRARY_STORE,
				management_db_service.getSharedLibraryStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());

		// Check that the wrapped version also works
		
		MongoDbManagementDbService management_db_service2 = management_db_service.getFilteredDb(Optional.empty(), Optional.empty());
		
		assertEquals(MongoDbManagementDbService.DATA_ANALYTIC_THREAD_STORE,
				management_db_service2.getAnalyticThreadStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals(MongoDbManagementDbService.DATA_BUCKET_STATUS_STORE,
				management_db_service2.getDataBucketStatusStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals(MongoDbManagementDbService.DATA_BUCKET_STORE,
				management_db_service2.getDataBucketStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());
		
		assertEquals(MongoDbManagementDbService.SHARED_LIBRARY_STORE,
				management_db_service2.getSharedLibraryStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getFullName());

		try {
			management_db_service.getPerAnalyticThreadState(null, null, null);
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			assertEquals(RuntimeException.class, e.getClass());
			assertEquals(e.getMessage(), "This method is currently not supported");
		}
		
		try {
			management_db_service.getPerBucketState(null, null, null);
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			assertEquals(RuntimeException.class, e.getClass());
			assertEquals(e.getMessage(), "This method is currently not supported");			
		}
		try {
			management_db_service.getPerLibraryState(null, null, null);
			fail("Should have thrown an exception");
		}
		catch (Exception e) {
			assertEquals(RuntimeException.class, e.getClass());
			assertEquals(e.getMessage(), "This method is currently not supported");
		}

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
		// Bucket status
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
		// Shared Library Store
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
		// Retry Store
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
}
