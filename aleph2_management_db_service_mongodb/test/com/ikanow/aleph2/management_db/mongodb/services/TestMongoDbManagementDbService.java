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

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class TestMongoDbManagementDbService {

	@SuppressWarnings("unchecked")
	@Test
	public void testCrudAccess() {
		
		MockMongoDbCrudServiceFactory mock_crud_service_factory = new MockMongoDbCrudServiceFactory();

		MongoDbManagementDbService management_db_service = new MongoDbManagementDbService(mock_crud_service_factory);
		
		assertEquals(MongoDbManagementDbService.DATA_ANALYTIC_THREAD_STORE,
				management_db_service.getAnalyticThreadStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).getFullName());
		
		assertEquals(MongoDbManagementDbService.DATA_BUCKET_STATUS_STORE,
				management_db_service.getDataBucketStatusStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).getFullName());
		
		assertEquals(MongoDbManagementDbService.DATA_BUCKET_STORE,
				management_db_service.getDataBucketStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).getFullName());
		
		assertEquals(MongoDbManagementDbService.SHARED_LIBRARY_STORE,
				management_db_service.getSharedLibraryStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).getFullName());

		// Check that the wrapped version also works
		
		MongoDbManagementDbService management_db_service2 = management_db_service.getFilteredDb(Optional.empty(), Optional.empty());
		
		assertEquals(MongoDbManagementDbService.DATA_ANALYTIC_THREAD_STORE,
				management_db_service2.getAnalyticThreadStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).getFullName());
		
		assertEquals(MongoDbManagementDbService.DATA_BUCKET_STATUS_STORE,
				management_db_service2.getDataBucketStatusStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).getFullName());
		
		assertEquals(MongoDbManagementDbService.DATA_BUCKET_STORE,
				management_db_service2.getDataBucketStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).getFullName());
		
		assertEquals(MongoDbManagementDbService.SHARED_LIBRARY_STORE,
				management_db_service2.getSharedLibraryStore().getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).getFullName());

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

		assertEquals("test", management_db_service.getUnderlyingPlatformDriver(DB.class, Optional.of("test")).getName());
		assertEquals("test1.test2", management_db_service.getUnderlyingPlatformDriver(DBCollection.class, Optional.of("test1.test2")).getFullName());
		
		assertEquals("test3.test4", 
				((DBCollection)management_db_service.getUnderlyingPlatformDriver(ICrudService.class, 
						Optional.of("test3.test4")).getUnderlyingPlatformDriver(DBCollection.class, Optional.empty())).getFullName());
		
		assertEquals("test2.test5", 
				((DBCollection)management_db_service.getUnderlyingPlatformDriver(ICrudService.class, 
						Optional.of("test2.test5/com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean")).getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()))
						.getFullName());
		
		try {
			management_db_service.getUnderlyingPlatformDriver(String.class, Optional.empty());
			fail("Should have thrown an exception");
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
	}
}
