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
package com.ikanow.aleph2.management_db.mongodb.utils;

import static org.junit.Assert.*;

import java.util.Optional;
import java.util.stream.IntStream;

import org.junit.Test;

import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;

public class TestMongoDbCollectionUtils {

	@Test
	public void test_findDatabase() {

		final String path1 = "/test+extra/4354____42";
		final String coll_name1 = "test_extra_4354_42_t__bb8a6a382d7b";
		assertEquals(coll_name1, BucketUtils.getUniqueSignature(path1, Optional.of("t")));		
		final String coll_name2 = "test_extra_4354_42__bb8a6a382d7b";
		assertEquals(coll_name2, BucketUtils.getUniqueSignature(path1, Optional.empty()));		
		
		// Set up some DBs:
		
		MockMongoDbCrudServiceFactory mock_crud_service_factory = new MockMongoDbCrudServiceFactory();
		
		// (Make sure they are all empty to start with)
		IntStream.range(1, 5).boxed().forEach(i -> mock_crud_service_factory.getMongoDb("test_findDatabase_" + i).dropDatabase());
		IntStream.range(1, 5).boxed().forEach(i -> assertEquals(0, mock_crud_service_factory.getMongoDb("test_findDatabase_" + i).getCollectionNames().size()));
		// (Now fill some in)
		mock_crud_service_factory.getMongoDb("test_findDatabase_1").getCollection("test_ext_more_componen_xx__ec9cbb79741c").save(new BasicDBObject());
		IntStream.range(1, 210).boxed().forEach(i -> mock_crud_service_factory.getMongoDb("test_findDatabase_1").getCollection("whatever" + i).save(new BasicDBObject()));
		mock_crud_service_factory.getMongoDb("test_findDatabase_2").getCollection("whatever").save(new BasicDBObject());
		mock_crud_service_factory.getMongoDb("test_findDatabase_3").getCollection("test_extra_4354_42_t__bb8a6a382d7b").save(new BasicDBObject());
		//(test_findDatabase_4 is empty)
		mock_crud_service_factory.getMongoDb("test_findDatabase_5").getCollection("test_extra_4354_42__bb8a6a382d7b").save(new BasicDBObject());
		//^(wont' ever get here here because test_findDatabase_4 is empty)
		
		// 2 cases to test:
		// 1) collection present in a DB
		// 2) exit on first empty DB
		final Mongo client = mock_crud_service_factory.getMongoDb("test").getMongo();
		
		// 1) Find collection:
		
		final DB db1 = MongoDbCollectionUtils.findDatabase(client, "test_findDatabase", coll_name1);
		assertEquals("test_findDatabase_3", db1.getName());
		
		// 2) Exit on first non-full DB
		
		final DB db2 = MongoDbCollectionUtils.findDatabase(client, "test_findDatabase", coll_name2);
		assertEquals("test_findDatabase_2", db2.getName());		
		
	}
}
