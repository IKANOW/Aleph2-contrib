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
package com.ikanow.aleph2.shared_services.crud.mongodb;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongojack.JacksonDBCollection;

import scala.Tuple2;
import static org.hamcrest.CoreMatchers.instanceOf;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.ObjectTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

import static org.junit.Assert.*;

public class TestMongoDbCrudService {

	public static class TestBean {
		public String _id() { return _id; }
		public String test_string() { return test_string; }

		String _id;
		String test_string;
		Boolean test_bool;
		Long test_long;
		List<String> test_string_list;
		Set<NestedTestBean> test_object_set;
		LinkedHashMap<String, Long> test_map;
		
		public static class NestedTestBean {
			String test_string;			
		}
	}
	
	////////////////////////////////////////////////
	
	// CREATION
	
	@Test
	public void testCreateSingleObject() throws InterruptedException, ExecutionException {
		
		final MockMongoDbCrudService<TestBean, String> service = 
				new MockMongoDbCrudService<TestBean, String>("test", "test", "testCreateSingleObject", 
						TestBean.class, String.class, Optional.empty(), Optional.empty(), Optional.empty());

		assertEquals(0, service._state.orig_coll.count());		
		
		// 1) Add a new object to an empty DB
		
		final TestBean test = new TestBean();
		test._id = "_id_1";
		test.test_string = "test_string_1";
		
		final Future<Supplier<Object>> result = service.storeObject(test);
		
		final Supplier<Object> val = result.get();
		
		assertEquals("_id_1", val.get());
		
		final DBObject retval = service._state.orig_coll.findOne();
		
		assertEquals(1, service._state.orig_coll.count());
		
		assertEquals("{ \"_id\" : \"_id_1\" , \"test_string\" : \"test_string_1\"}", retval.toString());
		
		// 2) Add the _id again, should fail
		
		final TestBean test2 = ObjectTemplateUtils.clone(test).with("test_string", "test_string_2").done();
		
		final Future<Supplier<Object>> result2 = service.storeObject(test2);
				
		Exception expected_ex = null;
		try {
			result2.get();
			fail("Should have thrown exception on duplicate insert");
		}
		catch (Exception e) {
			expected_ex = e;
		}
		assertThat(expected_ex.getCause(), instanceOf(MongoException.class));
		
		assertEquals(1, service._state.orig_coll.count());
		
		final DBObject retval2 = service._state.orig_coll.findOne();		
		
		assertEquals("{ \"_id\" : \"_id_1\" , \"test_string\" : \"test_string_1\"}", retval2.toString());		

		// 3) Add the same with override set 
		
		@SuppressWarnings("unused")
		final Future<Supplier<Object>> result3 = service.storeObject(test2, true);
		
		assertEquals(1, service._state.orig_coll.count());
		
		final DBObject retval3 = service._state.orig_coll.findOne();		
		
		assertEquals("{ \"_id\" : \"_id_1\" , \"test_string\" : \"test_string_2\"}", retval3.toString());		
		
		//4) add with no id
		
		final TestBean test4 = new TestBean();
		test4.test_string = "test_string_4";
		
		final Future<Supplier<Object>> result4 = service.storeObject(test4, true);
		
		assertEquals(2, service._state.orig_coll.count());
		
		final String id = result4.get().get().toString();
		
		final DBObject retval4 = service._state.orig_coll.findOne(id);
		
		assertEquals("test_string_4", retval4.get("test_string"));
	}
	
	public static class TestObjectIdBean {
		ObjectId _id;
	}

	@Test
	public void testCreateSingleObject_ObjectId() throws InterruptedException, ExecutionException {
		
		final MockMongoDbCrudService<TestObjectIdBean, ObjectId> service = 
				new MockMongoDbCrudService<TestObjectIdBean, ObjectId>("test", "test", "testCreateSingleObject_ObjectId", 
						TestObjectIdBean.class, ObjectId.class, Optional.empty(), Optional.empty(), Optional.empty());

		assertEquals(0, service._state.orig_coll.count());		
		
		// 1) Add a new object to an empty DB
		
		final TestObjectIdBean test = new TestObjectIdBean();
		
		final Future<Supplier<Object>> result = service.storeObject(test);
		
		assertEquals(1, service._state.orig_coll.count());
		
		final Supplier<Object> val = result.get();
		
		ObjectId id = (ObjectId)val.get();
		
		final DBObject retval = service._state.orig_coll.findOne(id);
		
		assertEquals(new BasicDBObject("_id", id), retval);		
	}	
	
	@Test
	public void testCreateMultipleObjects() throws InterruptedException, ExecutionException {
		
		final MockMongoDbCrudService<TestBean, String> service = 
				new MockMongoDbCrudService<TestBean, String>("test", "test", "testCreateMultipleObjects", 
						TestBean.class, String.class, Optional.empty(), Optional.empty(), Optional.empty());

		// 1) Insertion without ids
		
		final List<TestBean> l = IntStream.rangeClosed(1, 10).boxed()
				.map(i -> ObjectTemplateUtils.build(TestBean.class).with("test_string", "test_string" + i).done())
				.collect(Collectors.toList());

		final Future<Tuple2<Supplier<List<Object>>, Supplier<Long>>> result = service.storeObjects(l);
		
		assertEquals(10, service._state.orig_coll.count());
		assertEquals((Long)(long)10, result.get()._2().get());
		
		final List<Object> ids = result.get()._1().get();
		IntStream.rangeClosed(1, 10).boxed().map(i -> Tuples._2T(i, ids.get(i-1)))
					.forEach(io -> {
						final DBObject val = service._state.orig_coll.findOne(io._2());
						assertNotEquals(null, val);
						assertEquals("test_string" + io._1(), val.get("test_string"));
					});
		
		// 2) Insertion with ids
		
		service._state.orig_coll.drop();

		final List<TestBean> l2 = IntStream.rangeClosed(51, 100).boxed()
								.map(i -> ObjectTemplateUtils.build(TestBean.class).with("_id", "id" + i).with("test_string", "test_string" + i).done())
								.collect(Collectors.toList());
				
		final Future<Tuple2<Supplier<List<Object>>, Supplier<Long>>> result_2 = service.storeObjects(l2);
		
		assertEquals(50, service._state.orig_coll.count());
		assertEquals((Long)(long)50, result_2.get()._2().get());
		
		// 3) Insertion with dups - fail and stop

		final List<TestBean> l3 = IntStream.rangeClosed(1, 200).boxed()
				.map(i -> ObjectTemplateUtils.build(TestBean.class)
						.with("_id", "id" + i).
						with("test_string", "test_string" + i).done())
				.collect(Collectors.toList());
		
		final Future<Tuple2<Supplier<List<Object>>, Supplier<Long>>> result_3 = service.storeObjects(l3);

		Exception expected_ex = null;
		try {
			result_3.get();
			fail("Should have thrown exception on duplicate insert");
		}
		catch (Exception e) {
			expected_ex = e;
		}
		assertThat(expected_ex.getCause(), instanceOf(MongoException.class));		
		
		// Yikes - it has inserted objects up to the error though...
		assertEquals(100, service._state.orig_coll.count());		
		
		// 4) Insertion with dups - fail and continue
		
		final List<TestBean> l4 = IntStream.rangeClosed(21, 120).boxed()
				.map(i -> ObjectTemplateUtils.build(TestBean.class).with("_id", "id" + i).with("test_string", "test_string" + i).done())
				.collect(Collectors.toList());
		
		final Future<Tuple2<Supplier<List<Object>>, Supplier<Long>>> result_4 = service.storeObjects(l4, true);

		assertEquals(100L, (long)result_4.get()._2().get());		
		assertEquals(120, service._state.orig_coll.count());		
	}

	////////////////////////////////////////////////
	
	// RETRIEVAL
	
	@Test
	public void testIndexes() throws InterruptedException, ExecutionException {		
		
		final MockMongoDbCrudService<TestBean, String> service = 
				new MockMongoDbCrudService<TestBean, String>("test", "test", "testIndexes", 
						TestBean.class, String.class, Optional.empty(), Optional.empty(), Optional.empty());

		// Insert some objects to index
		
		final List<TestBean> l = IntStream.rangeClosed(1, 1000).boxed()
				.map(i -> ObjectTemplateUtils.build(TestBean.class).with("test_string", "test_string" + i).done())
				.collect(Collectors.toList());

		service.storeObjects(l);
		
		assertEquals(1000, service._state.orig_coll.count());
		
		// 1) Add a new index
		
		final List<DBObject> initial_indexes = service._state.orig_coll.getIndexInfo();
		assertEquals("[{ \"v\" : 1 , \"key\" : { \"_id\" : 1} , \"ns\" : \"test.testIndexes\" , \"name\" : \"_id_\"}]", initial_indexes.toString());
		
		final Future<Boolean> done = service.optimizeQuery(Arrays.asList("test_string", "_id"));
		
		assertEquals(true, done.get());

		final List<DBObject> new_indexes = service._state.orig_coll.getIndexInfo();		
		
		final BasicDBObject expected_index_nested = new BasicDBObject("test_string", 1);
		expected_index_nested.put("_id", 1);
		final BasicDBObject expected_index = new BasicDBObject("v", 1);
		expected_index.put("key", expected_index_nested);
		expected_index.put("ns", "test.testIndexes");
		expected_index.put( "name", "test_string_1__id_1");
		expected_index.put("background", true);
		
		final List<DBObject> expected_new_indexes = Arrays.asList(initial_indexes.get(0), expected_index);
		
		assertEquals(expected_new_indexes.toString(), new_indexes.toString());
		
		// 3) Remove an index that doesn't exist
		
		final boolean index_existed = service.deregisterOptimizedQuery(Arrays.asList("test_string", "test_long"));
		
		assertEquals(false, index_existed);

		final List<DBObject> nearly_final_indexes = service._state.orig_coll.getIndexInfo();		
		
		assertEquals(expected_new_indexes.toString(), nearly_final_indexes.toString());		
		
		// 4) Remove the index we just added
		
		final boolean index_existed_4 = service.deregisterOptimizedQuery(Arrays.asList("test_string", "_id"));
		
		assertEquals(true, index_existed_4);
		
		final List<DBObject> expected_new_indexes_4 = Arrays.asList(initial_indexes.get(0), expected_index);
		
		final List<DBObject> final_indexes = service._state.orig_coll.getIndexInfo();		
		
		assertEquals(expected_new_indexes_4.toString(), final_indexes.toString());
	}
	
	protected String copyableOutput(Object o) {
		return o.toString().replace("\"", "\\\"");
	}
	protected void sysOut(String s) {
		System.out.println(copyableOutput(s));
	}
	
	@Test
	public void singleObjectRetrieve() throws InterruptedException, ExecutionException {
		
		final MockMongoDbCrudService<TestBean, String> service = 
				new MockMongoDbCrudService<TestBean, String>("test", "test", "singleObjectRetrieve", 
						TestBean.class, String.class, Optional.empty(), Optional.empty(), Optional.empty());

		final List<TestBean> l = IntStream.rangeClosed(1, 10).boxed()
				.map(i -> ObjectTemplateUtils.build(TestBean.class)
								.with("_id", "id" + i)
								.with("test_string", "test_string" + i)
								.with("test_long", (Long)(long)i)
								.done())
				.collect(Collectors.toList());

		service.storeObjects(l);
		
		assertEquals(10, service._state.orig_coll.count());
		
		service.optimizeQuery(Arrays.asList("test_string")).get(); // (The get() waits for completion)
		
		// For asserting vs strings where possible:
		final JacksonDBCollection<TestBean, String> mapper = service._state.coll;

		// 1) Get object by _id, exists
		
		final Future<Optional<TestBean>> obj1 = service.getObjectById("id1");
		
		//DEBUG
		//sysOut(mapper.convertToDbObject(obj1.get().get()).toString());
		
		assertEquals("{ \"_id\" : \"id1\" , \"test_string\" : \"test_string1\" , \"test_long\" : 1}", mapper.convertToDbObject(obj1.get().get()).toString());
		
		// 2) Get object by _id, exists, subset of fields

		// 2a) inclusive:
		
		final Future<Optional<TestBean>> obj2a = service.getObjectById("id2", Arrays.asList("_id", "test_string"), true);		
		
		//DEBUG
		//sysOut(mapper.convertToDbObject(obj2a.get().get()).toString());
		
		assertEquals("{ \"_id\" : \"id2\" , \"test_string\" : \"test_string2\"}", mapper.convertToDbObject(obj2a.get().get()).toString());
		
		// 2b) exclusive:

		final Future<Optional<TestBean>> obj2b = service.getObjectById("id3", Arrays.asList("_id", "test_string"), false);		
		
		//DEBUG
		//sysOut(mapper.convertToDbObject(obj2b.get().get()).toString());
		
		assertEquals("{ \"test_long\" : 3}", mapper.convertToDbObject(obj2b.get().get()).toString());
		
		// 3) Get object by _id, doesn't exist
		
		final Future<Optional<TestBean>> obj3 = service.getObjectById("id100", Arrays.asList("_id", "test_string"), false);		
		
		assertEquals(false, obj3.get().isPresent());
		
		// 4) Get object by spec, exists
		
		final QueryComponent<TestBean> query = CrudUtils.allOf(TestBean.class)
					.when("_id", "id4")
					.withAny("test_string", Arrays.asList("test_string1", "test_string4"))
					.withPresent("test_long");
		
		final Future<Optional<TestBean>> obj4 = service.getObjectBySpec(query);
		
		assertEquals("{ \"_id\" : \"id4\" , \"test_string\" : \"test_string4\" , \"test_long\" : 4}", mapper.convertToDbObject(obj4.get().get()).toString());
		
		// 5) Get object by spec, exists, subset of fields
		
		final Future<Optional<TestBean>> obj5 = service.getObjectBySpec(query, Arrays.asList("_id", "test_string"), true);
		
		assertEquals("{ \"_id\" : \"id4\" , \"test_string\" : \"test_string4\"}", mapper.convertToDbObject(obj5.get().get()).toString());
				
		// 6) Get object by spec, doesn't exist

		final QueryComponent<TestBean> query6 = CrudUtils.allOf(TestBean.class)
				.when("_id", "id3")
				.withAny("test_string", Arrays.asList("test_string1", "test_string4"))
				.withPresent("test_long");
	
		
		final Future<Optional<TestBean>> obj6 = service.getObjectBySpec(query6, Arrays.asList("_id", "test_string"), false);		
		assertEquals(false, obj6.get().isPresent());
	}

	@Test
	public void multiObjectRetrieve() throws InterruptedException, ExecutionException {
		
		final MockMongoDbCrudService<TestBean, String> service = 
				new MockMongoDbCrudService<TestBean, String>("test", "test", "multiObjectRetrieve", 
						TestBean.class, String.class, Optional.empty(), Optional.empty(), Optional.empty());

		final List<TestBean> l = IntStream.rangeClosed(0, 9).boxed()
				.map(i -> ObjectTemplateUtils.build(TestBean.class)
								.with("_id", "id" + i)
								.with("test_string", "test_string" + i)
								.with("test_long", (Long)(long)i)
								.done())
				.collect(Collectors.toList());

		service.storeObjects(l);
		
		assertEquals(10, service._state.orig_coll.count());
		
		service.optimizeQuery(Arrays.asList("test_string")).get(); // (The get() waits for completion)
		
		// For asserting vs strings where possible:
		final JacksonDBCollection<TestBean, String> mapper = service._state.coll;

		// 1) Simple retrieve, no fields specified - sort

		final QueryComponent<TestBean> query = CrudUtils.allOf(TestBean.class)
				.rangeAbove("_id", "id4", true)
				.withPresent("test_long")
				.orderBy(Tuples._2T("test_string", -1));
		
		try (Cursor<TestBean> cursor = service.getObjectsBySpec(query).get()) {
		
			assertEquals(5, cursor.count());
			
			final List<TestBean> objs = StreamSupport.stream(Optionals.ofNullable(cursor).spliterator(), false).collect(Collectors.toList());
			
			assertEquals(5, objs.size());
			
			final DBObject first_obj = mapper.convertToDbObject(objs.get(0));
			
			assertEquals("{ \"_id\" : \"id9\" , \"test_string\" : \"test_string9\" , \"test_long\" : 9}", first_obj.toString());			
		} 
		catch (Exception e) {
			//(fail on close, normally carry on - but here error out)
			fail("getObjectsBySpec errored on close"); 
		}
		
		// 2) Simple retrieve, field specified (exclusive) - sort and limit

		final QueryComponent<TestBean> query_2 = CrudUtils.allOf(TestBean.class)
				.rangeAbove("_id", "id4", false)
				.withPresent("test_long")
				.orderBy(Tuples._2T("test_long", 1)).limit(4);
		
		try (Cursor<TestBean> cursor = service.getObjectsBySpec(query_2, Arrays.asList("test_string"), false).get()) {
		
			assertEquals(6, cursor.count()); // (count ignores limit)
			
			final List<TestBean> objs = StreamSupport.stream(Optionals.ofNullable(cursor).spliterator(), false).collect(Collectors.toList());
			
			assertEquals(4, objs.size());
			
			final DBObject first_obj = mapper.convertToDbObject(objs.get(0));
			
			assertEquals("{ \"_id\" : \"id4\" , \"test_long\" : 4}", first_obj.toString());			
		} 
		catch (Exception e) {
			//(fail on close, normally carry on - but here error out)
			fail("getObjectsBySpec errored on close"); 
		}
		
		// 3) Simple retrieve, no docs returned
		
		final QueryComponent<TestBean> query_3 = CrudUtils.allOf(TestBean.class)
				.rangeAbove("_id", "id9", true)
				.withPresent("test_long")
				.limit(4);
		
		try (Cursor<TestBean> cursor = service.getObjectsBySpec(query_3, Arrays.asList("test_string"), false).get()) {
			final List<TestBean> objs = StreamSupport.stream(Optionals.ofNullable(cursor).spliterator(), false).collect(Collectors.toList());
			
			assertEquals(0, objs.size());
		}
		catch (Exception e) {
			//(fail on close, normally carry on - but here error out)
			fail("getObjectsBySpec errored on close"); 
		}
	}
	
	@Test
	public void testCounting() throws InterruptedException, ExecutionException {
		
		final MockMongoDbCrudService<TestBean, String> service = 
				new MockMongoDbCrudService<TestBean, String>("test", "test", "testCounting", 
						TestBean.class, String.class, Optional.empty(), Optional.empty(), Optional.empty());

		final List<TestBean> l = IntStream.rangeClosed(0, 9).boxed()
				.map(i -> ObjectTemplateUtils.build(TestBean.class)
								.with("_id", "id" + i)
								.with("test_string", "test_string" + i)
								.with("test_long", (Long)(long)i)
								.done())
				.collect(Collectors.toList());

		service.storeObjects(l);
		
		assertEquals(10, service._state.orig_coll.count());
		
		service.optimizeQuery(Arrays.asList("test_string")).get(); // (The get() waits for completion)
		
		// 1) all docs
		
		assertEquals(10L, (long)service.countObjects().get());
		
		// 2) count subset of docs

		final QueryComponent<TestBean> query_2 = CrudUtils.allOf(TestBean.class)
				.rangeAbove("_id", "id4", false)
				.withPresent("test_long")
				.orderBy(Tuples._2T("test_long", 1));

		assertEquals(6L, (long)service.countObjectsBySpec(query_2).get());		
		
		// 3) subset of docs (limit)
		
		final QueryComponent<TestBean> query_3 = CrudUtils.allOf(TestBean.class)
				.rangeAbove("_id", "id4", false)
				.withPresent("test_long")
				.orderBy(Tuples._2T("test_long", 1)).limit(4);

		assertEquals(4L, (long)service.countObjectsBySpec(query_3).get());
		
		// 4) no docs
		
		final QueryComponent<TestBean> query_4 = CrudUtils.allOf(TestBean.class)
				.rangeAbove("_id", "id99", false)
				.withPresent("test_long")
				.orderBy(Tuples._2T("test_long", 1)).limit(4);

		assertEquals(0L, (long)service.countObjectsBySpec(query_4).get());
	}
	
	//TODO (updates)
	
	//TODO (find and modify)
	
	//TODO (deletes)
	
}