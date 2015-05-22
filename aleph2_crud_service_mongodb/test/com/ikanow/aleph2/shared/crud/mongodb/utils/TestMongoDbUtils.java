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
package com.ikanow.aleph2.shared.crud.mongodb.utils;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;
import org.mongojack.internal.MongoJackModule;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.MultiQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.BeanTemplate;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.shared.crud.mongodb.utils.MongoDbUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public class TestMongoDbUtils {

	// Test objects
	
	public static class TestBean {
		public static class NestedNestedTestBean {
			public String nested_nested_string_field() { return nested_nested_string_field; }
			
			private String nested_nested_string_field;
		}
		public static class NestedTestBean {
			public String nested_string_field() { return nested_string_field; }
			public NestedNestedTestBean nested_object() { return nested_object; }
			public List<String> nested_string_list() { return nested_string_list; }
			
			private List<String> nested_string_list;
			private String nested_string_field;
			private NestedNestedTestBean nested_object;
		}		
		public String string_field() { return string_field; }
		public List<String> string_fields() { return string_fields; }
		public Boolean bool_field() { return bool_field; }
		public Long long_field() { return long_field; }
		public List<NestedTestBean> nested_list() { return nested_list; }
		public Map<String, String> map() { return map; }
		public NestedTestBean nested_object() { return nested_object; }
		
		protected TestBean() {}
		private String string_field;
		private List<String> string_fields;
		private Boolean bool_field;
		private Long long_field;
		private List<NestedTestBean> nested_list;
		private Map<String, String> map;
		private NestedTestBean nested_object;
	}
	
	///////////////////////////////////////////////
	
	// QUERY CREATION TESTING
	
	@Test
	public void emptyQuery() {
		
		// No meta:
		
		final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(BeanTemplate.of(new TestBean())); 
		
		final Tuple2<DBObject, DBObject> query_meta_1 = MongoDbUtils.convertToMongoQuery(query_comp_1);

		assertEquals("{ }", query_meta_1._1().toString());
		assertEquals("{ }", query_meta_1._2().toString());
		
		// Meta fields
		
		BeanTemplate<TestBean> template2 = BeanTemplateUtils.build(TestBean.class).with(TestBean::string_field, null).done();
		
		final SingleQueryComponent<TestBean> query_comp_2 = CrudUtils.anyOf(template2)
													.orderBy(Tuples._2T("test_field_1", 1), Tuples._2T("test_field_2", -1));		

		final Tuple2<DBObject, DBObject> query_meta_2 = MongoDbUtils.convertToMongoQuery(query_comp_2);
		
		assertEquals("{ }", query_meta_2._1().toString());
		final BasicDBObject expected_meta_nested = new BasicDBObject("test_field_1", 1);
		expected_meta_nested.put("test_field_2", -1);
		final BasicDBObject expected_meta = new BasicDBObject("$sort", expected_meta_nested);
		assertEquals(expected_meta.toString(), query_meta_2._2().toString());
	}
	
	@Test
	public void basicSingleTest() {
		
		// Queries starting with allOf
		
		// Very simple

		BeanTemplate<TestBean> template1 = BeanTemplateUtils.build(TestBean.class).with(TestBean::string_field, "string_field").done();
		
		final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(template1).when(TestBean::bool_field, true);
		
		final SingleQueryComponent<TestBean> query_comp_1b = CrudUtils.allOf(TestBean.class)
				.when("bool_field", true)
				.when("string_field", "string_field");
		
		final Tuple2<DBObject, DBObject> query_meta_1 = MongoDbUtils.convertToMongoQuery(query_comp_1);
		final Tuple2<DBObject, DBObject> query_meta_1b = MongoDbUtils.convertToMongoQuery(query_comp_1b);
		
		final DBObject expected_1 = QueryBuilder.start().and(
						QueryBuilder.start("bool_field").is(true).get(),
						QueryBuilder.start("string_field").is("string_field").get()
							).get();
		
		assertEquals(expected_1.toString(), query_meta_1._1().toString());
		assertEquals(expected_1.toString(), query_meta_1b._1().toString());
		assertEquals("{ }", query_meta_1._2().toString());
		
		// Includes extra + all the checks except the range checks
		
		final SingleQueryComponent<TestBean> query_comp_2 = CrudUtils.anyOf(TestBean.class)
				.when(TestBean::string_field, "string_field")
				.withPresent(TestBean::bool_field)
				.withNotPresent(TestBean::long_field)
				.withAny(TestBean::string_field, Arrays.asList("test1a", "test1b"))
				.withAll(TestBean::long_field, Arrays.asList(10, 11, 12))
				.whenNot(TestBean::long_field, 13)
				.limit(100);

		final SingleQueryComponent<TestBean> query_comp_2b = CrudUtils.anyOf(TestBean.class)
				.when("string_field", "string_field")
				.withPresent("bool_field")
				.withNotPresent("long_field")
				.withAny("string_field", Arrays.asList("test1a", "test1b"))
				.withAll("long_field", Arrays.asList(10, 11, 12))
				.whenNot("long_field", 13)
				.limit(100);		
		
		final Tuple2<DBObject, DBObject> query_meta_2 = MongoDbUtils.convertToMongoQuery(query_comp_2);
		final Tuple2<DBObject, DBObject> query_meta_2b = MongoDbUtils.convertToMongoQuery(query_comp_2b);
		
		final DBObject expected_2 = QueryBuilder.start().or(
										QueryBuilder.start("string_field").is("string_field").get(),
										QueryBuilder.start("string_field").in(Arrays.asList("test1a", "test1b")).get(),
										QueryBuilder.start("bool_field").exists(true).get(),
										QueryBuilder.start("long_field").exists(false).get(),
										QueryBuilder.start("long_field").all(Arrays.asList(10, 11, 12)).get(),
										QueryBuilder.start("long_field").notEquals(13).get()										
									).get();
		
		assertEquals(expected_2.toString(), query_meta_2._1().toString());
		assertEquals(expected_2.toString(), query_meta_2b._1().toString());
		assertEquals("{ \"$limit\" : 100}", query_meta_2._2().toString());		
	}
	
	@Test
	public void testAllTheRangeQueries() {
		
		final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(TestBean.class)
				.rangeAbove(TestBean::string_field, "bbb", true)
				.rangeBelow(TestBean::string_field, "fff", false)
				.rangeIn(TestBean::string_field, "ccc", false, "ddd", true)
				.rangeIn(TestBean::string_field, "xxx", false, "yyy", false)
				
				.rangeAbove(TestBean::long_field, 1000, false)
				.rangeBelow(TestBean::long_field, 10000, true)
				.rangeIn(TestBean::long_field, 2000, true, 20000, true)
				.rangeIn(TestBean::long_field, 3000, true, 30000, false)
				
				.orderBy(Tuples._2T("test_field_1", 1), Tuples._2T("test_field_2", -1))
				.limit(200);

		final SingleQueryComponent<TestBean> query_comp_1b = CrudUtils.allOf(TestBean.class)
				.rangeAbove("string_field", "bbb", true)
				.rangeBelow("string_field", "fff", false)
				.rangeIn("string_field", "ccc", false, "ddd", true)
				.rangeIn("string_field", "xxx", false, "yyy", false)
				
				.rangeAbove("long_field", 1000, false)
				.rangeBelow("long_field", 10000, true)
				.rangeIn("long_field", 2000, true, 20000, true)
				.rangeIn("long_field", 3000, true, 30000, false)
				
				.orderBy(Tuples._2T("test_field_1", 1)).orderBy(Tuples._2T("test_field_2", -1))		
				.limit(200);
		
		final Tuple2<DBObject, DBObject> query_meta_1 = MongoDbUtils.convertToMongoQuery(query_comp_1);
		final Tuple2<DBObject, DBObject> query_meta_1b = MongoDbUtils.convertToMongoQuery(query_comp_1b);

		final DBObject expected_1 = QueryBuilder.start().and(
				QueryBuilder.start("string_field").greaterThan("bbb").get(),
				QueryBuilder.start("string_field").lessThanEquals("fff").get(),
				QueryBuilder.start("string_field").greaterThanEquals("ccc").lessThan("ddd").get(),
				QueryBuilder.start("string_field").greaterThanEquals("xxx").lessThanEquals("yyy").get(),
				
				QueryBuilder.start("long_field").greaterThanEquals(1000).get(),
				QueryBuilder.start("long_field").lessThan(10000).get(),
				QueryBuilder.start("long_field").greaterThan(2000).lessThan(20000).get(),
				QueryBuilder.start("long_field").greaterThan(3000).lessThanEquals(30000).get()
				
				).get();

		final BasicDBObject expected_meta_nested = new BasicDBObject("test_field_1", 1);
		expected_meta_nested.put("test_field_2", -1);
		final BasicDBObject expected_meta = new BasicDBObject("$limit", 200);
		expected_meta.put("$sort", expected_meta_nested);
		
		assertEquals(expected_1.toString(), query_meta_1._1().toString());
		assertEquals(expected_1.toString(), query_meta_1b._1().toString());
		assertEquals(expected_meta.toString(), query_meta_1._2().toString());
		
	}

	@Test 
	public void testNestedQueries() {
		
		// 1 level of nesting
		
		final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(TestBean.class)
														.when(TestBean::string_field, "a")
														.nested(TestBean::nested_list, 
																CrudUtils.anyOf(TestBean.NestedTestBean.class)
																	.when(TestBean.NestedTestBean::nested_string_field, "x")
																	.withAny(TestBean.NestedTestBean::nested_string_field, Arrays.asList("x", "y"))
																	.rangeIn("nested_string_field", "ccc", false, "ddd", true)
																	.limit(1000) // (should be ignored)
																	.orderBy(Tuples._2T("test_field_1", 1)) // (should be ignored)
														)
														.withPresent("long_field")
														.limit(5) 
														.orderBy(Tuples._2T("test_field_2", -1));
														
		final Tuple2<DBObject, DBObject> query_meta_1 = MongoDbUtils.convertToMongoQuery(query_comp_1);
		
				
		final DBObject expected_1 = QueryBuilder.start().and(
				QueryBuilder.start("string_field").is("a").get(),
				QueryBuilder.start("nested_list.nested_string_field").is("x").get(),
				QueryBuilder.start("nested_list.nested_string_field").in(Arrays.asList("x", "y")).get(),
				QueryBuilder.start("nested_list.nested_string_field").greaterThanEquals("ccc").lessThan("ddd").get(),
				QueryBuilder.start("long_field").exists(true).get()
				).get();		
		
		final BasicDBObject expected_meta_nested = new BasicDBObject("test_field_2", -1);
		final BasicDBObject expected_meta = new BasicDBObject("$limit", 5);
		expected_meta.put("$sort", expected_meta_nested);
		
		assertEquals(expected_1.toString(), query_meta_1._1().toString());
		assertEquals(expected_meta.toString(), query_meta_1._2().toString());
		
		// 2 levels of nesting

		BeanTemplate<TestBean.NestedTestBean> nestedBean = BeanTemplateUtils.build(TestBean.NestedTestBean.class).with("nested_string_field", "x").done();
		
		final SingleQueryComponent<TestBean> query_comp_2 = CrudUtils.allOf(TestBean.class)
				.when(TestBean::string_field, "a")
				.nested(TestBean::nested_list, 
						CrudUtils.anyOf(nestedBean)
							.when(TestBean.NestedTestBean::nested_string_field, "y")
							.nested(TestBean.NestedTestBean::nested_object,
									CrudUtils.allOf(TestBean.NestedNestedTestBean.class)
										.when(TestBean.NestedNestedTestBean::nested_nested_string_field, "z")
										.withNotPresent(TestBean.NestedNestedTestBean::nested_nested_string_field)
										.limit(1000) // (should be ignored)
										.orderBy(Tuples._2T("test_field_1", 1)) // (should be ignored)
									)
							.withAny(TestBean.NestedTestBean::nested_string_field, Arrays.asList("x", "y"))
							.rangeIn("nested_string_field", "ccc", false, "ddd", true)
				)
				.withPresent("long_field");
				
		final Tuple2<DBObject, DBObject> query_meta_2 = MongoDbUtils.convertToMongoQuery(query_comp_2);
		
		final DBObject expected_2 = QueryBuilder.start().and(
				QueryBuilder.start("string_field").is("a").get(),
				QueryBuilder.start("nested_list.nested_string_field").is("x").get(),
				QueryBuilder.start("nested_list.nested_string_field").is("y").get(),
				QueryBuilder.start("nested_list.nested_string_field").in(Arrays.asList("x", "y")).get(),	
				QueryBuilder.start("nested_list.nested_string_field").greaterThanEquals("ccc").lessThan("ddd").get(),
				QueryBuilder.start("nested_list.nested_object.nested_nested_string_field").is("z").get(),
				QueryBuilder.start("nested_list.nested_object.nested_nested_string_field").exists(false).get(),
				QueryBuilder.start("long_field").exists(true).get()
				).get();		
		
		assertEquals(expected_2.toString(), query_meta_2._1().toString());
		assertEquals("{ }", query_meta_2._2().toString());
	}
	
	@Test
	public void testMultipleQueries() {

		// Just to test .. single node versions
		
		final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(TestBean.class)
				.rangeAbove(TestBean::string_field, "bbb", true)
				.rangeBelow(TestBean::string_field, "fff", false)
				.rangeIn(TestBean::string_field, "ccc", false, "ddd", true)
				.rangeIn(TestBean::string_field, "xxx", false, "yyy", false)
				
				.rangeAbove(TestBean::long_field, 1000, false)
				.rangeBelow(TestBean::long_field, 10000, true)
				.rangeIn(TestBean::long_field, 2000, true, 20000, true)
				.rangeIn(TestBean::long_field, 3000, true, 30000, false)
				
				.orderBy(Tuples._2T("test_field_1", 1))	// should be ignored
				.limit(200); // should be ignored

		final MultiQueryComponent<TestBean> multi_query_1 = CrudUtils.<TestBean>allOf(query_comp_1).orderBy(Tuples._2T("test_field_2", -1)).limit(5);
		final MultiQueryComponent<TestBean> multi_query_2 = CrudUtils.<TestBean>anyOf(query_comp_1);
				
		final QueryBuilder expected_1 = QueryBuilder.start().and(
				QueryBuilder.start("string_field").greaterThan("bbb").get(), 
				QueryBuilder.start("string_field").lessThanEquals("fff").get(),
				QueryBuilder.start("string_field").greaterThanEquals("ccc").lessThan("ddd").get(),
				QueryBuilder.start("string_field").greaterThanEquals("xxx").lessThanEquals("yyy").get(),
				
				QueryBuilder.start("long_field").greaterThanEquals(1000).get(),
				QueryBuilder.start("long_field").lessThan(10000).get(),
				QueryBuilder.start("long_field").greaterThan(2000).lessThan(20000).get(),
				QueryBuilder.start("long_field").greaterThan(3000).lessThanEquals(30000).get()
				
				);
		
		final Tuple2<DBObject, DBObject> query_meta_1 = MongoDbUtils.convertToMongoQuery(multi_query_1);
		final Tuple2<DBObject, DBObject> query_meta_2 = MongoDbUtils.convertToMongoQuery(multi_query_2);
		
		final DBObject multi_expected_1 = QueryBuilder.start().and((DBObject)expected_1.get()).get();
		final DBObject multi_expected_2 = QueryBuilder.start().or((DBObject)expected_1.get()).get();
		
		assertEquals(multi_expected_1.toString(), query_meta_1._1().toString());
		assertEquals(multi_expected_2.toString(), query_meta_2._1().toString());

		final BasicDBObject expected_meta_nested = new BasicDBObject("test_field_2", -1);
		final BasicDBObject expected_meta = new BasicDBObject("$limit", 5);
		expected_meta.put("$sort", expected_meta_nested);

		assertEquals(expected_meta.toString(), query_meta_1._2().toString());
		assertEquals("{ }", query_meta_2._2().toString());		
		
		// Multiple nested
		
		final SingleQueryComponent<TestBean> query_comp_2 = CrudUtils.allOf(TestBean.class)
				.when(TestBean::string_field, "a")
				.nested(TestBean::nested_list, 
						CrudUtils.anyOf(TestBean.NestedTestBean.class)
							.when(TestBean.NestedTestBean::nested_string_field, "x")
							.nested(TestBean.NestedTestBean::nested_object,
									CrudUtils.allOf(TestBean.NestedNestedTestBean.class)
										.when(TestBean.NestedNestedTestBean::nested_nested_string_field, "z")
										.withNotPresent(TestBean.NestedNestedTestBean::nested_nested_string_field)
										.limit(1000) // (should be ignored)
										.orderBy(Tuples._2T("test_field_1", 1)) // (should be ignored)
									)
							.withAny(TestBean.NestedTestBean::nested_string_field, Arrays.asList("x", "y"))
							.rangeIn("nested_string_field", "ccc", false, "ddd", true)
				)
				.withPresent("long_field");
		
		final MultiQueryComponent<TestBean> multi_query_3 = CrudUtils.allOf(query_comp_1, query_comp_2).limit(5);
		final MultiQueryComponent<TestBean> multi_query_4 = CrudUtils.anyOf(query_comp_1, query_comp_2).orderBy(Tuples._2T("test_field_2", -1));

		final MultiQueryComponent<TestBean> multi_query_5 = CrudUtils.<TestBean>allOf(query_comp_1).also(query_comp_2).limit(5);
		final MultiQueryComponent<TestBean> multi_query_6 = CrudUtils.<TestBean>anyOf(query_comp_1).also(query_comp_2).orderBy().orderBy(Tuples._2T("test_field_2", -1));
		
		
		final Tuple2<DBObject, DBObject> query_meta_3 = MongoDbUtils.convertToMongoQuery(multi_query_3);
		final Tuple2<DBObject, DBObject> query_meta_4 = MongoDbUtils.convertToMongoQuery(multi_query_4);
		final Tuple2<DBObject, DBObject> query_meta_5 = MongoDbUtils.convertToMongoQuery(multi_query_5);
		final Tuple2<DBObject, DBObject> query_meta_6 = MongoDbUtils.convertToMongoQuery(multi_query_6);
		
		final QueryBuilder expected_2 = QueryBuilder.start().and(
				QueryBuilder.start("string_field").is("a").get(),
				QueryBuilder.start("nested_list.nested_string_field").is("x").get(),
				QueryBuilder.start("nested_list.nested_string_field").in(Arrays.asList("x", "y")).get(),	
				QueryBuilder.start("nested_list.nested_string_field").greaterThanEquals("ccc").lessThan("ddd").get(),
				QueryBuilder.start("nested_list.nested_object.nested_nested_string_field").is("z").get(),
				QueryBuilder.start("nested_list.nested_object.nested_nested_string_field").exists(false).get(),
				QueryBuilder.start("long_field").exists(true).get()
				);		
		
		final DBObject multi_expected_3 = QueryBuilder.start().and((DBObject)expected_1.get(), (DBObject)expected_2.get()).get();
		final DBObject multi_expected_4 = QueryBuilder.start().or((DBObject)expected_1.get(), (DBObject)expected_2.get()).get();
		
		assertEquals(multi_expected_3.toString(), query_meta_3._1().toString());
		assertEquals(multi_expected_4.toString(), query_meta_4._1().toString());
		assertEquals(multi_expected_3.toString(), query_meta_5._1().toString());
		assertEquals(multi_expected_4.toString(), query_meta_6._1().toString());

		final BasicDBObject expected_meta_nested_2 = new BasicDBObject("test_field_2", -1);
		final BasicDBObject expected_meta_2 = new BasicDBObject("$sort", expected_meta_nested_2);

		assertEquals("{ \"$limit\" : 5}", query_meta_3._2().toString());		
		assertEquals(expected_meta_2.toString(), query_meta_4._2().toString());
		assertEquals("{ \"$limit\" : 5}", query_meta_5._2().toString());		
		assertEquals(expected_meta_2.toString(), query_meta_6._2().toString());
	}
	
	///////////////////////////////////////////////
	
	// UPDATE CREATION TESTING
	
	///////////////////////////////////////////////////////////////////////	
	///////////////////////////////////////////////////////////////////////	
	///////////////////////////////////////////////////////////////////////	

	// UPDATE TESTING - BEAN 
	
	@Test
	public void updateBeanTest() {
		
		// Test 1 - getter fields
		
		final BeanTemplate<TestBean.NestedTestBean> nested1 = BeanTemplateUtils.build(TestBean.NestedTestBean.class)
																	.with("nested_string_field", "test1").done(); //(2)
		final UpdateComponent<TestBean> test1 = 
				CrudUtils.update(TestBean.class)
					.add(TestBean::string_fields, "AA", false) //(0)
					.increment(TestBean::long_field, 4) //(1)
					.nested(TestBean::nested_list, 
							CrudUtils.update(nested1) //(2)
								.unset(TestBean.NestedTestBean::nested_string_field) //(3a)
								.remove(TestBean.NestedTestBean::nested_string_list, Arrays.asList("x", "y", "z")) //(4)
								.add(TestBean.NestedTestBean::nested_string_list, "A", true) // (5)
							)
					.unset(TestBean::bool_field) //(3b)
					.unset(TestBean::nested_object) //(3c)
					.remove(TestBean::nested_list, CrudUtils.allOf(TestBean.NestedTestBean.class).when("nested_string_field", "1")) //6)
					;
		
		final DBObject result1 = MongoDbUtils.createUpdateObject(test1);
		
		final String expected1 = "{ \"$push\" : { \"string_fields\" : \"AA\"} , \"$inc\" : { \"long_field\" : 4} , \"$set\" : { \"nested_list.nested_string_field\" : \"test1\"} , \"$unset\" : { \"nested_list.nested_string_field\" : 1 , \"bool_field\" : 1 , \"nested_object\" : 1} , \"$pullAll\" : { \"nested_list.nested_string_list\" : [ \"x\" , \"y\" , \"z\"]} , \"$addToSet\" : { \"nested_list.nested_string_list\" : \"A\"} , \"$pull\" : { \"nested_list\" : { \"$and\" : [ { \"nested_string_field\" : \"1\"}]}}}";
		// (see above for analysis of results) 
		
		assertEquals(expected1, result1.toString());
		
		// Test 1b - string fields
		
		final BeanTemplate<TestBean.NestedTestBean> nested1b = BeanTemplateUtils.build(TestBean.NestedTestBean.class)
																	.with("nested_string_field", "test1").done(); //(2)
		final UpdateComponent<TestBean> test1b = 
				CrudUtils.update(TestBean.class)
					.add("string_fields", "AA", false) //(0)
					.increment("long_field", 4) //(1)
					.nested("nested_list", 
							CrudUtils.update(nested1b) //(2)
								.unset("nested_string_field") //(3a)
								.remove("nested_string_list", Arrays.asList("x", "y", "z")) //(4)
								.add("nested_string_list", "A", true) // (5)
							)
					.unset("bool_field") //(3b)
					.unset("nested_object") //(3c)
					.remove("nested_list", CrudUtils.allOf(TestBean.NestedTestBean.class).when("nested_string_field", "1")) //6)
					;
		
		final DBObject result1b = MongoDbUtils.createUpdateObject(test1b);
		
		final String expected1b = "{ \"$push\" : { \"string_fields\" : \"AA\"} , \"$inc\" : { \"long_field\" : 4} , \"$set\" : { \"nested_list.nested_string_field\" : \"test1\"} , \"$unset\" : { \"nested_list.nested_string_field\" : 1 , \"bool_field\" : 1 , \"nested_object\" : 1} , \"$pullAll\" : { \"nested_list.nested_string_list\" : [ \"x\" , \"y\" , \"z\"]} , \"$addToSet\" : { \"nested_list.nested_string_list\" : \"A\"} , \"$pull\" : { \"nested_list\" : { \"$and\" : [ { \"nested_string_field\" : \"1\"}]}}}";
		// (see above for analysis of results) 
		
		assertEquals(expected1b, result1b.toString());
		
		// Test2 - more coverage
		
		final BeanTemplate<TestBean> testbean2 = BeanTemplateUtils.build(TestBean.class).with(TestBean::map, ImmutableMap.<String, String>builder().put("a", "b").build()).done();
		
		final UpdateComponent<TestBean> test2 = 
				CrudUtils.update(testbean2) //(3)
					.set(TestBean::string_fields, "A")
					.add(TestBean::string_fields, Arrays.asList("a", "b", "c"), true) //(1)
					.set("long_field", 1L)
					.nested("nested_list",
							CrudUtils.update(TestBean.NestedTestBean.class) 
								.add(TestBean.NestedTestBean::nested_string_list, "A", false) // (will be overwritten)
							)
					.add("nested_list.nested_string_list", Arrays.asList("x", "y"), false); //(2)
		
		final DBObject result2 = MongoDbUtils.createUpdateObject(test2);
		
		final String expected2 = "{ \"$set\" : { \"string_fields\" : \"A\" , \"long_field\" : 1 , \"map\" : { \"a\" : \"b\"}} , \"$addToSet\" : { \"string_fields\" : { \"$each\" : [ \"a\" , \"b\" , \"c\"]}} , \"$push\" : { \"nested_list.nested_string_list\" : { \"$each\" : [ \"x\" , \"y\"]}}}";
		// (see above for analysis of results) 
		
		assertEquals(expected2, result2.toString());
				
		// Test3 - delete object
		
		final UpdateComponent<TestBean> test3 = CrudUtils.update(TestBean.class).deleteObject();
		
		final DBObject result3 = MongoDbUtils.createUpdateObject(test3);
		
		assertEquals("{ \"$unset\" :  null }", result3.toString());				
		
		// Test4 - bean templates as JsonNode (see _json for the other way round!)

		final BeanTemplate<TestBean.NestedTestBean> nested4a = BeanTemplateUtils.build(TestBean.NestedTestBean.class)
				.with("nested_string_field", "test4a").done(); //(2)

		final BeanTemplate<TestBean.NestedTestBean> nested4b = BeanTemplateUtils.build(TestBean.NestedTestBean.class)
				.with("nested_string_field", "test4b").done(); //(2)
		
		//convert to JsonNode:
		final ObjectMapper object_mapper = MongoJackModule.configure(BeanTemplateUtils.configureMapper(Optional.empty()));
		JsonNode nested4a_json = object_mapper.valueToTree(nested4a.get());
		JsonNode nested4b_json = object_mapper.valueToTree(nested4b.get());
		
		final UpdateComponent<TestBean> test4 = 
				CrudUtils.update(TestBean.class)
					.set(TestBean::nested_object, nested4a_json)
					.add(TestBean::nested_list, Arrays.asList(nested4a_json, nested4b_json), false);
				
		final DBObject result4 = MongoDbUtils.createUpdateObject(test4);
		
		assertEquals("{ \"$set\" : { \"nested_object\" : { \"nested_string_field\" : \"test4a\"}} , \"$push\" : { \"nested_list\" : { \"$each\" : [ { \"nested_string_field\" : \"test4a\"} , { \"nested_string_field\" : \"test4b\"}]}}}", result4.toString());
	}
		
	//////////////////////////
	
	@Test
	public void updateBeanTest_json() {
		
		// Test 1 - getters
		
		final BeanTemplate<TestBean.NestedTestBean> nested1 = BeanTemplateUtils.build(TestBean.NestedTestBean.class)
																	.with("nested_string_field", "test1").done(); //(2)
		final UpdateComponent<JsonNode> test1 = 
				CrudUtils.update_json(TestBean.class)
					.add(TestBean::string_fields, "AA", false) //(0)
					.increment(TestBean::long_field, 4) //(1)
					.nested(TestBean::nested_list, 
							CrudUtils.update(nested1) //(2)
								.unset(TestBean.NestedTestBean::nested_string_field) //(3a)
								.remove(TestBean.NestedTestBean::nested_string_list, Arrays.asList("x", "y", "z")) //(4)
								.add(TestBean.NestedTestBean::nested_string_list, "A", true) // (5)
							)
					.unset(TestBean::bool_field) //(3b)
					.unset(TestBean::nested_object) //(3c)
					.remove(TestBean::nested_list, CrudUtils.allOf(TestBean.NestedTestBean.class).when("nested_string_field", "1")) //6)
					;
		
		final DBObject result1 = MongoDbUtils.createUpdateObject(test1);
		
		final String expected1 = "{ \"$push\" : { \"string_fields\" : \"AA\"} , \"$inc\" : { \"long_field\" : 4} , \"$set\" : { \"nested_list.nested_string_field\" : \"test1\"} , \"$unset\" : { \"nested_list.nested_string_field\" : 1 , \"bool_field\" : 1 , \"nested_object\" : 1} , \"$pullAll\" : { \"nested_list.nested_string_list\" : [ \"x\" , \"y\" , \"z\"]} , \"$addToSet\" : { \"nested_list.nested_string_list\" : \"A\"} , \"$pull\" : { \"nested_list\" : { \"$and\" : [ { \"nested_string_field\" : \"1\"}]}}}";
		// (see above for analysis of results) 
		
		assertEquals(expected1, result1.toString());
		
		// Test 1b - string fields
		
		final BeanTemplate<TestBean.NestedTestBean> nested1b = BeanTemplateUtils.build(TestBean.NestedTestBean.class)
																	.with("nested_string_field", "test1").done(); //(2)
		final UpdateComponent<JsonNode> test1b = 
				CrudUtils.update_json(TestBean.class)
					.add("string_fields", "AA", false) //(0)
					.increment("long_field", 4) //(1)
					.nested("nested_list", 
							CrudUtils.update(nested1b) //(2)
								.unset("nested_string_field") //(3a)
								.remove("nested_string_list", "x") //(4)
								.add("nested_string_list", "A", true) // (5)
							)
					.unset("bool_field") //(3b)
					.unset("nested_object") //(3c)
					.remove("nested_list", CrudUtils.allOf(TestBean.NestedTestBean.class).when("nested_string_field", "1")) //6)
					;
		
		final DBObject result1b = MongoDbUtils.createUpdateObject(test1b);
		
		final String expected1b = "{ \"$push\" : { \"string_fields\" : \"AA\"} , \"$inc\" : { \"long_field\" : 4} , \"$set\" : { \"nested_list.nested_string_field\" : \"test1\"} , \"$unset\" : { \"nested_list.nested_string_field\" : 1 , \"bool_field\" : 1 , \"nested_object\" : 1} , \"$pullAll\" : { \"nested_list.nested_string_list\" : [ \"x\"]} , \"$addToSet\" : { \"nested_list.nested_string_list\" : \"A\"} , \"$pull\" : { \"nested_list\" : { \"$and\" : [ { \"nested_string_field\" : \"1\"}]}}}";
		// (see above for analysis of results) 
		
		assertEquals(expected1b, result1b.toString());
		
		// Test2 - More coverage
		
		final BeanTemplate<TestBean> testbean2 = BeanTemplateUtils.build(TestBean.class).with(TestBean::map, ImmutableMap.<String, String>builder().put("a", "b").build()).done();
		
		final UpdateComponent<JsonNode> test2 = 
				CrudUtils.update_json(testbean2) //(3)
					.set(TestBean::string_fields, "A")
					.add(TestBean::string_fields, Arrays.asList("a", "b", "c"), true) //(1)
					.set("long_field", 1L)
					.nested("nested_list",
							CrudUtils.update(TestBean.NestedTestBean.class) 
								.add(TestBean.NestedTestBean::nested_string_list, "A", false) // (will be overwritten)
							)
					.add("nested_list.nested_string_list", Arrays.asList("x", "y"), false); //(2)
		
		final DBObject result2 = MongoDbUtils.createUpdateObject(test2);
		
		final String expected2 = "{ \"$set\" : { \"string_fields\" : \"A\" , \"long_field\" : 1 , \"map\" : { \"a\" : \"b\"}} , \"$addToSet\" : { \"string_fields\" : { \"$each\" : [ \"a\" , \"b\" , \"c\"]}} , \"$push\" : { \"nested_list.nested_string_list\" : { \"$each\" : [ \"x\" , \"y\"]}}}";
		// (see above for analysis of results) 
		
		assertEquals(expected2, result2.toString());
				
		// Test3 - delete object
		
		final UpdateComponent<JsonNode> test3 = CrudUtils.update_json(TestBean.class).deleteObject();
		
		final DBObject result3 = MongoDbUtils.createUpdateObject(test3);
		
		assertEquals("{ \"$unset\" :  null }", result3.toString());
		
		// Test4 - bean templates

		final BeanTemplate<TestBean.NestedTestBean> nested4a = BeanTemplateUtils.build(TestBean.NestedTestBean.class)
				.with("nested_string_field", "test4a").done(); //(2)

		final BeanTemplate<TestBean.NestedTestBean> nested4b = BeanTemplateUtils.build(TestBean.NestedTestBean.class)
				.with("nested_string_field", "test4b").done(); //(2)

		
		final UpdateComponent<JsonNode> test4 = 
				CrudUtils.update_json(TestBean.class)
					.set(TestBean::nested_object, nested4a)
					.add(TestBean::nested_list, Arrays.asList(nested4a, nested4b), false);
				
		final DBObject result4 = MongoDbUtils.createUpdateObject(test4);
		
		assertEquals("{ \"$set\" : { \"nested_object\" : { \"nested_string_field\" : \"test4a\"}} , \"$push\" : { \"nested_list\" : { \"$each\" : [ { \"nested_string_field\" : \"test4a\"} , { \"nested_string_field\" : \"test4b\"}]}}}", result4.toString());
	}
	
}
