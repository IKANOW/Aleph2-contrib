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
package com.ikanow.aleph2.shared.crud.elasticsearch.utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.MultiQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.BeanTemplate;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Tuples;

public class TestElasticsearchUtils {

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
		public String _id() { return _id; }
		public String string_field() { return string_field; }
		public List<String> string_fields() { return string_fields; }
		public Boolean bool_field() { return bool_field; }
		public Long long_field() { return long_field; }
		public List<NestedTestBean> nested_list() { return nested_list; }
		public Map<String, String> map() { return map; }
		public NestedTestBean nested_object() { return nested_object; }
		
		protected TestBean() {}
		private String _id;
		private String string_field;
		private List<String> string_fields;
		private Boolean bool_field;
		private Long long_field;
		private List<NestedTestBean> nested_list;
		private Map<String, String> map;
		private NestedTestBean nested_object;
	}

	public static class MockSearchRequestBuilder {
		final ArrayList<Tuple2<String, SortOrder>> sort = new ArrayList<>();
		final ArgumentCaptor<Integer> size_arg = ArgumentCaptor.forClass(int.class);
		final SearchRequestBuilder srb;
		@SuppressWarnings("rawtypes")
		MockSearchRequestBuilder() {
			srb = Mockito.mock(SearchRequestBuilder.class);
			ArgumentCaptor<String> sort_field = ArgumentCaptor.forClass(String.class);
			ArgumentCaptor<SortOrder> sort_order = ArgumentCaptor.forClass(SortOrder.class);
			Mockito.when(srb.addSort(sort_field.capture(), sort_order.capture())).thenAnswer(
					new Answer() {
						public Object answer(InvocationOnMock invocation) {
							sort.add(Tuples._2T(sort_field.getValue(), sort_order.getValue()));
							return srb;
						}
					});
			Mockito.when(srb.setSize(size_arg.capture())).thenReturn(srb);
		}
	}
	
	private static String sortOutQuotesAndStuff(final String before) {
		return before.replace("\"", "'").replace("\n", "").replace("\r", "");
	}
	private static String toXContentThenString(final FilterBuilder f) throws IOException {
		XContentBuilder result_1 = XContentFactory.contentBuilder(XContentType.JSON);
		f.toXContent(result_1, ToXContent.EMPTY_PARAMS);
		return sortOutQuotesAndStuff(result_1.string());		
	}
	
	///////////////////////////////////////////////
	
	// QUERY CREATION TESTING
	
	@Test
	public void test_emptyQuery() {
		
		// No meta:
		
		{
			final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(BeanTemplate.of(new TestBean())); 		
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_1 = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_1);
			MockSearchRequestBuilder msrb = new MockSearchRequestBuilder();
			query_meta_1._2().apply(msrb.srb);
			assertEquals("{  'match_all' : { }}", sortOutQuotesAndStuff(query_meta_1._1().toString()));
			assertEquals(0, msrb.size_arg.getAllValues().size());
			assertEquals(0, msrb.sort.size());
		}		
		// Meta fields
		
		{
			BeanTemplate<TestBean> template2 = BeanTemplateUtils.build(TestBean.class).with(TestBean::string_field, null).done();
			
			final SingleQueryComponent<TestBean> query_comp_2 = CrudUtils.anyOf(template2)
														.orderBy(Tuples._2T("test_field_1", 1), Tuples._2T("test_field_2", -1));		
	
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_2 = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_2);
			
			MockSearchRequestBuilder msrb = new MockSearchRequestBuilder();
			query_meta_2._2().apply(msrb.srb);
			assertEquals("{  'match_all' : { }}", sortOutQuotesAndStuff(query_meta_2._1().toString()));
			assertEquals(0, msrb.size_arg.getAllValues().size());
			assertEquals(Arrays.asList(Tuples._2T("test_field_1", SortOrder.ASC), Tuples._2T("test_field_2", SortOrder.DESC)), msrb.sort);
		}
	}
	
	@Test
	public void test_basicSingleTest() throws IOException {
		
		// Queries starting with allOf
		
		// Very simple

		{
			BeanTemplate<TestBean> template1 = BeanTemplateUtils.build(TestBean.class).with(TestBean::string_field, "string_field").done();
			
			final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(template1).when(TestBean::bool_field, true);
			
			final SingleQueryComponent<TestBean> query_comp_1b = CrudUtils.allOf(TestBean.class)
					.when("bool_field", true)
					.when("string_field", "string_field");
			
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_1 = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_1);
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_1b = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_1b);
			
			final XContentBuilder expected_1 = XContentFactory.jsonBuilder().startObject()
													.startObject("and").startArray("filters")
														.startObject()
															.startObject("term")
																.field("bool_field", true)
															.endObject()
														.endObject()
														.startObject()
															.startObject("term")
																.field("string_field", "string_field")
															.endObject()
														.endObject()
													.endArray().endObject()
												.endObject();
			
			assertEquals(sortOutQuotesAndStuff(expected_1.string()), toXContentThenString(query_meta_1._1()));
			assertEquals(sortOutQuotesAndStuff(expected_1.string()), toXContentThenString(query_meta_1b._1()));
			final MockSearchRequestBuilder msrb = new MockSearchRequestBuilder();
			query_meta_1._2().apply(msrb.srb);
			assertEquals(0, msrb.size_arg.getAllValues().size());
			assertEquals(0, msrb.sort.size());
		}
		
		// Includes extra + all the checks except the range checks
		
		{
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
			
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_2 = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_2);
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_2b = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_2b);
			
			final XContentBuilder expected_2 = XContentFactory.jsonBuilder().startObject()
													.startObject("or").startArray("filters")
														.startObject()
															.startObject("term")
																.field("string_field", "string_field")
															.endObject()
														.endObject()
														.startObject()
															.startObject("terms")
																.array("string_field", "test1a", "test1b")
																.field("execution", "or")
															.endObject()
														.endObject()
														.startObject()
															.startObject("exists")
																.field("field", "bool_field")
															.endObject()
														.endObject()
														.startObject()
															.startObject("not").startObject("filter")
																.startObject("exists")
																	.field("field", "long_field")
																.endObject()
															.endObject().endObject()
														.endObject()
														.startObject()
															.startObject("terms")
																.array("long_field", 10, 11, 12)
																.field("execution", "and")
															.endObject()
														.endObject()
														.startObject()
															.startObject("not").startObject("filter")
																.startObject("term")
																	.field("long_field", 13)
																.endObject()
															.endObject().endObject()
														.endObject()
														
													.endArray().endObject()
												.endObject();
			
			assertEquals(sortOutQuotesAndStuff(expected_2.string()), toXContentThenString(query_meta_2._1()));
			assertEquals(sortOutQuotesAndStuff(expected_2.string()), toXContentThenString(query_meta_2b._1()));
			final MockSearchRequestBuilder msrb1 = new MockSearchRequestBuilder();
			query_meta_2._2().apply(msrb1.srb);
			assertEquals(100, (int)msrb1.size_arg.getValue());
			assertEquals(0, msrb1.sort.size());
			final MockSearchRequestBuilder msrb2 = new MockSearchRequestBuilder();
			query_meta_2._2().apply(msrb2.srb);
			assertEquals(100, (int)msrb2.size_arg.getValue());
			assertEquals(0, msrb2.sort.size());
		}
	}
	
	@Test
	public void test_AllTheRangeQueries() throws IOException {
		
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
		
		final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_1 = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_1);
		final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_1b = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_1b);

		final XContentBuilder expected_1 = 
				XContentFactory.jsonBuilder().startObject()
					.startObject("and").startArray("filters")
						.startObject()
							.startObject("range")
								.startObject("string_field")
									.field("from", "bbb")
									.field("to", (String)null)
									.field("include_lower", false)
									.field("include_upper", true)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("string_field")
									.field("from", (String)null)
									.field("to", "fff")
									.field("include_lower", true)
									.field("include_upper", true)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("string_field")
									.field("from", "ccc")
									.field("to", "ddd")
									.field("include_lower", true)
									.field("include_upper", false)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("string_field")
									.field("from", "xxx")
									.field("to", "yyy")
									.field("include_lower", true)
									.field("include_upper", true)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("long_field")
									.field("from", 1000)
									.field("to", (String)null)
									.field("include_lower", true)
									.field("include_upper", true)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("long_field")
									.field("from", (String)null)
									.field("to", 10000)
									.field("include_lower", true)
									.field("include_upper", false)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("long_field")
									.field("from", 2000)
									.field("to", 20000)
									.field("include_lower", false)
									.field("include_upper", false)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("long_field")
									.field("from", 3000)
									.field("to", 30000)
									.field("include_lower", false)
									.field("include_upper", true)
								.endObject()
							.endObject()
						.endObject()
					.endArray().endObject()
				.endObject();

		final MockSearchRequestBuilder msrb1 = new MockSearchRequestBuilder();
		query_meta_1._2().apply(msrb1.srb);
		final MockSearchRequestBuilder msrb1b = new MockSearchRequestBuilder();
		query_meta_1b._2().apply(msrb1b.srb);
		
		assertEquals(sortOutQuotesAndStuff(expected_1.string()), toXContentThenString(query_meta_1._1()));
		assertEquals(sortOutQuotesAndStuff(expected_1.string()), toXContentThenString(query_meta_1b._1()));
		
		assertEquals(Arrays.asList(Tuples._2T("test_field_1", SortOrder.ASC), Tuples._2T("test_field_2", SortOrder.DESC)), msrb1.sort);
		assertEquals(Arrays.asList(Tuples._2T("test_field_1", SortOrder.ASC), Tuples._2T("test_field_2", SortOrder.DESC)), msrb1b.sort);
		assertEquals(1, (int)msrb1.size_arg.getAllValues().size());
		assertEquals(1, (int)msrb1b.size_arg.getAllValues().size());
		assertEquals(200, (int)msrb1.size_arg.getValue());
		assertEquals(200, (int)msrb1b.size_arg.getValue());		
	}

	@Test 
	public void test_NestedQueries() throws IOException {
		
		// 1 level of nesting
		{
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
															
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_1 = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_1);			
					
			
			final XContentBuilder expected_1 = 
					XContentFactory.jsonBuilder().startObject()
					.startObject("and").startArray("filters")
						.startObject()
							.startObject("term")
								.field("string_field", "a")
							.endObject()
						.endObject()
						.startObject()
							.startObject("term")
								.field("nested_list.nested_string_field", "x")
							.endObject()
						.endObject()
						.startObject()
							.startObject("terms")
								.array("nested_list.nested_string_field", "x", "y")
								.field("execution", "or")
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("nested_list.nested_string_field")
									.field("from", "ccc")
									.field("to", "ddd")
									.field("include_lower", true)
									.field("include_upper", false)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("exists")
								.field("field", "long_field")
							.endObject()
						.endObject()
					.endArray().endObject()
					.endObject();
		
			final MockSearchRequestBuilder msrb1 = new MockSearchRequestBuilder();
			query_meta_1._2().apply(msrb1.srb);
			
			assertEquals(sortOutQuotesAndStuff(expected_1.string()), toXContentThenString(query_meta_1._1()));
			assertEquals(Arrays.asList(Tuples._2T("test_field_2", SortOrder.DESC)), msrb1.sort);
			assertEquals(1, (int)msrb1.size_arg.getAllValues().size());
			assertEquals(5, (int)msrb1.size_arg.getValue());
		}			
		// 2 levels of nesting
		{
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
					
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_2 = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_2);

			final XContentBuilder expected_2 = 
					XContentFactory.jsonBuilder().startObject()
					.startObject("and").startArray("filters")
						.startObject()
							.startObject("term")
								.field("string_field", "a")
							.endObject()
						.endObject()
						.startObject()
							.startObject("term")
								.field("nested_list.nested_string_field", "x")
							.endObject()
						.endObject()
						.startObject()
							.startObject("term")
								.field("nested_list.nested_string_field", "y")
							.endObject()
						.endObject()
						.startObject()
							.startObject("terms")
								.array("nested_list.nested_string_field", "x", "y")
								.field("execution", "or")
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("nested_list.nested_string_field")
									.field("from", "ccc")
									.field("to", "ddd")
									.field("include_lower", true)
									.field("include_upper", false)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("term")
								.field("nested_list.nested_object.nested_nested_string_field", "z")
							.endObject()
						.endObject()
						.startObject()
							.startObject("not").startObject("filter")
								.startObject("exists")
									.field("field", "nested_list.nested_object.nested_nested_string_field")
								.endObject()
							.endObject().endObject()
						.endObject()
						.startObject()
							.startObject("exists")
								.field("field", "long_field")
							.endObject()
						.endObject()
					.endArray().endObject()
					.endObject();
		
			final MockSearchRequestBuilder msrb1 = new MockSearchRequestBuilder();
			query_meta_2._2().apply(msrb1.srb);
									
			assertEquals(sortOutQuotesAndStuff(expected_2.string()), toXContentThenString(query_meta_2._1()));
			assertEquals(Arrays.asList(), msrb1.sort);
			assertEquals(0, (int)msrb1.size_arg.getAllValues().size());			
		}
	}
	
	@Test
	public void test_handleIdAndTypeDifferently() throws IOException {
		
		// id - single value

		{
			BeanTemplate<TestBean> template1 = BeanTemplateUtils.build(TestBean.class).with(TestBean::_id, "id_field").done();
			
			final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(template1).when(TestBean::bool_field, true);
			
			assertTrue("No id ranges", ElasticsearchUtils.queryContainsIdRanges(query_comp_1));
			
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_1 = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_1);
			
			final XContentBuilder expected_1 = XContentFactory.jsonBuilder().startObject()
													.startObject("and").startArray("filters")
														.startObject()
															.startObject("term")
																.field("bool_field", true)
															.endObject()
														.endObject()
														.startObject()
															.startObject("ids")
																.array("types")
																.array("values", "id_field")
															.endObject()
														.endObject()
													.endArray().endObject()
												.endObject();
			
			assertEquals(sortOutQuotesAndStuff(expected_1.string()), toXContentThenString(query_meta_1._1()));
		}

		// Error case 1: exists on "ids"
		
		{
			final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(TestBean.class)
																	.when(TestBean::bool_field, true)
																	.withPresent(TestBean::_id);
		
			try {
				ElasticsearchUtils.queryContainsIdRanges(query_comp_1);
				fail("Should have thrown exception here");
			}
			catch (RuntimeException e) {
				assertEquals(ErrorUtils.EXISTS_ON_IDS, e.getMessage());
			}			
		}		
		
		// Error case 1b: exists on "_type"
		
		{
			final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(TestBean.class)
																	.when(TestBean::bool_field, true)
																	.withPresent("_type");
		
			try {
				ElasticsearchUtils.queryContainsIdRanges(query_comp_1);
				fail("Should have thrown exception here");
			}
			catch (RuntimeException e) {
				assertEquals(ErrorUtils.EXISTS_ON_TYPES, e.getMessage());
			}			
		}		
		
		// id - single value. not

		{
			final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(TestBean.class)
					.when(TestBean::bool_field, true)
					.whenNot(TestBean::_id, "not_id");
			
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_1 = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_1);
			
			final XContentBuilder expected_1 = XContentFactory.jsonBuilder().startObject()
													.startObject("and").startArray("filters")
														.startObject()
															.startObject("term")
																.field("bool_field", true)
															.endObject()
														.endObject()
														.startObject()
															.startObject("not").startObject("filter")
																.startObject("ids")
																	.array("types")
																	.array("values", "not_id")
																	.endObject()
															.endObject().endObject()
														.endObject()
													.endArray().endObject()
												.endObject();
			
			assertEquals(sortOutQuotesAndStuff(expected_1.string()), toXContentThenString(query_meta_1._1()));
		}
		
		// type - single value. not

		{
			final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(TestBean.class)
					.when(TestBean::bool_field, true)
					.whenNot("_type", "not_id");
			
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_1 = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_1);
			
			final XContentBuilder expected_1 = XContentFactory.jsonBuilder().startObject()
													.startObject("and").startArray("filters")
														.startObject()
															.startObject("term")
																.field("bool_field", true)
															.endObject()
														.endObject()
														.startObject()
															.startObject("not").startObject("filter")
																.startObject("type")
																	.field("value", "not_id")
																	.endObject()
															.endObject().endObject()
														.endObject()
													.endArray().endObject()
												.endObject();
			
			assertEquals(sortOutQuotesAndStuff(expected_1.string()), toXContentThenString(query_meta_1._1()));
		}

		// type - single value. set

		{
			final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(TestBean.class)
					.when(TestBean::bool_field, true)
					.when("_type", "id");
			
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_1 = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_1);
			
			final XContentBuilder expected_1 = XContentFactory.jsonBuilder().startObject()
													.startObject("and").startArray("filters")
														.startObject()
															.startObject("term")
																.field("bool_field", true)
															.endObject()
														.endObject()
														.startObject()
															.startObject("type")
																.field("value", "id")
																.endObject()
														.endObject()
													.endArray().endObject()
												.endObject();
			
			assertEquals(sortOutQuotesAndStuff(expected_1.string()), toXContentThenString(query_meta_1._1()));
		}
		
		// id - multi value

		{
			final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(TestBean.class)
																	.when(TestBean::bool_field, true)
																	.withAny("_id", Arrays.asList("id1", "id2", "id3"));
			
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_1 = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_1);
			
			final XContentBuilder expected_1 = XContentFactory.jsonBuilder().startObject()
													.startObject("and").startArray("filters")
														.startObject()
															.startObject("term")
																.field("bool_field", true)
															.endObject()
														.endObject()
														.startObject()
															.startObject("ids")
																.array("types")
																.array("values", "id1", "id2", "id3")
															.endObject()
														.endObject()
													.endArray().endObject()
												.endObject();
			
			assertEquals(sortOutQuotesAndStuff(expected_1.string()), toXContentThenString(query_meta_1._1()));
		}

		// type - multi value, any_of

		{
			final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(TestBean.class)
																	.when(TestBean::bool_field, true)
																	.withAny("_type", Arrays.asList("id1", "id2", "id3"));
			
			try {
				ElasticsearchUtils.queryContainsIdRanges(query_comp_1);
				fail("Should have thrown exception here");
			}
			catch (RuntimeException e) {
				assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "any_of/_type"), e.getMessage());
			}			
		}
		
		// type - multi value, all_of

		{
			final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.anyOf(TestBean.class)
																	.when(TestBean::bool_field, true)
																	.withAll("_type", Arrays.asList("id1", "id2", "id3"));
			
			try {
				ElasticsearchUtils.queryContainsIdRanges(query_comp_1);
				fail("Should have thrown exception here");
			}
			catch (RuntimeException e) {
				assertEquals(ErrorUtils.ALL_OF_ON_TYPES, e.getMessage());
			}			
		}
		
		// Error case 1: all of "ids"
		
		{
			final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(TestBean.class)
																	.when(TestBean::bool_field, true)
																	.withAll("_id", Arrays.asList("id1", "id2", "id3"));
		
			try {
				ElasticsearchUtils.queryContainsIdRanges(query_comp_1);
				fail("Should have thrown exception here");
			}
			catch (RuntimeException e) {
				assertEquals(ErrorUtils.ALL_OF_ON_IDS, e.getMessage());
			}			
		}		
		
		// Error case 2: id range check fails

		{
			final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(TestBean.class)
																	.when(TestBean::bool_field, true)
																	.rangeAbove(TestBean::_id, "lower_id", true);
		
			assertTrue("Range check should fail", ElasticsearchUtils.queryContainsIdRanges(query_comp_1));
			
			try {
				ElasticsearchUtils.convertToElasticsearchFilter(query_comp_1, false);
				fail("Should have thrown exception here");
			}
			catch (RuntimeException e) {
				assertEquals(ErrorUtils.NO_ID_RANGES_UNLESS_IDS_INDEXED, e.getMessage());
			}			
			
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_1 = ElasticsearchUtils.convertToElasticsearchFilter(query_comp_1, true);
			
			final XContentBuilder expected_1 = XContentFactory.jsonBuilder().startObject()
													.startObject("and").startArray("filters")
														.startObject()
															.startObject("term")
																.field("bool_field", true)
															.endObject()
														.endObject()
														.startObject()
														.startObject("range")
															.startObject("_id")
																	.field("from", "lower_id")
																	.field("to", (String) null)
																	.field("include_lower", false)
																	.field("include_upper", true)
																.endObject()
															.endObject()
														.endObject()
													.endArray().endObject()
												.endObject();
			
			assertEquals(sortOutQuotesAndStuff(expected_1.string()), toXContentThenString(query_meta_1._1()));
			
		}		
		
		// Error case 2 - "_type" range fails
		
		{
			final SingleQueryComponent<TestBean> query_comp_1 = CrudUtils.allOf(TestBean.class)
					.when(TestBean::bool_field, true)
					.rangeAbove("_type", "lower_id", true);
		
			try {
				ElasticsearchUtils.queryContainsIdRanges(query_comp_1);
				fail("Should have thrown exception here");
			}
			catch (RuntimeException e) {
				assertEquals(ErrorUtils.RANGES_ON_TYPES, e.getMessage());
			}			
		}		
		
		
	}
		
	@Test
	public void test_MultipleQueries() throws IOException {

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
		
		Function<XContentBuilder, XContentBuilder> xcbuilder = Lambdas.wrap_u(xcb ->
				xcb.startObject()
					.startObject("and").startArray("filters")
						.startObject()
							.startObject("range")
								.startObject("string_field")
									.field("from", "bbb")
									.field("to", (String)null)
									.field("include_lower", false)
									.field("include_upper", true)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("string_field")
									.field("from", (String)null)
									.field("to", "fff")
									.field("include_lower", true)
									.field("include_upper", true)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("string_field")
									.field("from", "ccc")
									.field("to", "ddd")
									.field("include_lower", true)
									.field("include_upper", false)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("string_field")
									.field("from", "xxx")
									.field("to", "yyy")
									.field("include_lower", true)
									.field("include_upper", true)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("long_field")
									.field("from", 1000)
									.field("to", (String)null)
									.field("include_lower", true)
									.field("include_upper", true)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("long_field")
									.field("from", (String)null)
									.field("to", 10000)
									.field("include_lower", true)
									.field("include_upper", false)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("long_field")
									.field("from", 2000)
									.field("to", 20000)
									.field("include_lower", false)
									.field("include_upper", false)
								.endObject()
							.endObject()
						.endObject()
						.startObject()
							.startObject("range")
								.startObject("long_field")
									.field("from", 3000)
									.field("to", 30000)
									.field("include_lower", false)
									.field("include_upper", true)
								.endObject()
							.endObject()
						.endObject()
					.endArray().endObject()
				.endObject());
		
		{
			final MultiQueryComponent<TestBean> multi_query_1 = CrudUtils.<TestBean>allOf(query_comp_1).orderBy(Tuples._2T("test_field_2", -1)).limit(5);
			final MultiQueryComponent<TestBean> multi_query_2 = CrudUtils.<TestBean>anyOf(query_comp_1);
					
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_1 = ElasticsearchUtils.convertToElasticsearchFilter(multi_query_1);
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_2 = ElasticsearchUtils.convertToElasticsearchFilter(multi_query_2);
	
			final MockSearchRequestBuilder msrb1 = new MockSearchRequestBuilder();
			query_meta_1._2().apply(msrb1.srb);
			final MockSearchRequestBuilder msrb2 = new MockSearchRequestBuilder();
			query_meta_2._2().apply(msrb2.srb);
	
			final XContentBuilder multi_expected_1 = xcbuilder.apply(
					XContentFactory.jsonBuilder().startObject()
						.startObject("and").startArray("filters")
						)
						.endArray().endObject()
					.endObject();
	
			final XContentBuilder multi_expected_2 = xcbuilder.apply(
					XContentFactory.jsonBuilder().startObject()
						.startObject("or").startArray("filters")
						)
						.endArray().endObject()
					.endObject();
			
			assertEquals(sortOutQuotesAndStuff(multi_expected_1.string()), toXContentThenString(query_meta_1._1()));
			assertEquals(sortOutQuotesAndStuff(multi_expected_2.string()), toXContentThenString(query_meta_2._1()));
			
			assertEquals(Arrays.asList(Tuples._2T("test_field_2", SortOrder.DESC)), msrb1.sort);
			assertEquals(1, (int)msrb1.size_arg.getAllValues().size());
			assertEquals(5, (int)msrb1.size_arg.getValue());
			
			assertEquals(Arrays.asList(), msrb2.sort);
			assertEquals(0, (int)msrb2.size_arg.getAllValues().size());
		}
		// Multiple nested
		{
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

			Function<XContentBuilder, XContentBuilder> xcbuilder_2 = Lambdas.wrap_u(xcb ->
				xcb.startObject()
					.startObject("and").startArray("filters")
					.startObject()
						.startObject("term")
							.field("string_field", "a")
						.endObject()
					.endObject()
					.startObject()
						.startObject("term")
							.field("nested_list.nested_string_field", "x")
						.endObject()
					.endObject()
					.startObject()
						.startObject("terms")
							.array("nested_list.nested_string_field", "x", "y")
							.field("execution", "or")
						.endObject()
					.endObject()
					.startObject()
						.startObject("range")
							.startObject("nested_list.nested_string_field")
								.field("from", "ccc")
								.field("to", "ddd")
								.field("include_lower", true)
								.field("include_upper", false)
							.endObject()
						.endObject()
					.endObject()
					.startObject()
						.startObject("term")
							.field("nested_list.nested_object.nested_nested_string_field", "z")
						.endObject()
					.endObject()
					.startObject()
						.startObject("not").startObject("filter")
							.startObject("exists")
								.field("field", "nested_list.nested_object.nested_nested_string_field")
							.endObject()
						.endObject().endObject()
					.endObject()
					.startObject()
						.startObject("exists")
							.field("field", "long_field")
						.endObject()
					.endObject()
				.endArray().endObject().endObject()
				);
								
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_3 = ElasticsearchUtils.convertToElasticsearchFilter(multi_query_3);
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_4 = ElasticsearchUtils.convertToElasticsearchFilter(multi_query_4);
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_5 = ElasticsearchUtils.convertToElasticsearchFilter(multi_query_5);
			final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query_meta_6 = ElasticsearchUtils.convertToElasticsearchFilter(multi_query_6);

			final XContentBuilder multi_expected_3 =
				Lambdas.wrap_u(__ -> 
					XContentFactory.jsonBuilder().startObject().startObject("and").startArray("filters"))
							.andThen(xcbuilder)
							.andThen(xcbuilder_2)
						.apply(null)
						.endArray().endObject()
					.endObject();
			
			final XContentBuilder multi_expected_4 =
					Lambdas.wrap_u(__ -> 
						XContentFactory.jsonBuilder().startObject().startObject("or").startArray("filters"))
								.andThen(xcbuilder)
								.andThen(xcbuilder_2)
							.apply(null)
							.endArray().endObject()
						.endObject();
			
			assertEquals(sortOutQuotesAndStuff(multi_expected_3.string()), toXContentThenString(query_meta_3._1()));
			assertEquals(sortOutQuotesAndStuff(multi_expected_4.string()), toXContentThenString(query_meta_4._1()));
			assertEquals(sortOutQuotesAndStuff(multi_expected_3.string()), toXContentThenString(query_meta_3._1()));
			assertEquals(sortOutQuotesAndStuff(multi_expected_4.string()), toXContentThenString(query_meta_4._1()));

			final MockSearchRequestBuilder msrb3 = new MockSearchRequestBuilder();
			query_meta_3._2().apply(msrb3.srb);
			final MockSearchRequestBuilder msrb4 = new MockSearchRequestBuilder();
			query_meta_4._2().apply(msrb4.srb);
			final MockSearchRequestBuilder msrb5 = new MockSearchRequestBuilder();
			query_meta_5._2().apply(msrb5.srb);
			final MockSearchRequestBuilder msrb6 = new MockSearchRequestBuilder();
			query_meta_6._2().apply(msrb6.srb);
			
			
			assertEquals(Arrays.asList(), msrb3.sort);
			assertEquals(1, (int)msrb3.size_arg.getAllValues().size());
			assertEquals(5, (int)msrb3.size_arg.getValue());

			assertEquals(Arrays.asList(Tuples._2T("test_field_2", SortOrder.DESC)), msrb4.sort);
			assertEquals(0, (int)msrb4.size_arg.getAllValues().size());
			
			assertEquals(Arrays.asList(), msrb5.sort);
			assertEquals(1, (int)msrb5.size_arg.getAllValues().size());
			assertEquals(5, (int)msrb5.size_arg.getValue());
			
			assertEquals(Arrays.asList(Tuples._2T("test_field_2", SortOrder.DESC)), msrb6.sort);
			assertEquals(0, (int)msrb6.size_arg.getAllValues().size());
		}
	}
	
	@Test
	public void test_NestedMultiQuery() throws IOException {
		
		final BeanTemplate<TestBean> template1a = BeanTemplateUtils.build(TestBean.class).with(TestBean::string_field, "string_field").done();
		final SingleQueryComponent<TestBean> query_comp_1a = CrudUtils.anyOf(template1a);		
		final SingleQueryComponent<TestBean> query_comp_2a = CrudUtils.allOf(TestBean.class).when(TestBean::bool_field, true);	
		final MultiQueryComponent<TestBean> multi_query_1 = CrudUtils.allOf(Arrays.asList(query_comp_1a, query_comp_2a));

		final BeanTemplate<TestBean> template1b = BeanTemplateUtils.build(TestBean.class).with(TestBean::string_field, "string_field_b").done();
		final SingleQueryComponent<TestBean> query_comp_1b = CrudUtils.allOf(template1b);		
		final SingleQueryComponent<TestBean> query_comp_2b = CrudUtils.anyOf(TestBean.class).when(TestBean::bool_field, false);		
		final MultiQueryComponent<TestBean> multi_query_2 = CrudUtils.allOf(query_comp_1b, query_comp_2b);
		
		final MultiQueryComponent<TestBean> multi_query_test_1 = CrudUtils.anyOf(multi_query_1, multi_query_2);
		final MultiQueryComponent<TestBean> multi_query_test_2 = CrudUtils.allOf(multi_query_1, query_comp_1b);
		
		final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> multi_query_meta_1 = ElasticsearchUtils.convertToElasticsearchFilter(multi_query_test_1);
		final Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> multi_query_meta_2 = ElasticsearchUtils.convertToElasticsearchFilter(multi_query_test_2);
		
		final XContentBuilder expected_1 = XContentFactory.jsonBuilder().startObject()
				.startObject("or").startArray("filters")
					.startObject()				
						.startObject("and").startArray("filters")
							.startObject()
								.startObject("or").startArray("filters")
									.startObject()				
										.startObject("term")
											.field("string_field", "string_field")
										.endObject()
									.endObject()
								.endArray().endObject()
							.endObject()
							.startObject()
								.startObject("and").startArray("filters")
									.startObject()
										.startObject("term")
											.field("bool_field", true)
										.endObject()
									.endObject()
								.endArray().endObject()
							.endObject()
						.endArray().endObject()
					.endObject()
					
					.startObject()				
						.startObject("and").startArray("filters")
							.startObject()
								.startObject("and").startArray("filters")
									.startObject()
										.startObject("term")
											.field("string_field", "string_field_b")
										.endObject()
									.endObject()
								.endArray().endObject()
							.endObject()
							.startObject()
								.startObject("or").startArray("filters")
									.startObject()
										.startObject("term")
											.field("bool_field", false)
										.endObject()
									.endObject()
								.endArray().endObject()
							.endObject()
						.endArray().endObject()
					.endObject()
					
				.endArray().endObject()
			.endObject();
		
		final XContentBuilder expected_2 = XContentFactory.jsonBuilder().startObject()
				.startObject("and").startArray("filters")
					.startObject()				
						.startObject("and").startArray("filters")
							.startObject()
								.startObject("or").startArray("filters")
									.startObject()				
										.startObject("term")
											.field("string_field", "string_field")
										.endObject()
									.endObject()
								.endArray().endObject()
							.endObject()
							.startObject()
								.startObject("and").startArray("filters")
									.startObject()
										.startObject("term")
											.field("bool_field", true)
										.endObject()
									.endObject()
								.endArray().endObject()
							.endObject()
						.endArray().endObject()
					.endObject()
					
					.startObject()				
						.startObject("and").startArray("filters")
							.startObject()
								.startObject("term")
									.field("string_field", "string_field_b")
								.endObject()
							.endObject()
						.endArray().endObject()
					.endObject()
					
				.endArray().endObject()
			.endObject();
		
		assertEquals(sortOutQuotesAndStuff(expected_1.string()), toXContentThenString(multi_query_meta_1._1()));
		assertEquals(sortOutQuotesAndStuff(expected_2.string()), toXContentThenString(multi_query_meta_2._1()));

	}
	
	
}
