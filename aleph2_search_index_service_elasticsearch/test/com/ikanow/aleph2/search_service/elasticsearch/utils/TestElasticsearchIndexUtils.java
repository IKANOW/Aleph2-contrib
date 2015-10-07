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
package com.ikanow.aleph2.search_service.elasticsearch.utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean.CollidePolicy;
import com.typesafe.config.ConfigFactory;

import fj.data.Either;

public class TestElasticsearchIndexUtils {

	static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	ElasticsearchIndexServiceConfigBean _config ;
	
	@Before
	public void getConfig() throws JsonProcessingException, IOException {
		
		_config = ElasticsearchIndexConfigUtils.buildConfigBean(ConfigFactory.empty());
		
		_config.columnar_technology_override().enabled_field_data_analyzed().put("test_type_123", ImmutableMap.<String, Object>builder().put("format", "test1").build());
		_config.columnar_technology_override().enabled_field_data_notanalyzed().put("test_type_123", ImmutableMap.<String, Object>builder().put("format", "test2").build());
	}
	
	@Test
	public void test_baseNames() {
		
		// Index stuff
		{
			final String base_index = ElasticsearchIndexUtils.getBaseIndexName(BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test+1-1").done().get(), Optional.empty());
			
			assertEquals("test_1_1__514e7056b0d8", base_index);
			
			final String base_index2 = ElasticsearchIndexUtils.getBaseIndexName(BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test+1-1/another__test").done().get(), Optional.empty());
			
			assertEquals("test_1_1_another_test__f73d191c0424", base_index2);
			
			final String base_index3 = ElasticsearchIndexUtils.getBaseIndexName(BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test+1-1/another__test/VERY/long/string").done().get(), Optional.empty());
			
			assertEquals("test_1_1_long_string__2711e659d5a6", base_index3);
		}
		
		// More complex index case: override set:
		{
			final DataBucketBean test_index_override = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema,
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
											.with("technology_override_schema",
													ImmutableMap.builder().put("index_name_override", "test_index_override").build()
													)
								.done().get())
							.done().get())
					.done().get();

			final String base_index = ElasticsearchIndexUtils.getBaseIndexName(test_index_override, Optional.empty());
			
			assertEquals("test_index_override", base_index);
		}
		
		// Type stuff
		{
			// Tests:
			// 0a) no data schema 0b) no search index schema, 0c) no settings, 0d) disabled search index schema
			// 1a) collide_policy==error, type_name_or_prefix not set
			// 1b) collide_policy==error, type_name_or_prefix set
			// 2a) collide_policy==new_type, type_name_or_prefix not set
			// 2b) collide_policy==new_type, type_name_or_prefix set

			final DataBucketBean test_bucket_0a = BeanTemplateUtils.build(DataBucketBean.class).done().get();
			final DataBucketBean test_bucket_0b = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class).done().get()).done().get();
			final DataBucketBean test_bucket_0c = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema,
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
								.done().get())
							.done().get())
					.done().get();
			final DataBucketBean test_bucket_0d = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema,
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
											.with("enabled", false)
											.with("technology_override_schema",
													ImmutableMap.builder().put("collide_policy", "error").build()
													)
								.done().get())
							.done().get())
					.done().get();

			final DataBucketBean test_bucket_1a = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema,
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
											.with("technology_override_schema",
													ImmutableMap.builder()
														.put("collide_policy", "error")
													.build()
													)
										.done().get()
								)
							.done().get()
							)
					.done().get();						
			final DataBucketBean test_bucket_1b = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema,
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
											.with("enabled", true)
											.with("technology_override_schema",
													ImmutableMap.builder()
														.put("collide_policy", "error")
														.put("type_name_or_prefix", "test1")
													.build()
													)
										.done().get()
								)
							.done().get()
							)
					.done().get();			

			final DataBucketBean test_bucket_2a = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema,
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
											.with("technology_override_schema",
													ImmutableMap.builder()
														.put("collide_policy", "new_type")
													.build()
													)
										.done().get()
								)
							.done().get()
							)
					.done().get();						
			final DataBucketBean test_bucket_2b = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema,
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
											.with("enabled", true)
											.with("technology_override_schema",
													ImmutableMap.builder()
														.put("collide_policy", "new_type")
														.put("type_name_or_prefix", "test2")
													.build()
													)
										.done().get()
								)
							.done().get()
							)
					.done().get();			
			
			assertEquals("_default_", ElasticsearchIndexUtils.getTypeKey(test_bucket_0a, _mapper));
			assertEquals("_default_", ElasticsearchIndexUtils.getTypeKey(test_bucket_0b, _mapper));
			assertEquals("_default_", ElasticsearchIndexUtils.getTypeKey(test_bucket_0c, _mapper));
			assertEquals("_default_", ElasticsearchIndexUtils.getTypeKey(test_bucket_0d, _mapper));
			assertEquals("data_object", ElasticsearchIndexUtils.getTypeKey(test_bucket_1a, _mapper));
			assertEquals("test1", ElasticsearchIndexUtils.getTypeKey(test_bucket_1b, _mapper));
			assertEquals("_default_", ElasticsearchIndexUtils.getTypeKey(test_bucket_2a, _mapper));
			assertEquals("_default_", ElasticsearchIndexUtils.getTypeKey(test_bucket_2b, _mapper));
		}
	}

	@Test
	public void test_parseDefaultMapping() throws JsonProcessingException, IOException {

		// Check the different components
		
		// Build/"unbuild" match pair
		
		assertEquals(Tuples._2T("*", "*"), ElasticsearchIndexUtils.buildMatchPair(_mapper.readTree("{}")));
		
		assertEquals(Tuples._2T("field*", "*"), ElasticsearchIndexUtils.buildMatchPair(_mapper.readTree("{\"match\":\"field*\"}")));
		
		assertEquals(Tuples._2T("field*field", "type*"), ElasticsearchIndexUtils.buildMatchPair(_mapper.readTree("{\"match\":\"field*field\", \"match_mapping_type\": \"type*\"}")));
		
		assertEquals("testBARSTAR_string", ElasticsearchIndexUtils.getFieldNameFromMatchPair(Tuples._2T("test_*", "string")));
		
		// More complex objects
		
		final String properties = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/properties_test.json"), Charsets.UTF_8);
		final String templates = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/templates_test.json"), Charsets.UTF_8);
		final String both = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/full_mapping_test.json"), Charsets.UTF_8);
		
		final JsonNode properties_json = _mapper.readTree(properties);
		final JsonNode templates_json = _mapper.readTree(templates);
		final JsonNode both_json = _mapper.readTree(both);
		
		// Properties, empty + non-empty
		
		final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> props_test1 = ElasticsearchIndexUtils.getProperties(templates_json);
		assertTrue("Empty map if not present", props_test1.isEmpty());

		final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> props_test2 = ElasticsearchIndexUtils.getProperties(properties_json);
		assertEquals(4, props_test2.size());
		assertEquals(Arrays.asList("@version", "@timestamp", "sourceKey", "geoip"), 
				props_test2.keySet().stream().map(e -> e.left().value()).collect(Collectors.toList()));
		
		assertEquals("{\"type\":\"string\",\"index\":\"not_analyzed\"}", 
						props_test2.get(Either.left("sourceKey")).toString());
		
		// Templates, empty + non-empty

		final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> templates_test1 = ElasticsearchIndexUtils.getTemplates(properties_json);
		assertTrue("Empty map if not present", templates_test1.isEmpty());
		
		final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> templates_test2 = ElasticsearchIndexUtils.getTemplates(templates_json);
		assertEquals(2, templates_test2.size());
		assertEquals(Arrays.asList(
					Tuples._2T("*", "string"),
					Tuples._2T("*", "number")
				), 
				templates_test2.keySet().stream().map(e -> e.right().value()).collect(Collectors.toList()));
		
		// Some more properties test
		
		final List<String> nested_properties = ElasticsearchIndexUtils.getAllFixedFields_internal(properties_json).collect(Collectors.toList());
		assertEquals(Arrays.asList("@version", "@timestamp", "sourceKey", "geoip", "geoip.location"), nested_properties);

		final Set<String> nested_properties_2 = ElasticsearchIndexUtils.getAllFixedFields(both_json);
		assertEquals(Arrays.asList("sourceKey", "@timestamp", "geoip", "geoip.location", "@version"), new ArrayList<String>(nested_properties_2));		
		
		// Putting it all together...
		
		final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> 
			total_result1 = ElasticsearchIndexUtils.parseDefaultMapping(both_json, Optional.of("type_test"));
		
		assertEquals(4, total_result1.size());
		assertEquals("{\"match\":\"test*\",\"match_mapping_type\":\"number\",\"mapping\":{\"type\":\"number\",\"index\":\"analyzed\"}}", total_result1.get(Either.right(Tuples._2T("test*", "number"))).toString());
		assertEquals("{\"type\":\"date\"}", total_result1.get(Either.left("@timestamp1")).toString());
		
		final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> 
		total_result2 = ElasticsearchIndexUtils.parseDefaultMapping(both_json, Optional.empty());
	
		assertEquals(7, total_result2.size());
		assertEquals(true, total_result2.get(Either.right(Tuples._2T("*", "string"))).get("mapping").get("omit_norms").asBoolean());
		assertEquals("{\"type\":\"date\",\"fielddata\":{}}", total_result2.get(Either.left("@timestamp")).toString());	
		
		// A couple of error checks:
		// - Missing mapping
		// - Mapping not an object
	}

	private String strip(final String s) { return s.replace("\"", "'").replace("\r", " ").replace("\n", " "); }
	
	@Test
	public void test_columnarMapping_standalone() throws JsonProcessingException, IOException {
		final String both = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/full_mapping_test.json"), Charsets.UTF_8);
		final JsonNode both_json = _mapper.readTree(both);		
		
		final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> field_lookups = ElasticsearchIndexUtils.parseDefaultMapping(both_json, Optional.empty());

		//DEBUG
//		System.out.println("(Field lookups = " + field_lookups + ")");
//		System.out.println("(Analyzed default = " + _config.columnar_technology_override().default_field_data_analyzed() + ")");
//		System.out.println("(NotAnalyzed default = " + _config.columnar_technology_override().default_field_data_notanalyzed() + ")");
		
		// 1) Mappings - field name specified (include)
		{
			final Stream<String> test_stream1 = Stream.of("@version", "field_not_present", "@timestamp");
			
			final Stream<Tuple2<Either<String, Tuple2<String, String>>, JsonNode>> test_stream_result_1 =
					ElasticsearchIndexUtils.createFieldIncludeLookups(test_stream1, fn -> Either.left(fn), field_lookups, 
								_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_notanalyzed(), JsonNode.class),
								_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_analyzed(), JsonNode.class),
								false,
							_mapper, "_default_");
	
			final Map<Either<String, Tuple2<String, String>>, JsonNode> test_map_result_1 = 		
					test_stream_result_1.collect(Collectors.toMap(
												t2 -> t2._1(),
												t2 -> t2._2()
												));
			final String test_map_expected_1 = "{Left(@timestamp)={'type':'date','fielddata':{}}, Right((field_not_present,*))={'mapping':{'index':'not_analyzed','type':'{dynamic_type}','fielddata':{'format':'doc_values'}},'path_match':'field_not_present','match_mapping_type':'*'}, Left(@version)={'type':'string','index':'analyzed','fielddata':{'format':'fst'}}}";
			assertEquals(test_map_expected_1, strip(test_map_result_1.toString()));
			
			//DEBUG
			//System.out.println("(Field column lookups = " + test_map_result_1 + ")");
		}		
		
		// 2) Mappings - field pattern specified (include)
		{
			final Stream<String> test_stream1 = Stream.of("*", "test*");
			
			final Stream<Tuple2<Either<String, Tuple2<String, String>>, JsonNode>> test_stream_result_1 =
					ElasticsearchIndexUtils.createFieldIncludeLookups(test_stream1, 
							fn -> Either.right(Tuples._2T(fn, "*")), 
							field_lookups, 
								_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_notanalyzed(), JsonNode.class),
								_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_analyzed(), JsonNode.class), 
								true,
							_mapper, "_default_");
	
			final Map<Either<String, Tuple2<String, String>>, JsonNode> test_map_result_1 = 		
					test_stream_result_1.collect(Collectors.toMap(
												t2 -> t2._1(),
												t2 -> t2._2()
												));
			
			final String test_map_expected_1 = "{Right((test*,*))={'match':'test*','match_mapping_type':'*','mapping':{'type':'string','index':'analyzed','omit_norms':true,'fields':{'raw':{'type':'string','index':'not_analyzed','ignore_above':256,'fielddata':{'format':'doc_values'}}},'fielddata':{'format':'fst'}}}, Right((*,*))={'mapping':{'index':'not_analyzed','type':'{dynamic_type}','fielddata':{'format':'doc_values'}},'path_match':'*','match_mapping_type':'*'}}";
			assertEquals(test_map_expected_1, strip(test_map_result_1.toString()));
			
			//DEBUG
			//System.out.println("(Field column lookups = " + test_map_result_1 + ")");			
		}
		
		// 3) Mappings - field name specified (exclude)
		{
			final Stream<String> test_stream1 = Stream.of("@version", "field_not_present", "@timestamp");
			
			final Stream<Tuple2<Either<String, Tuple2<String, String>>, JsonNode>> test_stream_result_1 =
					ElasticsearchIndexUtils.createFieldExcludeLookups(test_stream1, fn -> Either.left(fn), field_lookups, 
							_mapper, "_default_");
	
			final Map<Either<String, Tuple2<String, String>>, JsonNode> test_map_result_1 = 		
					test_stream_result_1.collect(Collectors.toMap(
												t2 -> t2._1(),
												t2 -> t2._2()
												));
			final String test_map_expected_1 = "{Left(@timestamp)={'type':'date','fielddata':{'format':'disabled'}}, Right((field_not_present,*))={'mapping':{'index':'not_analyzed','type':'{dynamic_type}','fielddata':{'format':'disabled'}},'path_match':'field_not_present','match_mapping_type':'*'}, Left(@version)={'type':'string','index':'analyzed','fielddata':{'format':'disabled'}}}";
			assertEquals(test_map_expected_1, strip(test_map_result_1.toString()));
			
			//DEBUG
			//System.out.println("(Field column lookups = " + test_map_result_1 + ")");
		}		
		
		
		// 4) Mappings - field type specified (exclude)
		{
			final Stream<String> test_stream1 = Stream.of("*", "test*");
			
			final Stream<Tuple2<Either<String, Tuple2<String, String>>, JsonNode>> test_stream_result_1 =
					ElasticsearchIndexUtils.createFieldExcludeLookups(test_stream1, 
							fn -> Either.right(Tuples._2T(fn, "*")), 
							field_lookups, 
							_mapper, "_default_");
	
			final Map<Either<String, Tuple2<String, String>>, JsonNode> test_map_result_1 = 		
					test_stream_result_1.collect(Collectors.toMap(
												t2 -> t2._1(),
												t2 -> t2._2()
												));
			
			final String test_map_expected_1 = "{Right((test*,*))={'match':'test*','match_mapping_type':'*','mapping':{'type':'string','index':'analyzed','omit_norms':true,'fields':{'raw':{'type':'string','index':'not_analyzed','ignore_above':256,'fielddata':{'format':'disabled'}}},'fielddata':{'format':'disabled'}}}, Right((*,*))={'mapping':{'index':'not_analyzed','type':'{dynamic_type}','fielddata':{'format':'disabled'}},'path_match':'*','match_mapping_type':'*'}}";
			assertEquals(test_map_expected_1, strip(test_map_result_1.toString()));

			//DEBUG
			//System.out.println("(Field column lookups = " + test_map_result_1 + ")");			
			
		}
		
		// 5) Check with type specific fielddata formats
		{
			assertEquals(2, _config.columnar_technology_override().enabled_field_data_analyzed().size());
			assertEquals(2, _config.columnar_technology_override().enabled_field_data_notanalyzed().size());
			assertTrue("Did override settings", _config.columnar_technology_override().enabled_field_data_analyzed().containsKey("test_type_123"));
			assertTrue("Did override settings", _config.columnar_technology_override().enabled_field_data_notanalyzed().containsKey("test_type_123"));
			
			final Stream<String> test_stream1 = Stream.of("test_type_123");
			
			final Stream<Tuple2<Either<String, Tuple2<String, String>>, JsonNode>> test_stream_result_1 =
					ElasticsearchIndexUtils.createFieldIncludeLookups(test_stream1, 
							fn -> Either.left(fn), 
							field_lookups, 
								_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_notanalyzed(), JsonNode.class),
								_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_analyzed(), JsonNode.class), 
								false,
							_mapper, "test_type_123");
	
			final Map<Either<String, Tuple2<String, String>>, JsonNode> test_map_result_1 = 		
					test_stream_result_1.collect(Collectors.toMap(
												t2 -> t2._1(),
												t2 -> t2._2()
												));
			
			final String test_map_expected_1 = "{Right((test_type_123,*))={'mapping':{'index':'not_analyzed','type':'{dynamic_type}','fielddata':{'format':'test2'}},'path_match':'test_type_123','match_mapping_type':'*'}}";
			assertEquals(test_map_expected_1, strip(test_map_result_1.toString()));
			
		}		
	}

	@Test
	public void test_columnarMapping_integrated() throws JsonProcessingException, IOException {
		final String both = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/full_mapping_test.json"), Charsets.UTF_8);
		final JsonNode both_json = _mapper.readTree(both);		
		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::data_schema, 
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::columnar_schema,
									BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class)
										.with("field_include_list", Arrays.asList("column_only_enabled", "@timestamp", "@version"))
										.with("field_exclude_list", Arrays.asList("column_only_disabled"))
										.with("field_type_include_list", Arrays.asList("string"))
										.with("field_type_exclude_list", Arrays.asList("number"))
										.with("field_include_pattern_list", Arrays.asList("test*", "column_only_enabled2*"))
										.with("field_exclude_pattern_list", Arrays.asList("*noindex", "column_only_disabled2*"))
									.done().get()
							)
						.done().get()
						)
				.done().get();

		final String expected = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/mapping_test_results.json"), Charsets.UTF_8);
		final JsonNode expected_json = _mapper.readTree(expected);		
		
		
		// 1) Default
		{
			final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> field_lookups = ElasticsearchIndexUtils.parseDefaultMapping(both_json, Optional.empty());
			
			//DEBUG
//			System.out.println("(Field lookups = " + field_lookups + ")");
//			System.out.println("(Analyzed default = " + _config.columnar_technology_override().default_field_data_analyzed() + ")");
//			System.out.println("(NotAnalyzed default = " + _config.columnar_technology_override().default_field_data_notanalyzed() + ")");
		
			final XContentBuilder test_result = ElasticsearchIndexUtils.getColumnarMapping(
					test_bucket, Optional.empty(), field_lookups, 
					_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_notanalyzed(), JsonNode.class),
					_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_analyzed(), JsonNode.class), 
					_mapper.convertValue(_config.columnar_technology_override().default_field_data_notanalyzed(), JsonNode.class),
					_mapper.convertValue(_config.columnar_technology_override().default_field_data_analyzed(), JsonNode.class), 
				_mapper, "_default_");
	
			final ObjectNode expected_remove_search_settings = ((ObjectNode) expected_json.get("mappings").get("_default_")).remove(Arrays.asList("_meta", "_all", "_source"));
			assertEquals(expected_remove_search_settings.toString(), test_result.bytes().toUtf8());
			
			// 1b) While we're here, just test that the temporal service doesn't change the XContent
			
			final XContentBuilder test_result_1b_1 = ElasticsearchIndexUtils.getTemporalMapping(test_bucket, Optional.of(test_result));
			
			assertEquals(test_result_1b_1.bytes().toUtf8(), test_result.bytes().toUtf8());
			
			// Slightly more complex, add non null temporal mapping (which is just ignored for mappings purpose, it's used elsewhere)
			
			final DataBucketBean test_bucket_temporal = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::temporal_schema,
										BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class)
											.with("grouping_time_period", "1w")
										.done().get()
								)
							.done().get()
							)
					.done().get();			
			
			final XContentBuilder test_result_1b_2 = ElasticsearchIndexUtils.getTemporalMapping(test_bucket_temporal, Optional.of(test_result));
			
			assertEquals(test_result_1b_2.bytes().toUtf8(), test_result.bytes().toUtf8());
						
			// 1c) Check it exceptions out if there's a duplicate key
			
			// (It not longer exceptions out with duplicate keys, it just ignores the latter ones)
			
//			final DataBucketBean test_bucket_error = BeanTemplateUtils.build(DataBucketBean.class)
//					.with(DataBucketBean::data_schema, 
//							BeanTemplateUtils.build(DataSchemaBean.class)
//								.with(DataSchemaBean::columnar_schema,
//										BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class)
//											.with("field_include_list", Arrays.asList("column_only_enabled", "@timestamp", "@version"))
//											.with("field_exclude_list", Arrays.asList("column_only_enabled"))
//											.with("field_type_include_list", Arrays.asList("string"))
//											.with("field_type_exclude_list", Arrays.asList("number"))
//											.with("field_include_pattern_list", Arrays.asList("test*", "column_only_enabled*"))
//											.with("field_exclude_pattern_list", Arrays.asList("*noindex", "column_only_disabled*"))
//										.done().get()
//								)
//							.done().get()
//							)
//					.done().get();
//	
//
//			try {
//				ElasticsearchIndexUtils.getColumnarMapping(
//						test_bucket_error, Optional.empty(), field_lookups, 
//						_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_notanalyzed(), JsonNode.class),
//						_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_analyzed(), JsonNode.class), 
//						_mapper.convertValue(_config.columnar_technology_override().default_field_data_notanalyzed(), JsonNode.class),
//						_mapper.convertValue(_config.columnar_technology_override().default_field_data_analyzed(), JsonNode.class), 
//					_mapper);
//				
//				fail("Should have thrown exception");
//			}
//			catch (Exception e) {} // expected, carry on
			
		}
		
		// 2) Types instead of "_defaults_"
		
		// 2a) type exists
		
		{
			final String test_type = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/full_mapping_test_type.json"), Charsets.UTF_8);
			final JsonNode test_type_json = _mapper.readTree(test_type);		
			
			
			final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> field_lookups = ElasticsearchIndexUtils.parseDefaultMapping(test_type_json, Optional.of("type_test"));			
			
			final XContentBuilder test_result = ElasticsearchIndexUtils.getColumnarMapping(
					test_bucket, Optional.of(XContentFactory.jsonBuilder().startObject()), field_lookups, 
					_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_notanalyzed(), JsonNode.class),
					_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_analyzed(), JsonNode.class), 
					_mapper.convertValue(_config.columnar_technology_override().default_field_data_notanalyzed(), JsonNode.class),
					_mapper.convertValue(_config.columnar_technology_override().default_field_data_analyzed(), JsonNode.class), 
				_mapper, "_default_");
	
			assertEquals(expected_json.get("mappings").get("_default_").toString(), test_result.bytes().toUtf8());
		}
		
		// 2b) type doesn't exist, should fall back to _default_

		{
			final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> field_lookups = ElasticsearchIndexUtils.parseDefaultMapping(both_json, Optional.of("no_such_type"));			
			
			final XContentBuilder test_result = ElasticsearchIndexUtils.getColumnarMapping(
					test_bucket, Optional.of(XContentFactory.jsonBuilder().startObject()), field_lookups, 
					_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_notanalyzed(), JsonNode.class),
					_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_analyzed(), JsonNode.class), 
					_mapper.convertValue(_config.columnar_technology_override().default_field_data_notanalyzed(), JsonNode.class),
					_mapper.convertValue(_config.columnar_technology_override().default_field_data_analyzed(), JsonNode.class), 
				_mapper, "_default_");
	
			assertEquals(expected_json.get("mappings").get("_default_").toString(), test_result.bytes().toUtf8());
		}
	}
	
	@Test
	public void test_searchMapping_integrated() throws JsonProcessingException, IOException {
		
		final ElasticsearchIndexServiceConfigBean config_bean = ElasticsearchIndexConfigUtils.buildConfigBean(ConfigFactory.empty());	
		
		// TEST with default config, no settings specified in mapping
		{		
			final String default_settings = "{\"settings\":{\"index.indices.fielddata.cache.size\":\"10%\",\"index.refresh_interval\":\"5s\"},\"mappings\":{\"_default_\":{\"_meta\":{\"bucket_path\":null,\"secondary_buffer\":\"\"},\"_all\":{\"enabled\":false},\"_source\":{\"enabled\":true}}}}";
			
			final DataBucketBean test_bucket_0a = BeanTemplateUtils.build(DataBucketBean.class).done().get();
			final DataBucketBean test_bucket_0b = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class).done().get()).done().get();
			final DataBucketBean test_bucket_0c = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema,
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
								.done().get())
							.done().get())
					.done().get();
			final DataBucketBean test_bucket_0d = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema,
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
											.with("enabled", false)
											.with("technology_override_schema",
													ImmutableMap.builder().build()
													)
								.done().get())
							.done().get())
					.done().get();
			final DataBucketBean test_bucket_0e = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema,
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
											.with("enabled", false)
											.with("technology_override_schema",
													ImmutableMap.builder().put("settings", ImmutableMap.builder().build()).build()
													)
								.done().get())
							.done().get())
					.done().get();
			
			// Nothing at all:
			assertEquals(default_settings, ElasticsearchIndexUtils.getSearchServiceMapping(test_bucket_0a, Optional.empty(), config_bean, Optional.of(XContentFactory.jsonBuilder().startObject()), _mapper).bytes().toUtf8());
			assertEquals(default_settings, ElasticsearchIndexUtils.getSearchServiceMapping(test_bucket_0b, Optional.empty(), config_bean, Optional.empty(), _mapper).bytes().toUtf8());
			assertEquals(default_settings, ElasticsearchIndexUtils.getSearchServiceMapping(test_bucket_0c, Optional.empty(), config_bean, Optional.empty(), _mapper).bytes().toUtf8());
			assertEquals(default_settings, ElasticsearchIndexUtils.getSearchServiceMapping(test_bucket_0d, Optional.empty(), config_bean, Optional.empty(), _mapper).bytes().toUtf8());
			assertEquals(default_settings, ElasticsearchIndexUtils.getSearchServiceMapping(test_bucket_0e, Optional.empty(), config_bean, Optional.empty(), _mapper).bytes().toUtf8());
			
			// Not even config
			final ElasticsearchIndexServiceConfigBean config_bean2 = BeanTemplateUtils.build(ElasticsearchIndexServiceConfigBean.class).done().get();
			assertEquals("{\"mappings\":{\"_default_\":{\"_meta\":{\"bucket_path\":null,\"secondary_buffer\":\"\"}}}}", ElasticsearchIndexUtils.getSearchServiceMapping(test_bucket_0a, Optional.empty(), config_bean2, Optional.of(XContentFactory.jsonBuilder().startObject()), _mapper).bytes().toUtf8());
		}		
		
		// TEST with settings specified in mapping
		{
			final String user_settings = "{\"settings\":{\"index.indices.fielddata.cache.size\":\"25%\"},\"mappings\":{\"data_object\":{\"_meta\":{\"bucket_path\":null,\"secondary_buffer\":\"\"},\"_all\":{\"enabled\":false},\"_source\":{\"enabled\":true}}}}";
			
			final DataBucketBean test_bucket_1 = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema,
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
											.with("enabled", true)
											.with("technology_override_schema",
													ImmutableMap.builder().put("settings", 
															ImmutableMap.builder()
																.put("index.indices.fielddata.cache.size", "25%")
															.build())
															.put("collide_policy", "error")
													.build()
													)
								.done().get())
							.done().get())
					.done().get();

			final ElasticsearchIndexServiceConfigBean schema_config = ElasticsearchIndexConfigUtils.buildConfigBeanFromSchema(test_bucket_1, config_bean, _mapper);
			
			assertEquals(user_settings, ElasticsearchIndexUtils.getSearchServiceMapping(test_bucket_1, Optional.empty(), schema_config, Optional.empty(), _mapper).bytes().toUtf8());			
		}
		
		// TEST with mapping overrides
		{
			final String user_settings = "{\"settings\":{\"index.indices.fielddata.cache.size\":\"25%\"},\"mappings\":{\"test_type\":{\"_meta\":{\"bucket_path\":null,\"secondary_buffer\":\"\"},\"_all\":{\"enabled\":false}}}}";
			
			final DataBucketBean test_bucket_1 = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::data_schema, 
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::search_index_schema,
										BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
											.with("enabled", true)
											.with("technology_override_schema",
													ImmutableMap.builder().put("settings", 
															ImmutableMap.builder()
																.put("index.indices.fielddata.cache.size", "25%")
															.build())
															.put("collide_policy", "error")
															.put("type_name_or_prefix", "test_type")
															.put("mapping_overrides",
																	ImmutableMap.builder()
																		.put("_default_", ImmutableMap.builder().put("_all", ImmutableMap.builder().put("enabled", true).build()).build())
																		.put("test_type", ImmutableMap.builder().put("_all", ImmutableMap.builder().put("enabled", false).build()).build())
																	.build())
													.build()
													)
								.done().get())
							.done().get())
					.done().get();
			
			final ElasticsearchIndexServiceConfigBean schema_config = ElasticsearchIndexConfigUtils.buildConfigBeanFromSchema(test_bucket_1, config_bean, _mapper);
			
			assertEquals(user_settings, ElasticsearchIndexUtils.getSearchServiceMapping(test_bucket_1, Optional.empty(), schema_config, Optional.empty(), _mapper).bytes().toUtf8());						
		}
	}

	
	@Test
	public void test_templateMapping() throws JsonProcessingException, IOException {
		final DataBucketBean b = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test/template/mapping").done().get();

		final String expected = "{\"template\":\"test_template_mapping__3f584adbcb13*\"}";
		
		assertEquals(expected, ElasticsearchIndexUtils.getTemplateMapping(b, Optional.empty()).bytes().toUtf8());
	}
	
	@Test
	public void test_fullMapping() throws JsonProcessingException, IOException {
		
		final String both = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/full_mapping_test.json"), Charsets.UTF_8);
		final JsonNode both_json = _mapper.readTree(both);		
		final String uuid = "de305d54-75b4-431b-adb2-eb6b9e546015";
		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, uuid)
				.with(DataBucketBean::full_name, "/test/full/mapping")
				.with(DataBucketBean::data_schema, 
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::search_index_schema,
									BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
										.with("enabled", true)
										.with("technology_override_schema",
												ImmutableMap.builder()
													.put("settings", 
														ImmutableMap.builder()
															.put("index.refresh_interval","10s")
														.build())
													.put("mappings", both_json.get("mappings"))	
													.build()
												)
									.done().get()
									)
							.with(DataSchemaBean::columnar_schema,
									BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class)
										.with("field_include_list", Arrays.asList("column_only_enabled", "@timestamp", "@version"))
										.with("field_exclude_list", Arrays.asList("column_only_disabled"))
										.with("field_include_pattern_list", Arrays.asList("test*", "column_only_enabled2*"))
										.with("field_type_include_list", Arrays.asList("string"))
										.with("field_exclude_pattern_list", Arrays.asList("*noindex", "column_only_disabled2*"))
										.with("field_type_exclude_list", Arrays.asList("number"))
									.done().get()
							)
						.done().get()
						)
				.done().get();

		final String expected = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/mapping_test_results.json"), Charsets.UTF_8);
		final JsonNode expected_json = _mapper.readTree(expected);				
		
		final ElasticsearchIndexServiceConfigBean schema_config = ElasticsearchIndexConfigUtils.buildConfigBeanFromSchema(test_bucket, _config, _mapper);			
		
		// 1) Sub method
		{
			final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> field_lookups = ElasticsearchIndexUtils.parseDefaultMapping(both_json, Optional.empty());
			
			final XContentBuilder test_result = ElasticsearchIndexUtils.getFullMapping(
					test_bucket, Optional.empty(), schema_config, field_lookups, 
					_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_notanalyzed(), JsonNode.class),
					_mapper.convertValue(_config.columnar_technology_override().enabled_field_data_analyzed(), JsonNode.class), 
					_mapper.convertValue(_config.columnar_technology_override().default_field_data_notanalyzed(), JsonNode.class),
					_mapper.convertValue(_config.columnar_technology_override().default_field_data_analyzed(), JsonNode.class), 
				_mapper, "misc_test");
			
			assertEquals(expected_json.toString(), test_result.bytes().toUtf8());
		}
		
		// Final method
		{ 
			final XContentBuilder test_result = ElasticsearchIndexUtils.createIndexMapping(test_bucket, Optional.empty(), schema_config, _mapper, "_default_");			
			assertEquals(expected_json.toString(), test_result.bytes().toUtf8());
		}
	}

	
	@Test
	public void test_defaultMappings() {
		
		final DataBucketBean search_index_test = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/test")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::search_index_schema, 
									BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
										//(empty)
									.done().get())
						.done().get()
						)
			.done().get();
		
		final String expected = "{\"template\":\"test_test__f19167d49eac*\",\"settings\":{\"index.indices.fielddata.cache.size\":\"10%\",\"index.refresh_interval\":\"5s\"},\"mappings\":{\"_default_\":{\"_meta\":{\"bucket_path\":\"/test/test\",\"secondary_buffer\":\"\"},\"_all\":{\"enabled\":false},\"_source\":{\"enabled\":true},\"properties\":{\"@timestamp\":{\"fielddata\":{\"format\":\"doc_values\"},\"index\":\"not_analyzed\",\"type\":\"date\"}},\"dynamic_templates\":[{\"STAR_string\":{\"mapping\":{\"fielddata\":{\"format\":\"disabled\"},\"fields\":{\"raw\":{\"fielddata\":{\"format\":\"disabled\"},\"ignore_above\":256,\"index\":\"not_analyzed\",\"type\":\"string\"}},\"index\":\"analyzed\",\"omit_norms\":true,\"type\":\"string\"},\"match\":\"*\",\"match_mapping_type\":\"string\"}},{\"STAR_STAR\":{\"mapping\":{\"fielddata\":{\"format\":\"disabled\"},\"index\":\"not_analyzed\",\"type\":\"{dynamic_type}\"},\"match\":\"*\",\"match_mapping_type\":\"*\"}}]}}}";
		
		// Search index schema only
		{			
			final ElasticsearchIndexServiceConfigBean schema_config = ElasticsearchIndexConfigUtils.buildConfigBeanFromSchema(search_index_test, _config, _mapper);
			
			final Optional<String> type = Optional.ofNullable(schema_config.search_technology_override()).map(t -> t.type_name_or_prefix());
			final String index_type = CollidePolicy.new_type == Optional.ofNullable(schema_config.search_technology_override())
					.map(t -> t.collide_policy()).orElse(CollidePolicy.new_type)
						? "_default_"
						: type.orElse(ElasticsearchIndexServiceConfigBean.DEFAULT_FIXED_TYPE_NAME);
			
			final XContentBuilder mapping = ElasticsearchIndexUtils.createIndexMapping(search_index_test, Optional.empty(), schema_config, _mapper, index_type);
	
			assertEquals("Get expected search_index_test schema", expected, mapping.bytes().toUtf8());
		}
		
		// Temporal + search index schema
		{
			final DataBucketBean temporal_test = BeanTemplateUtils.clone(search_index_test)
													.with(DataBucketBean::data_schema, 
															BeanTemplateUtils.clone(search_index_test.data_schema())
																.with(DataSchemaBean::temporal_schema, 
																		BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class).done().get()
																		)
																.done()
															).done();
			
			final ElasticsearchIndexServiceConfigBean schema_config = ElasticsearchIndexConfigUtils.buildConfigBeanFromSchema(temporal_test, _config, _mapper);
			
			final Optional<String> type = Optional.ofNullable(schema_config.search_technology_override()).map(t -> t.type_name_or_prefix());
			final String index_type = CollidePolicy.new_type == Optional.ofNullable(schema_config.search_technology_override())
					.map(t -> t.collide_policy()).orElse(CollidePolicy.new_type)
						? "_default_"
						: type.orElse(ElasticsearchIndexServiceConfigBean.DEFAULT_FIXED_TYPE_NAME);
			
			final XContentBuilder mapping = ElasticsearchIndexUtils.createIndexMapping(temporal_test, Optional.empty(), schema_config, _mapper, index_type);
	
			assertEquals("Get expected search_index_test schema", expected, mapping.bytes().toUtf8());
		}
		
		// Temporal + search index schema, with time field specified
		{
			final DataBucketBean temporal_test = BeanTemplateUtils.clone(search_index_test)
													.with(DataBucketBean::data_schema, 
															BeanTemplateUtils.clone(search_index_test.data_schema())
																.with(DataSchemaBean::temporal_schema, 
																		BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class)
																			.with(DataSchemaBean.TemporalSchemaBean::time_field, "testtime")
																		.done().get()
																		)
																.done()
															).done();
			
			final ElasticsearchIndexServiceConfigBean schema_config = ElasticsearchIndexConfigUtils.buildConfigBeanFromSchema(temporal_test, _config, _mapper);
			
			//

			//(has testtime inserted)
			final String expected2 = "{\"template\":\"test_test__f19167d49eac*\",\"settings\":{\"index.indices.fielddata.cache.size\":\"10%\",\"index.refresh_interval\":\"5s\"},\"mappings\":{\"_default_\":{\"_meta\":{\"bucket_path\":\"/test/test\",\"secondary_buffer\":\"\"},\"_all\":{\"enabled\":false},\"_source\":{\"enabled\":true},\"properties\":{\"@timestamp\":{\"fielddata\":{\"format\":\"doc_values\"},\"index\":\"not_analyzed\",\"type\":\"date\"},\"testtime\":{\"fielddata\":{\"format\":\"doc_values\"},\"index\":\"not_analyzed\",\"type\":\"date\"}},\"dynamic_templates\":[{\"STAR_string\":{\"mapping\":{\"fielddata\":{\"format\":\"disabled\"},\"fields\":{\"raw\":{\"fielddata\":{\"format\":\"disabled\"},\"ignore_above\":256,\"index\":\"not_analyzed\",\"type\":\"string\"}},\"index\":\"analyzed\",\"omit_norms\":true,\"type\":\"string\"},\"match\":\"*\",\"match_mapping_type\":\"string\"}},{\"STAR_STAR\":{\"mapping\":{\"fielddata\":{\"format\":\"disabled\"},\"index\":\"not_analyzed\",\"type\":\"{dynamic_type}\"},\"match\":\"*\",\"match_mapping_type\":\"*\"}}]}}}";

			final Optional<String> type = Optional.ofNullable(schema_config.search_technology_override()).map(t -> t.type_name_or_prefix());
			final String index_type = CollidePolicy.new_type == Optional.ofNullable(schema_config.search_technology_override())
					.map(t -> t.collide_policy()).orElse(CollidePolicy.new_type)
						? "_default_"
						: type.orElse(ElasticsearchIndexServiceConfigBean.DEFAULT_FIXED_TYPE_NAME);
			
			final XContentBuilder mapping = ElasticsearchIndexUtils.createIndexMapping(temporal_test, Optional.empty(), schema_config, _mapper, index_type);
	
			assertEquals("Get expected search_index_test schema", expected2, mapping.bytes().toUtf8());
		}
		
		// Columnar + search index schema
		{
			final DataBucketBean columnar_test = BeanTemplateUtils.clone(search_index_test)
													.with(DataBucketBean::data_schema, 
															BeanTemplateUtils.clone(search_index_test.data_schema())
																.with(DataSchemaBean::columnar_schema, 
																		BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class).done().get()
																		)
																.done()
															).done();
			
			final ElasticsearchIndexServiceConfigBean schema_config = ElasticsearchIndexConfigUtils.buildConfigBeanFromSchema(columnar_test, _config, _mapper);
			
			final Optional<String> type = Optional.ofNullable(schema_config.search_technology_override()).map(t -> t.type_name_or_prefix());
			final String index_type = CollidePolicy.new_type == Optional.ofNullable(schema_config.search_technology_override())
					.map(t -> t.collide_policy()).orElse(CollidePolicy.new_type)
						? "_default_"
						: type.orElse(ElasticsearchIndexServiceConfigBean.DEFAULT_FIXED_TYPE_NAME);
			
			final XContentBuilder mapping = ElasticsearchIndexUtils.createIndexMapping(columnar_test, Optional.empty(), schema_config, _mapper, index_type);
	
			assertEquals("Get expected search_index_test schema", expected, mapping.bytes().toUtf8());
		}
		
		// Columnar + temporlal search index schema
		{
			final DataBucketBean temporal_columnar_test = BeanTemplateUtils.clone(search_index_test)
													.with(DataBucketBean::data_schema, 
															BeanTemplateUtils.clone(search_index_test.data_schema())
																.with(DataSchemaBean::temporal_schema, 
																		BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class).done().get()
																		)
																.with(DataSchemaBean::columnar_schema, 
																		BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class).done().get()
																		)
																.done()
															).done();
			
			final ElasticsearchIndexServiceConfigBean schema_config = ElasticsearchIndexConfigUtils.buildConfigBeanFromSchema(temporal_columnar_test, _config, _mapper);
			
			final Optional<String> type = Optional.ofNullable(schema_config.search_technology_override()).map(t -> t.type_name_or_prefix());
			final String index_type = CollidePolicy.new_type == Optional.ofNullable(schema_config.search_technology_override())
					.map(t -> t.collide_policy()).orElse(CollidePolicy.new_type)
						? "_default_"
						: type.orElse(ElasticsearchIndexServiceConfigBean.DEFAULT_FIXED_TYPE_NAME);
			
			final XContentBuilder mapping = ElasticsearchIndexUtils.createIndexMapping(temporal_columnar_test, Optional.empty(), schema_config, _mapper, index_type);
	
			assertEquals("Get expected search_index_test schema", expected, mapping.bytes().toUtf8());
		}
		
	}
	
}
