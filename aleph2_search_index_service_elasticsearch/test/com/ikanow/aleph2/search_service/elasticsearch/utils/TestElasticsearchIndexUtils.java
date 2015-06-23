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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean;
import com.typesafe.config.ConfigFactory;

import fj.data.Either;

public class TestElasticsearchIndexUtils {

	static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	ElasticsearchIndexServiceConfigBean _config ;
	
	@Before
	public void getConfig() {
		_config = ElasticsearchIndexConfigUtils.buildConfigBean(ConfigFactory.empty());
	}
	
	@Test
	public void test_baseNames() {
		
		final String uuid = "de305d54-75b4-431b-adb2-eb6b9e546014";
		
		final String base_index = ElasticsearchIndexUtils.getBaseIndexName(BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::_id, uuid).done().get());
		
		assertEquals("de305d54_75b4_431b_adb2_eb6b9e546014", base_index);
		
		assertEquals(uuid, ElasticsearchIndexUtils.getBucketIdFromIndexName("de305d54_75b4_431b_adb2_eb6b9e546014_2015"));
		
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
								_mapper.convertValue(_config.columnar_technology_override().default_field_data_analyzed(), JsonNode.class), 
								_mapper.convertValue(_config.columnar_technology_override().default_field_data_notanalyzed(), JsonNode.class),
							_mapper);
	
			final Map<Either<String, Tuple2<String, String>>, JsonNode> test_map_result_1 = 		
					test_stream_result_1.collect(Collectors.toMap(
												t2 -> t2._1(),
												t2 -> t2._2()
												));
			final String test_map_expected_1 = "{Left(@timestamp)={'type':'date','fielddata':{}}, Left(@version)={'type':'string','index':'analyzed','fielddata':{'format':'fst'}}, Left(field_not_present)={'index':'not_analyzed','fielddata':{'format':'doc_values'}}}";
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
								_mapper.convertValue(_config.columnar_technology_override().default_field_data_analyzed(), JsonNode.class), 
								_mapper.convertValue(_config.columnar_technology_override().default_field_data_notanalyzed(), JsonNode.class),
							_mapper);
	
			final Map<Either<String, Tuple2<String, String>>, JsonNode> test_map_result_1 = 		
					test_stream_result_1.collect(Collectors.toMap(
												t2 -> t2._1(),
												t2 -> t2._2()
												));
			
			final String test_map_expected_1 = "{Right((test*,*))={'match':'test*','match_mapping_type':'*','mapping':{'type':'string','index':'analyzed','omit_norms':true,'fields':{'raw':{'type':'string','index':'not_analyzed','ignore_above':256}},'fielddata':{'format':'fst'}}}, Right((*,*))={'mapping':{'index':'not_analyzed','fielddata':{'format':'doc_values'}},'match':'*','match_mapping_type':'*'}}";
			assertEquals(test_map_expected_1, strip(test_map_result_1.toString()));
			
			//DEBUG
			//System.out.println("(Field column lookups = " + test_map_result_1 + ")");			
		}
		
		// 3) Mappings - field name specified (exclude)
		{
			final Stream<String> test_stream1 = Stream.of("@version", "field_not_present", "@timestamp");
			
			final Stream<Tuple2<Either<String, Tuple2<String, String>>, JsonNode>> test_stream_result_1 =
					ElasticsearchIndexUtils.createFieldExcludeLookups(test_stream1, fn -> Either.left(fn), field_lookups, 
								_mapper.convertValue(_config.columnar_technology_override().default_field_data_analyzed(), JsonNode.class), 
								_mapper.convertValue(_config.columnar_technology_override().default_field_data_notanalyzed(), JsonNode.class),
							_mapper);
	
			final Map<Either<String, Tuple2<String, String>>, JsonNode> test_map_result_1 = 		
					test_stream_result_1.collect(Collectors.toMap(
												t2 -> t2._1(),
												t2 -> t2._2()
												));
			final String test_map_expected_1 = "{Left(@timestamp)={'type':'date','fielddata':{}}, Left(@version)={'type':'string','index':'analyzed','fielddata':{'format':'disabled'}}, Left(field_not_present)={'index':'not_analyzed','fielddata':{'format':'disabled'}}}";
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
								_mapper.convertValue(_config.columnar_technology_override().default_field_data_analyzed(), JsonNode.class), 
								_mapper.convertValue(_config.columnar_technology_override().default_field_data_notanalyzed(), JsonNode.class),
							_mapper);
	
			final Map<Either<String, Tuple2<String, String>>, JsonNode> test_map_result_1 = 		
					test_stream_result_1.collect(Collectors.toMap(
												t2 -> t2._1(),
												t2 -> t2._2()
												));
			
			final String test_map_expected_1 = "{Right((test*,*))={'match':'test*','match_mapping_type':'*','mapping':{'type':'string','index':'analyzed','omit_norms':true,'fields':{'raw':{'type':'string','index':'not_analyzed','ignore_above':256}},'fielddata':{'format':'disabled'}}}, Right((*,*))={'mapping':{'index':'not_analyzed','fielddata':{'format':'disabled'}},'match':'*','match_mapping_type':'*'}}";
			assertEquals(test_map_expected_1, strip(test_map_result_1.toString()));

			//DEBUG
			//System.out.println("(Field column lookups = " + test_map_result_1 + ")");			
			
		}
		
		//TODO: non-default fielddata formats
	}

	@Test
	public void test_columnarMapping_integrated() throws JsonProcessingException, IOException {
		final String both = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/full_mapping_test.json"), Charsets.UTF_8);
		final JsonNode both_json = _mapper.readTree(both);		
		
		final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> field_lookups = ElasticsearchIndexUtils.parseDefaultMapping(both_json, Optional.empty());
		
		System.out.println("(Field lookups = " + field_lookups + ")");
		System.out.println("(Analyzed default = " + _config.columnar_technology_override().default_field_data_analyzed() + ")");
		System.out.println("(NotAnalyzed default = " + _config.columnar_technology_override().default_field_data_notanalyzed() + ")");
		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::data_schema, 
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::columnar_schema,
									BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class)
										.with("field_include_list", Arrays.asList("column_only_enabled", "@timestamp", "@version"))
										.with("field_exclude_list", Arrays.asList("column_only_disabled"))
										.with("field_type_include_list", Arrays.asList("string"))
										.with("field_type_exclude_list", Arrays.asList("number"))
										.with("field_include_pattern_list", Arrays.asList("test*", "column_only_enabled*"))
										.with("field_exclude_pattern_list", Arrays.asList("*noindex", "column_only_disabled*"))
									.done().get()
							)
						.done().get()
						)
				.done().get();

		final XContentBuilder test_result = ElasticsearchIndexUtils.getColumnarMapping(
				test_bucket, Optional.empty(), field_lookups, 
				_mapper.convertValue(_config.columnar_technology_override().default_field_data_analyzed(), JsonNode.class), 
				_mapper.convertValue(_config.columnar_technology_override().default_field_data_notanalyzed(), JsonNode.class),
			_mapper);

		final String expected = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/mapping_test_results.json"), Charsets.UTF_8);
		final JsonNode expected_json = _mapper.readTree(expected);		


		assertEquals(expected_json.get("mappings").get("_default_").toString(), test_result.bytes().toUtf8());
		
		//DEBUG
		//System.out.println("XContent = " + test_result.bytes().toUtf8());
		
		//TODO: some things to try:
		// 1) null values
		// 2) duplicate keys (should exception)
		// 3) like the above but with a specific type
	}

	
}
