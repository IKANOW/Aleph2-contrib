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
import java.util.stream.Collectors;

import org.junit.Test;

import scala.Tuple2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

import fj.data.Either;

public class TestElasticsearchIndexUtils {

	static ObjectMapper _mapper = new ObjectMapper();
	
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
		
		// Build match pair
		
		assertEquals(Tuples._2T("STAR", "STAR"), ElasticsearchIndexUtils.buildMatchPair(_mapper.readTree("{}")));
		
		assertEquals(Tuples._2T("fieldSTAR", "STAR"), ElasticsearchIndexUtils.buildMatchPair(_mapper.readTree("{\"match\":\"field*\"}")));
		
		assertEquals(Tuples._2T("fieldSTARfield", "type*"), ElasticsearchIndexUtils.buildMatchPair(_mapper.readTree("{\"match\":\"field*field\", \"match_mapping_type\": \"type*\"}")));
		
		// More complex objects
		
		final String properties = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/properties_test.json"), Charsets.UTF_8);
		final String templates = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/templates_test.json"), Charsets.UTF_8);
		
		final JsonNode properties_json = _mapper.readTree(properties);
		final JsonNode templates_json = _mapper.readTree(templates);
		
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
					Tuples._2T("STAR", "string"),
					Tuples._2T("STAR", "number")
				), 
				templates_test2.keySet().stream().map(e -> e.right().value()).collect(Collectors.toList()));
		
		// Putting it all together...
		
		//TODO
		
		// A couple of error checks:
		// - Missing mapping
		// - Mapping not an object
		
		
	}
	
}
