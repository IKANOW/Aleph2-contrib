/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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

package com.ikanow.aleph2.search_service.elasticsearch.utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Optional;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

import fj.data.Validation;

/**
 * @author Alex
 *
 */
public class TestElasticsearchHiveUtils {

	public static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	@Test
	public void test_generateHiveSchema() throws IOException {
		final String hive_schema = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/sample_hive_schema.json"), Charsets.UTF_8);
		final JsonNode hive_schema_json = _mapper.readTree(hive_schema);
		
		final Validation<String, String> test_success = ElasticsearchHiveUtils.generateHiveSchema("PREFIX-", hive_schema_json, true);
		assertTrue("Failed: " + (test_success.isFail() ? test_success.fail() : "(no error)"), test_success.isSuccess());
		final String hand_checked_results = "PREFIX-(primitive_field BIGINT,raw_struct STRUCT<raw_field_1: VARCHAR,raw_field_2: DATE>,raw_map MAP<STRING, TIMESTAMP>,raw_array ARRAY<TINYINT>,raw_union_1 UNIONTYPE< SMALLINT>,raw_union_2 UNIONTYPE< INT, BOOLEAN>,nested_struct STRUCT<raw_field_1: FLOAT,nested_field2: STRUCT<nested_raw_1: DOUBLE,nested_nested_2: ARRAY<BINARY>>>,nested_map MAP<STRING, STRUCT<raw_field_1: CHAR,nested_nested_2: UNIONTYPE< STRING, STRING, DATE>>>,nested_array_1 ARRAY<ARRAY<STRING>>,nested_array_2 ARRAY<STRUCT<raw_field_1: STRING>>,nested_union UNIONTYPE< STRUCT<raw_field_1: STRING>>)";
		assertEquals(hand_checked_results, test_success.success());		
	}
	
}
