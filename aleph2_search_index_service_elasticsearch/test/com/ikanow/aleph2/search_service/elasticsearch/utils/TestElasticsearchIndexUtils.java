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

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

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
		
		assertEquals(Tuples._2T("STAR", "STAR"), ElasticsearchIndexUtils.buildMatchPair(_mapper.readTree("{}")));
		
		assertEquals(Tuples._2T("fieldSTAR", "STAR"), ElasticsearchIndexUtils.buildMatchPair(_mapper.readTree("{\"match\":\"field*\"}")));
		
		assertEquals(Tuples._2T("fieldSTARfield", "type*"), ElasticsearchIndexUtils.buildMatchPair(_mapper.readTree("{\"match\":\"field*field\", \"match_mapping_type\": \"type*\"}")));
		
		//TODO now test some of the logic
		
//	      "properties" : {
//	         "@version": { "type": "string", "index": "not_analyzed" },
//	         "@timestamp": { "type": "date" },
//	         "sourceKey": { "type": "string", "index": "not_analyzed" },
//	         "geoip"  : {
//	           "type" : "object",
//	             "dynamic": true,
//	             "path": "full",
//	             "properties" : {
//	               "location" : { "type" : "geo_point" }
//	             }
//	         }
//	       }
		
	}
	
}
