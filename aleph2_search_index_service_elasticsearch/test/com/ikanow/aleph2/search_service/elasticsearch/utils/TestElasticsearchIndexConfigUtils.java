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

import java.util.Map;

import org.elasticsearch.common.collect.ImmutableMap;
import org.junit.Test;

import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestElasticsearchIndexConfigUtils {

	//TODO: test mapping overrides
	
	@Test 
	public void test_configUtils() {
		
		// Check defaults are applied if none are specified
		{
			final Map<String, String> config_map = ImmutableMap.<String, String>builder()
												.put("ElasticsearchIndexService.elasticsearch_connection", "localhost:9200")
												.put("ElasticsearchIndexService.cluster_name", "test")
												.build();
			
			final Config config = ConfigFactory.parseMap(config_map);
			
			final ElasticsearchIndexServiceConfigBean config_bean = ElasticsearchIndexConfigUtils.buildConfigBean(config);
			
			// Derived fields
			assertTrue("Default field settings", null != config_bean.columnar_technology_override().default_field_data_analyzed());
			assertTrue("Default field settings for analyzed fields", null != config_bean.columnar_technology_override().default_field_data_analyzed().get("_default"));
			assertTrue("Default field settings for non-analyzied fields", null != config_bean.columnar_technology_override().default_field_data_notanalyzed().get("_default"));
			assertEquals(ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean.CollidePolicy.new_type, config_bean.search_technology_override().collide_policy());
			assertTrue("Search Settings", null != config_bean.search_technology_override().settings());
			assertTrue("Search Settings", config_bean.search_technology_override().settings().size() > 0);
			
			// Original fields
			assertEquals("localhost:9200", config_bean.elasticsearch_connection());
			assertEquals("test", config_bean.cluster_name());
												
		}
		// Check defaults aren't applied if they are specified (search index)
		
		{
			final Map<String, String> config_map = ImmutableMap.<String, String>builder()
					.put("ElasticsearchIndexService.elasticsearch_connection", "localhost:9200")
					.put("ElasticsearchIndexService.cluster_name", "test")
					.put("ElasticsearchIndexService.search_technology_override.collide_policy", "error")
					.build();
	
			final Config config = ConfigFactory.parseMap(config_map);
	
			final ElasticsearchIndexServiceConfigBean config_bean = ElasticsearchIndexConfigUtils.buildConfigBean(config);
			
			// Original fields
			assertEquals("localhost:9200", config_bean.elasticsearch_connection());
			assertEquals("test", config_bean.cluster_name());
			
			// Derived fields (search)
			assertEquals(ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean.CollidePolicy.error, config_bean.search_technology_override().collide_policy());
			assertTrue("Search Settings", null != config_bean.search_technology_override().settings());
			assertTrue("Search Settings", config_bean.search_technology_override().settings().size() > 0);
			
			// Derived fields (columnar)
			assertTrue("Default field settings", null != config_bean.columnar_technology_override().default_field_data_analyzed());
			assertTrue("Default field settings for analyzed fields", null != config_bean.columnar_technology_override().default_field_data_analyzed().get("_default"));
			assertTrue("Default field settings for non-analyzied fields", null != config_bean.columnar_technology_override().default_field_data_notanalyzed().get("_default"));
		}
		
		// Check defaults aren't applied if they are specified (columnar)
		
		{
			final Map<String, String> config_map = ImmutableMap.<String, String>builder()
					.put("ElasticsearchIndexService.elasticsearch_connection", "localhost:9200")
					.put("ElasticsearchIndexService.cluster_name", "test")
					.put("ElasticsearchIndexService.columnar_technology_override.test", "test")
					.build();
	
			final Config config = ConfigFactory.parseMap(config_map);
	
			final ElasticsearchIndexServiceConfigBean config_bean = ElasticsearchIndexConfigUtils.buildConfigBean(config);
			
			// Original fields
			assertEquals("localhost:9200", config_bean.elasticsearch_connection());
			assertEquals("test", config_bean.cluster_name());
			
			// Derived fields (search)
			assertEquals(ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean.CollidePolicy.new_type, config_bean.search_technology_override().collide_policy());
			assertTrue("Search Settings", null != config_bean.search_technology_override().settings());
			assertTrue("Search Settings", config_bean.search_technology_override().settings().size() > 0);
			
			// Derived fields (columnar)
			assertTrue("Default field settings", null != config_bean.columnar_technology_override().default_field_data_analyzed());
			assertTrue("Default field settings for analyzed fields", null != config_bean.columnar_technology_override().default_field_data_analyzed().get("_default"));
			assertTrue("Default field settings for non-analyzied fields", null != config_bean.columnar_technology_override().default_field_data_notanalyzed().get("_default"));			
		}
		
		// Check defaults aren't applied if they are specified (both)
		
		{
			final Map<String, String> config_map = ImmutableMap.<String, String>builder()
					.put("ElasticsearchIndexService.elasticsearch_connection", "localhost:9200")
					.put("ElasticsearchIndexService.cluster_name", "test")
					.put("ElasticsearchIndexService.columnar_technology_override.test", "test")
					.put("ElasticsearchIndexService.search_technology_override.collide_policy", "error")
					.build();
	
			final Config config = ConfigFactory.parseMap(config_map);
	
			final ElasticsearchIndexServiceConfigBean config_bean = ElasticsearchIndexConfigUtils.buildConfigBean(config);
			
			// Original fields
			assertEquals("localhost:9200", config_bean.elasticsearch_connection());
			assertEquals("test", config_bean.cluster_name());
			
			// Derived fields (search)
			assertEquals(ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean.CollidePolicy.error, config_bean.search_technology_override().collide_policy());
			assertTrue("Search Settings", null != config_bean.search_technology_override().settings());
			assertTrue("Search Settings", config_bean.search_technology_override().settings().size() > 0);
			
			// Derived fields (columnar)
			assertTrue("Default field settings", null != config_bean.columnar_technology_override().default_field_data_analyzed());
			assertTrue("Default field settings for analyzed fields", null != config_bean.columnar_technology_override().default_field_data_analyzed().get("_default"));
			assertTrue("Default field settings for non-analyzied fields", null != config_bean.columnar_technology_override().default_field_data_notanalyzed().get("_default"));			
		}
	}
}
