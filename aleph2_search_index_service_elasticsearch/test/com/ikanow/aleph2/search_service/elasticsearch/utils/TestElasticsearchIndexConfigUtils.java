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
import java.util.Map;
import java.util.Optional;

import org.elasticsearch.common.collect.ImmutableMap;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean.CollidePolicy;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestElasticsearchIndexConfigUtils {

	public static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	final ElasticsearchIndexServiceConfigBean _config = ElasticsearchIndexConfigUtils.buildConfigBean(ConfigFactory.empty());	
	
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
			assertTrue("Default field settings", null != config_bean.columnar_technology_override().enabled_field_data_analyzed());
			assertTrue("Default field settings for analyzed fields", null != config_bean.columnar_technology_override().enabled_field_data_analyzed().get("_default_"));
			assertTrue("Default field settings for non-analyzied fields", null != config_bean.columnar_technology_override().enabled_field_data_notanalyzed().get("_default_"));
			assertEquals(ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean.CollidePolicy.new_type, config_bean.search_technology_override().collide_policy());
			assertTrue("Search Settings", null != config_bean.search_technology_override().settings());
			assertTrue("Search Settings", config_bean.search_technology_override().settings().size() > 0);
			
			// Original fields
			assertEquals("localhost:9200", config_bean.elasticsearch_connection());
			assertEquals("test", config_bean.cluster_name());
			
			// Derived fields (temporal)
			assertTrue("Overriden temporal field", !config_bean.temporal_technology_override().enabled());			
												
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
			assertTrue("Default field settings", null != config_bean.columnar_technology_override().enabled_field_data_analyzed());
			assertTrue("Default field settings for analyzed fields", null != config_bean.columnar_technology_override().enabled_field_data_analyzed().get("_default_"));
			assertTrue("Default field settings for non-analyzied fields", null != config_bean.columnar_technology_override().enabled_field_data_notanalyzed().get("_default_"));
			
			// Derived fields (temporal)
			assertTrue("Overriden temporal field", !config_bean.temporal_technology_override().enabled());			
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
			assertTrue("Default field settings", null != config_bean.columnar_technology_override().enabled_field_data_analyzed());
			assertTrue("Default field settings for analyzed fields", null != config_bean.columnar_technology_override().enabled_field_data_analyzed().get("_default_"));
			assertTrue("Default field settings for non-analyzied fields", null != config_bean.columnar_technology_override().enabled_field_data_notanalyzed().get("_default_"));
			
			// Derived fields (temporal)
			assertTrue("Overriden temporal field", !config_bean.temporal_technology_override().enabled());
		}
		
		// Check defaults aren't applied if they are specified (both)
		
		{
			final Map<String, Object> config_map = ImmutableMap.<String, Object>builder()
					.put("ElasticsearchIndexService.elasticsearch_connection", "localhost:9200")
					.put("ElasticsearchIndexService.cluster_name", "test")
					.put("ElasticsearchIndexService.columnar_technology_override.test", "test")
					.put("ElasticsearchIndexService.search_technology_override.collide_policy", "error")
					.put("ElasticsearchIndexService.temporal_technology_override.enabled", true)
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
			assertTrue("Default field settings", null != config_bean.columnar_technology_override().enabled_field_data_analyzed());
			assertTrue("Default field settings for analyzed fields", null != config_bean.columnar_technology_override().enabled_field_data_analyzed().get("_default_"));
			assertTrue("Default field settings for non-analyzied fields", null != config_bean.columnar_technology_override().enabled_field_data_notanalyzed().get("_default_"));
			
			// Derived fields (temporal)
			assertTrue("Overriden temporal field", config_bean.temporal_technology_override().enabled());
		}
	}
	
	
	@Test
	public void test_schemaConversion() throws IOException {

		final String bucket_str = Resources.toString(Resources.getResource("com/ikanow/aleph2/search_service/elasticsearch/utils/bucket_schema_test.json"), Charsets.UTF_8);
				
		// All enabled:
		
		{
			final DataBucketBean bucket = BeanTemplateUtils.from(bucket_str, DataBucketBean.class).get();
			
			final ElasticsearchIndexServiceConfigBean schema_config = ElasticsearchIndexConfigUtils.buildConfigBeanFromSchema(bucket, _config, _mapper);
		
			assertEquals(CollidePolicy.error, schema_config.search_technology_override().collide_policy());
			assertTrue("Overrode columnar tech", schema_config.columnar_technology_override().enabled_field_data_analyzed().containsKey("xxx"));
			assertEquals("month", schema_config.temporal_technology_override().exist_age_max());
		}
		
		// disabled
		
		final String bucket_disabled_str = bucket_str.replace("\"enabled\": true", "\"enabled\": false");
		
		{
			final DataBucketBean bucket = BeanTemplateUtils.from(bucket_disabled_str, DataBucketBean.class).get();
			
			final ElasticsearchIndexServiceConfigBean schema_config = ElasticsearchIndexConfigUtils.buildConfigBeanFromSchema(bucket, _config, _mapper);
		
			assertEquals(CollidePolicy.new_type, schema_config.search_technology_override().collide_policy());
			assertTrue("Overrode columnar tech", !schema_config.columnar_technology_override().enabled_field_data_analyzed().containsKey("xxx"));
			assertTrue("Overrode columnar tech", schema_config.columnar_technology_override().enabled_field_data_analyzed().containsKey("_default_"));
			assertEquals(false, schema_config.temporal_technology_override().enabled());
			
		}
	}
}
