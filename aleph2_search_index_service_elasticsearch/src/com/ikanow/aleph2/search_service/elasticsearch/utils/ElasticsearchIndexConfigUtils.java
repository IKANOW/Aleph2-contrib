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

import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/** Configuration utilities for building a configuration bean from the various config elements
 * @author Alex
 */
public class ElasticsearchIndexConfigUtils {

	/** Builds a config bean using the provided defaults where not overridden by the user
	 * @param global_config - the global configuration
	 * @return - a fully filled out config bean
	 */
	public static ElasticsearchIndexServiceConfigBean buildConfigBean(final Config global_config) {
		
		try {
			
			final Config default_search = ConfigFactory.parseResources(Thread.currentThread().getContextClassLoader(), 
					"com/ikanow/aleph2/search_service/elasticsearch/data_model/default_search_index_override.conf").atPath("search_technology_override");
			
			final Config default_templates = ConfigFactory.parseResources(Thread.currentThread().getContextClassLoader(), 
					"com/ikanow/aleph2/search_service/elasticsearch/data_model/default_columnar_override.conf").atPath("columnar_technology_override");
			
			final Config default_temporal = ConfigFactory.parseResources(Thread.currentThread().getContextClassLoader(), 
					"com/ikanow/aleph2/search_service/elasticsearch/data_model/default_temporal_override.conf").atPath("temporal_technology_override");
			
			
			final Config user_overrides = PropertiesUtils.getSubConfig(global_config, ElasticsearchIndexServiceConfigBean.PROPERTIES_ROOT).orElse(ConfigFactory.empty());
			
			return BeanTemplateUtils.from(
					default_templates.withFallback(default_search).withFallback(default_temporal).entrySet().stream()
								.reduce(
										user_overrides, 
										(acc, kv) -> (acc.hasPath(kv.getKey())) ? acc : acc.withValue(kv.getKey(), kv.getValue()), 
										(acc1, acc2) -> acc1.withFallback(acc2) 
										),
								ElasticsearchIndexServiceConfigBean.class);
		}
		catch (Exception e) {
			throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_CONFIG_ERROR,
					ElasticsearchIndexServiceConfigBean.class.toString(),
					PropertiesUtils.getSubConfig(global_config, ElasticsearchIndexServiceConfigBean.PROPERTIES_ROOT).orElse(ConfigFactory.empty())
					), e);				
		}
	}
	
	/** Converts the bucket schema into the config bean
	 * @param bucket
	 * @param backup - provides defaults from the global configuration
	 * @return
	 */
	public static ElasticsearchIndexServiceConfigBean buildConfigBeanFromSchema(final DataBucketBean bucket, final ElasticsearchIndexServiceConfigBean backup, final ObjectMapper mapper) {
		final ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean search_index_bits =
				Optional.ofNullable(bucket.data_schema()).map(DataSchemaBean::search_index_schema)
							.filter(s -> Optional.ofNullable(s.enabled()).orElse(true))
							.map(s -> s.technology_override_schema())
							.map(map -> mapper.convertValue(map, ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean.class))
						.orElse(backup.search_technology_override());
		
		final ElasticsearchIndexServiceConfigBean.ColumnarSchemaDefaultBean columnar_bits =
				Optional.ofNullable(bucket.data_schema()).map(DataSchemaBean::columnar_schema)
							.filter(s -> Optional.ofNullable(s.enabled()).orElse(true))
							.map(s -> s.technology_override_schema())
							.map(map -> mapper.convertValue(map, ElasticsearchIndexServiceConfigBean.ColumnarSchemaDefaultBean.class))
						.orElse(backup.columnar_technology_override());
		
		final DataSchemaBean.TemporalSchemaBean temporal_bits = Optional.ofNullable(bucket.data_schema()).map(DataSchemaBean::temporal_schema)
																	.filter(s -> Optional.ofNullable(s.enabled()).orElse(true))				
																	.orElse(backup.temporal_technology_override());
		
		return BeanTemplateUtils.build(ElasticsearchIndexServiceConfigBean.class)
				.with(ElasticsearchIndexServiceConfigBean::search_technology_override, search_index_bits)
				.with(ElasticsearchIndexServiceConfigBean::columnar_technology_override, columnar_bits)
				.with(ElasticsearchIndexServiceConfigBean::temporal_technology_override, temporal_bits)
				.done().get();
	}
	
}
