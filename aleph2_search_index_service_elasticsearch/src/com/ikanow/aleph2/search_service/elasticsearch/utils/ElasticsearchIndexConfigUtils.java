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
package com.ikanow.aleph2.search_service.elasticsearch.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean.ColumnarSchemaDefaultBean;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchConfigurationBean;
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
			
			final Config default_docs = ConfigFactory.parseResources(Thread.currentThread().getContextClassLoader(), 
					"com/ikanow/aleph2/search_service/elasticsearch/data_model/default_document_schema_override.conf").atPath("document_schema_override");
			
			final Config user_overrides = PropertiesUtils.getSubConfig(global_config, ElasticsearchIndexServiceConfigBean.PROPERTIES_ROOT)
											.orElse(ConfigFactory.empty())
											.withFallback(PropertiesUtils.getSubConfig(global_config, ElasticsearchConfigurationBean.PROPERTIES_ROOT)
													.orElse(ConfigFactory.empty()).root());
			
			return BeanTemplateUtils.from(
					default_templates.withFallback(default_search).withFallback(default_temporal).withFallback(default_docs).entrySet().stream()
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
		
		final SearchIndexSchemaDefaultBean search_index_bits_tmp =
				Optional.ofNullable(bucket.data_schema()).map(DataSchemaBean::search_index_schema)
							.filter(s -> Optional.ofNullable(s.enabled()).orElse(true))
							.map(s -> s.technology_override_schema())
							.map(map -> mapper.convertValue(map, SearchIndexSchemaDefaultBean.class))
						.orElse(backup.search_technology_override());
		
		// The _actual_ settings technology override object is taken from either the bucket or the backup on a top-level-field by top-leve-field basis
		final SearchIndexSchemaDefaultBean search_index_bits =
				BeanTemplateUtils.clone(search_index_bits_tmp)
					//(target index size is a special case)
					.with(SearchIndexSchemaDefaultBean::target_index_size_mb, Optionals.of(() -> bucket.data_schema().search_index_schema().target_index_size_mb()).orElse(backup.search_technology_override().target_index_size_mb()))
					.with(SearchIndexSchemaDefaultBean::collide_policy, Optional.ofNullable(search_index_bits_tmp.collide_policy()).orElse(backup.search_technology_override().collide_policy()))
					.with(SearchIndexSchemaDefaultBean::type_name_or_prefix, Optional.ofNullable(search_index_bits_tmp.type_name_or_prefix()).orElse(backup.search_technology_override().type_name_or_prefix()))
					.with(SearchIndexSchemaDefaultBean::verbose, Optional.ofNullable(search_index_bits_tmp.verbose()).orElse(backup.search_technology_override().verbose()))
					.with(SearchIndexSchemaDefaultBean::settings, Optional.ofNullable(search_index_bits_tmp.settings()).orElse(backup.search_technology_override().settings()))
					.with(SearchIndexSchemaDefaultBean::aliases, Optional.ofNullable(search_index_bits_tmp.aliases()).orElse(backup.search_technology_override().aliases()))
					.with(SearchIndexSchemaDefaultBean::mappings, Optional.ofNullable(search_index_bits_tmp.mappings()).orElse(backup.search_technology_override().mappings()))
					.with(SearchIndexSchemaDefaultBean::mapping_overrides, Optional.ofNullable(search_index_bits_tmp.mapping_overrides()).orElse(backup.search_technology_override().mapping_overrides()))
				.done();
		
		final ColumnarSchemaDefaultBean columnar_bits_tmp =
				Optional.ofNullable(bucket.data_schema()).map(DataSchemaBean::columnar_schema)
							.filter(s -> Optional.ofNullable(s.enabled()).orElse(true))
							.map(s -> s.technology_override_schema())
							.map(map -> mapper.convertValue(map, ColumnarSchemaDefaultBean.class))
						.orElse(backup.columnar_technology_override());

		final ColumnarSchemaDefaultBean columnar_bits =
				BeanTemplateUtils.clone(columnar_bits_tmp)
					.with(ColumnarSchemaDefaultBean::default_field_data_analyzed, Optional.ofNullable(columnar_bits_tmp.default_field_data_analyzed()).orElse(backup.columnar_technology_override().default_field_data_analyzed()))
					.with(ColumnarSchemaDefaultBean::default_field_data_notanalyzed, Optional.ofNullable(columnar_bits_tmp.default_field_data_notanalyzed()).orElse(backup.columnar_technology_override().default_field_data_notanalyzed()))
					.with(ColumnarSchemaDefaultBean::enabled_field_data_analyzed, Optional.ofNullable(columnar_bits_tmp.enabled_field_data_analyzed()).orElse(backup.columnar_technology_override().enabled_field_data_analyzed()))
					.with(ColumnarSchemaDefaultBean::enabled_field_data_notanalyzed, Optional.ofNullable(columnar_bits_tmp.enabled_field_data_notanalyzed()).orElse(backup.columnar_technology_override().enabled_field_data_notanalyzed()))
				.done();		
		
		final DataSchemaBean.TemporalSchemaBean temporal_bits_tmp = Optional.ofNullable(bucket.data_schema()).map(DataSchemaBean::temporal_schema)
																	.filter(s -> Optional.ofNullable(s.enabled()).orElse(true))				
																	.orElse(backup.temporal_technology_override());
		
		final DataSchemaBean.TemporalSchemaBean temporal_bits =
				BeanTemplateUtils.clone(temporal_bits_tmp)
					.with(DataSchemaBean.TemporalSchemaBean::technology_override_schema, Lambdas.get(() -> {
						HashMap<String, Object> tmp = new HashMap<>();
						tmp.putAll(Optional.ofNullable(temporal_bits_tmp.technology_override_schema()).orElse(Collections.emptyMap()));
						tmp.putAll(Optional.ofNullable(backup.temporal_technology_override().technology_override_schema()).orElse(Collections.emptyMap()));
						return tmp;
					}))
				.done();		
		
		return BeanTemplateUtils.build(ElasticsearchIndexServiceConfigBean.class)
				.with(ElasticsearchIndexServiceConfigBean::search_technology_override, search_index_bits)
				.with(ElasticsearchIndexServiceConfigBean::columnar_technology_override, columnar_bits)
				.with(ElasticsearchIndexServiceConfigBean::temporal_technology_override, temporal_bits)
				.with(ElasticsearchIndexServiceConfigBean::document_schema_override, backup.document_schema_override())
				.done().get();
	}
	
}
