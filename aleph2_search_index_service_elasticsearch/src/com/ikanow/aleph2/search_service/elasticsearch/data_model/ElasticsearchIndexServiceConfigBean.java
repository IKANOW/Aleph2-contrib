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
package com.ikanow.aleph2.search_service.elasticsearch.data_model;

import java.util.Map;

import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchConfigurationBean;

/** The configuration bean for the Elasticsearch Index Service
 * @author Alex
 *
 */
public class ElasticsearchIndexServiceConfigBean extends ElasticsearchConfigurationBean {

	final public static String PROPERTIES_ROOT = "ElasticsearchIndexService";
	
	protected ElasticsearchIndexServiceConfigBean() {}
	
	public SearchIndexSchemaDefaultBean search_technology_override() { return search_technology_override; }
	public ColumnarSchemaDefaultBean columnar_technology_override() { return columnar_technology_override; }
	public DataSchemaBean.TemporalSchemaBean temporal_technology_override() { return temporal_technology_override; }

	private SearchIndexSchemaDefaultBean search_technology_override;
	private ColumnarSchemaDefaultBean columnar_technology_override;
	private DataSchemaBean.TemporalSchemaBean temporal_technology_override;
	
	// Can only construct this from a config element
	
	public static class SearchIndexSchemaDefaultBean {
		public CollidePolicy collide_policy() { return collide_policy; }
		public String type_name_or_prefix() { return type_name_or_prefix; }
		public Boolean verbose() { return verbose; };
		public Map<String, Object> settings() { return settings; }
		public Map<String, Object> mappings() { return mappings; }
		public Map<String, Map<String, Object>> mapping_overrides() { return mapping_overrides; }
		
		public enum CollidePolicy { error, new_type };
		private CollidePolicy collide_policy;
		private Boolean verbose;
		private String type_name_or_prefix;
		private Map<String, Object> settings;
		private Map<String, Object> mappings;
		private Map<String, Map<String, Object>> mapping_overrides;
	}
	public static class ColumnarSchemaDefaultBean {
		// the contents of the "fielddata" sub-object of a property for which an include has been specifed, takes from "_default", or field_type if the field_type can be inferred
		public Map<String, Map<String, Object>> enabled_field_data_analyzed() { return enabled_field_data_analyzed; }		
		public Map<String, Map<String, Object>> enabled_field_data_notanalyzed() { return enabled_field_data_notanalyzed; }		
		private Map<String, Map<String, Object>> enabled_field_data_analyzed;
		private Map<String, Map<String, Object>> enabled_field_data_notanalyzed;

		// the contents of the "fielddata" sub-object of a property for which no include/exclude has been specifed, takes from "_default", or field_type if the field_type can be inferred
		public Map<String, Map<String, Object>> default_field_data_analyzed() { return default_field_data_analyzed; }		
		public Map<String, Map<String, Object>> default_field_data_notanalyzed() { return default_field_data_notanalyzed; }		
		private Map<String, Map<String, Object>> default_field_data_analyzed;
		private Map<String, Map<String, Object>> default_field_data_notanalyzed;
				
		@SuppressWarnings("unused")
		private String test; // (just used for testing)
	}
}
