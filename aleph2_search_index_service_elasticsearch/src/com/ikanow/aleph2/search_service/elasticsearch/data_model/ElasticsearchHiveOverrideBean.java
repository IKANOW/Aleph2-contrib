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

package com.ikanow.aleph2.search_service.elasticsearch.data_model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Technology overrides
 * @author Alex
 *
 */
public class ElasticsearchHiveOverrideBean {
	/** List of table overrides (main_table plus views)
	 * @return
	 */
	public Map<String, TableOverride> table_overrides() { return Optional.ofNullable(table_overrides).orElse(Collections.emptyMap()); }

	private Map<String, TableOverride> table_overrides;
	
	/** Schema definition for the different ways that can override beans
	 * @author Alex
	 */
	public static class TableOverride {
		/** A list of types that are included in the table
		 * @return
		 */
		public List<String> types() { return Optional.ofNullable(types).orElse(Collections.emptyList()); }
		
		/** An elasticsearch query in the format "q=<query>"
		 * @return
		 */
		public String url_query() { return url_query; }
		
		/** A standard elasticsearch query object
		 * @return
		 */
		public Map<String, Object> json_query() { return json_query; }

		/** Allows to map between ES field names and hive compatible ones (eg date:@timestamp, ie from the new fieldname to the ES fieldname)
		 * @return
		 */
		public Map<String, String> name_mappings() { return name_mappings; }
		
		private List<String> types;
		private String url_query;
		private Map<String, Object> json_query;
		private Map<String, String> name_mappings;
	}
}
