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
package com.ikanow.aleph2.analytics.hadoop.data_model;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Defines a fairly generic Hadoop job - other beans will contain more complex functionality
 * @author alex
 */
public class BasicHadoopConfigBean {
	
	// Some Hadoop configuration fields	
	public final static String CONTEXT_SIGNATURE = "aleph2.job.context_signature";
	public final static String CONFIG_STRING_MAPPER = "aleph2.mapper.config_string";
	public final static String CONFIG_JSON_MAPPER = "aleph2.mapper.config_json";
	public final static String CONFIG_STRING_COMBINER = "aleph2.combiner.config_string";
	public final static String CONFIG_JSON_COMBINER = "aleph2.combiner.config_json";
	public final static String CONFIG_STRING_REDUCER = "aleph2.reducer.config_string";
	public final static String CONFIG_JSON_REDUCER = "aleph2.reducer.config_json";
	
	/** An individual step in the processing chain
	 * @author alex
	 */
	public class Step {
		/** Whether this step is enabled (defaults to true) - note disabling a step might cause the pipeline to fail
		 *  if the types then don't match)
		 * @return
		 */
		public Boolean enabled() { return enabled; }
		
		/** The class of the mapper/combiner/reducer
		 * @return
		 */
		public String entry_point() { return entry_point; }
		
		/** A string that is available to the mapper/combiner/reducer from the Configuration (get(CONFIG_STRING))
		 * @return
		 */
		public String config_string() { return config_string; }
		/** A JSON object that is available to the mapper/combiner/reducer in String form from the Configuration (get(CONFIG_JSON))
		 * @return
		 */
		public Map<String, Object> config_json() { return config_json == null ? config_json : Collections.unmodifiableMap(config_json); }

		/** Ignored except for reducers, sets a target number of reducers to use (Default: 1)
		 * @return
		 */
		public Long num_tasks() { return num_tasks; }

		/** Optional, overrides the class of the output key type (defaults to String)
		 *  (Obviously needs to match both the output class implementation specified by entry point, and the input
		 *   class implementation specified by the next stage)
		 *   Other notes:
		 *   - It is recommended to use fully qualified class names, though they will be inferred where possible
		 *   - Some attempt will be made to convert between types (eg Text <-> JsonNodeWritable <-> MapWritable)
		 * @return
		 */
		public String output_key_type() { return output_key_type; }
		
		/** Optional, overrides the class of the output value type (defaults to String)
		 *  (Obviously needs to match both the output class implementation specified by entry point, and the input
		 *   class implementation specified by the next stage)
		 *   Other notes:
		 *   - It is recommended to use fully qualified class names, though they will be inferred where possible
		 *   - Some attempt will be made to convert between types (eg Text <-> JsonNodeWritable <-> MapWritable)
		 * @return
		 */
		public String output_value_type() { return output_value_type; }

		private Boolean enabled;
		private String config_string;
		private Map<String, Object> config_json;
		private String entry_point;		
		private Long num_tasks;
		private String output_key_type;
		private String output_value_type;
	}
	/** A list of mappers to run in a pipeline. Must be non-empty
	 * @return
	 */
	public List<Step> mappers() { return mappers; }
		
	/** The combiner to apply in between the last mapper and then the reducer
	 *  Optional - if left blank then no combiner is generated, if an (empty object ({}), ie Step.entry_point is null
	 *  then the reducer is used as the combiner
	 * @return
	 */
	public Step combiner() { return combiner; }
	
	/** The reducer to run. If not specified/disabled then no reduction stage are run (the combiner must also
	 *  be not-present/disabled in this case)
	 * @return
	 */
	public Step reducer() { return reducer; }
	
	/** Optional, Mappers to run after the reducer - if the reducer is not run then these finalizers are appended
	 *  to the original mappers
	 * @return
	 */
	public List<Step> finalizers() { return finalizers; }
	
	/** For cases where it is known that an input format is available for the given input key/value types, these can be set
	 *  Otherwise will generate an error when the job is run (some attempt will be made to convert between formats)
	 *  For example, where it is known that the input is an Elasticsearch query then XXX can be used
	 *  (If the key type is overriden then the value must also be and vice versa)
	 * @return 
	 */
	public String input_key_type_override() { return input_key_type_override; }
	/** For cases where it is known that an input format is available for the given input key/value types, these can be set
	 *  Otherwise will generate an error when the job is run (some attempt will be made to convert between formats)
	 *  For example, where it is known that the input is an Elasticsearch query then XXX can be used
	 *  (If the key type is overriden then the value must also be and vice versa)
	 * @return
	 */
	public String input_value_type_override() { return input_value_type_override; }
	
	private List<Step> mappers;
	private Step combiner;
	private Step reducer;
	private List<Step> finalizers;	
	private String input_key_type_override;
	private String input_value_type_override;
}
