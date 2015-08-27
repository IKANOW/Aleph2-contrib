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
	public final static String CONTEXT_SIGNATURE = "aleph2.px.context_signature";
	public final static String CONFIG_STRING = "aleph2.px.config_string";
	public final static String CONFIG_STRIGN = "aleph2.px.config_json";
	
	/** An individual step in the processing chain
	 * @author alex
	 */
	public class Step {
		public String entry_point() { return entry_point; }
		
		public String config_string() { return config_string; }
		public Map<String, Object> config_json() { return config_json == null ? config_json : Collections.unmodifiableMap(config_json); }

		public Long num_tasks() { return num_tasks; }

		public String output_key() { return output_key; }
		public String output_value() { return output_value; }
		
		private String config_string;
		private Map<String, Object> config_json;
		private String entry_point;		
		private Long num_tasks;
		private String output_key;
		private String output_value;
	}
	public List<Step> mappers() { return mappers; }
	public Step combiner() { return combiner; }
	public Step reducer() { return reducer; }
	public List<Step> finalizers() { return finalizers; }
	
	private List<Step> mappers;
	private Step combiner;
	private Step reducer;
	private List<Step> finalizers;	
}
