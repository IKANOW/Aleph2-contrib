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
package com.ikanow.aleph2.analytics.spark.data_model;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/** Config bean for individual spark jobs
 * @author Alex
 */
public class SparkTopologyConfigBean implements Serializable {
	private static final long serialVersionUID = -6990533978104476468L;
	
	public static final String DEFAULT_SPARK_HOME = "/usr/hdp/current/spark-client/";
	
	/** Guice/Jackson/User c'tor
	 */
	public SparkTopologyConfigBean() {}
	
	/** The class name of the spark job to run
	 * @return
	 */
	public String entry_point() { return entry_point; }
	
	/** Spark job params
	 * @return
	 */
	public Map<String, String> config() { return Optional.ofNullable(config).orElse(Collections.emptyMap()); }

	/** Default Spark system params
	 * @return
	 */
	public Map<String, String> system_config() { return Optional.ofNullable(system_config).orElse(Collections.emptyMap()); }

	/** For scala jobs with interpreters, run this script
	 * @return
	 */
	public String script() { return script; }
	
	private String entry_point;
	private String script;
	private Map<String, String> config;
	private Map<String, String> system_config;
}
