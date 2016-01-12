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

/** Config bean for available technology overrides (in the enrichment metadata control bean)
 * @author Alex
 */
public class GlobalSparkConfigBean implements Serializable {
	private static final long serialVersionUID = -6990533978104476468L;
	
	public static final String DEFAULT_SPARK_HOME = "/usr/hdp/current/spark-client/";
	
	/** Guice/Jackson/User c'tor
	 */
	public GlobalSparkConfigBean() {}
	
	/** The spark home location (defaults to HDP)
	 * @return
	 */
	public String spark_home() { return Optional.ofNullable(spark_home).orElse(DEFAULT_SPARK_HOME); }
	
	/** Default Spark params
	 * @return
	 */
	public Map<String, String> config() { return Optional.ofNullable(config).orElse(Collections.emptyMap()); }

	private String spark_home;
	private Map<String, String> config;
}
