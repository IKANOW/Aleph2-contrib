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
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Config bean for individual spark jobs
 * @author Alex
 */
/**
 * @author Alex
 *
 */
public class SparkTopologyConfigBean implements Serializable {
	private static final long serialVersionUID = -6990533978104476468L;
	
	public static final String DEFAULT_SPARK_HOME = "/usr/hdp/current/spark-client/";
	public static final String DEFAULT_CLUSTER_MODE = "yarn-cluster";
	public static final String JOB_CONFIG_KEY = "spark.aleph2_job_config";
	
	public enum SparkType { r, python, jvm, js }
	
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
	public Map<String, String> spark_config() { return Optional.ofNullable(spark_config).orElse(Collections.emptyMap()); }

	/** An arbitrary JSON object (that is copied into spark_config as "spark.aleph2_job_config" as a JSON string if include_job_config_in_spark_config is true, see below)
	 * @return
	 */
	public Map<String, Object> job_config() { return job_config; }

	/** If true (default) then the job config JSON is added a string as 
	 * @return
	 */
	public Boolean include_job_config_in_spark_config() { return Optional.ofNullable(include_job_config_in_spark_config).orElse(true); }	
	
	/** Default Spark system params
	 * @return
	 */
	public Map<String, String> system_config() { return Optional.ofNullable(system_config).orElse(Collections.emptyMap()); }

	/** For scala jobs with interpreters (or python jobs), run this script
	 * @return
	 */
	public String script() { return script; }
	
	/** DEBUG parameter: can be local, local[K], local[*], yarn-client, or (default:) yarn-cluster
	 *  (see http://spark.apache.org/docs/latest/submitting-applications.html#master-urls)
	 * @return
	 */
	public String cluster_mode() { return Optional.ofNullable(cluster_mode).orElse(DEFAULT_CLUSTER_MODE); }
	
	/** The language of the client spark is going to execute: Spark or R or Python 
	 * @return
	 */
	public SparkType language() { return Optional.ofNullable(language).orElse(SparkType.jvm); }
	
	/** Allows the specification of JARs on the local filesystem (on which the spark job is being executed)
	 * @return
	 */
	public List<String> external_jars() { return Optional.ofNullable(external_jars).orElse(Collections.emptyList()); }
	
	/** Allows the specification of files on the local filesystem (on which the spark job is being executed)
	 * @return
	 */
	public List<String> external_files() { return Optional.ofNullable(external_files).orElse(Collections.emptyList()); }
	
	/** Allows the specification of language specific libraries on the local filesystem (on which the spark job is being executed)
	 * @return
	 */
	public List<String> external_lang_files() { return Optional.ofNullable(external_lang_files).orElse(Collections.emptyList()); }	
	
	/** Allows the specification of files from the shared library 
	 * @return
	 */
	public List<String> uploaded_files() { return Optional.ofNullable(uploaded_files).orElse(Collections.emptyList()); }
	
	/** Allows the specification of language specific libraries from the shared library
	 *  (In JS mode this refers to packages within the specified JARs that are evaluated before any user code, eg "/package.js" which must then
	 *   be in one of the job's JAR files) 
	 * @return
	 */
	public List<String> uploaded_lang_files() { return Optional.ofNullable(uploaded_lang_files).orElse(Collections.emptyList()); }	
	
	private String cluster_mode;
	
	private SparkType language;
	private String entry_point;
	private String script;
	
	private Map<String, String> spark_config;
	private Map<String, String> system_config;
	private Map<String, Object> job_config;
	private Boolean include_job_config_in_spark_config;

	private List<String> uploaded_files;
	private List<String> uploaded_lang_files;	
	
	private List<String> external_jars;
	private List<String> external_files;
	private List<String> external_lang_files;
}
