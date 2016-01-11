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
package com.ikanow.aleph2.analytics.spark.utils;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaRDD;
import org.elasticsearch.common.collect.ImmutableList;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;

/** Utilities for building spark jobs
 * @author Alex
 */
public class SparkTechnologyUtils {

	/** Creates a command line call to launch spark
	 * @param spark_home
	 * @param yarn_home
	 * @param spark_master
	 * @param main_clazz
	 * @param context_signature
	 * @param main_jar
	 * @param other_jars
	 * @param job_options
	 * @param spark_options
	 */
	public static ProcessBuilder createSparkJob(
			final String job_name,
			final String spark_home,
			final String yarn_home,
			final String spark_master,
			final String main_clazz,
			final String context_signature,
			final String main_jar,
			final Collection<String> other_jars,
			final Map<String, String> job_options,
			final Map<String, String> spark_options
			)
	{
		//https://spark.apache.org/docs/1.2.0/submitting-applications.html
		
		final List<String> command_line =
			ImmutableList.<String>builder()
				.add("bin/spark-submit")
				.add("--name")
				.add(job_name)
				.add("--class ")
				.add(main_clazz)
				.add("--master")
				.add("yarn-master")
				.add("--jars")
				.add(other_jars.stream().collect(Collectors.joining(",")))
				.addAll(job_options.isEmpty()
						? Collections.emptyList()
						: 							
						job_options.entrySet().stream().flatMap(kv -> Stream.of("--conf", kv.getKey() + "=" + kv.getValue())).collect(Collectors.toList())
					)
				.addAll(spark_options.entrySet().stream().flatMap(kv -> Stream.of(kv.getKey(), kv.getValue())).collect(Collectors.toList()))
				.add(main_jar)
				.add(context_signature)
				.build()
				;

		final ProcessBuilder pb = new ProcessBuilder();
		
		final Map<String, String> mutable_env = pb.environment();
		mutable_env.put("HADOOP_CONF_DIR", yarn_home);
		
		return pb
			.directory(new File(spark_home))
			.command(command_line)
			;		
	}
	
	//TODO: all the usual JAR building nonsense...
	
	public static Map<String, JavaRDD<Tuple2<Long, IBatchRecord>>> buildBatchSparkInputs(final IAnalyticsContext context) {
		
		//TODO handle HDFS and then aleph2 input stuff
		
		return null;
	}
	
	/** Validate the job
	 * @param new_analytic_bucket
	 * @param jobs
	 * @return
	 */
	public static BasicMessageBean validateJobs(final DataBucketBean new_analytic_bucket, final Collection<AnalyticThreadJobBean> jobs) {
		
		//TODO: currently only batch mode is supported
		//TODO: currently enrichments not supported
		
		final boolean success = true;
		final String message = "";
		return ErrorUtils.buildMessage(success, SparkTechnologyUtils.class, "validateJobs", message);
	}

}
