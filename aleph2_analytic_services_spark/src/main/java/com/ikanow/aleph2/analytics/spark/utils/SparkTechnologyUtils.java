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
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.analytics.spark.data_model.SparkTopologyConfigBean;

/** Utilities for building spark jobs
 * @author Alex
 */
public class SparkTechnologyUtils {

	public final static String SBT_SUBMIT_BINARY = "bin/spark-submit";
	
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
				.add(SBT_SUBMIT_BINARY)
				.add("--name")
				.add(job_name)
				.add("--class")
				.add(main_clazz)
				.add("--master")
				.add(spark_master)
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
	
	/** Cache the system and user classpaths and return HDFS paths
	 * @param bucket
	 * @param main_jar_path - my JAR path
	 * @param context
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws IllegalArgumentException 
	 */
	public static List<String> getCachedJarList(final DataBucketBean bucket, final String main_jar_path, final IAnalyticsContext context) throws IllegalArgumentException, InterruptedException, ExecutionException, IOException {
	    final FileContext fc = context.getServiceContext().getStorageService().getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
	    final String root_path = context.getServiceContext().getStorageService().getRootPath();
	    
	    // Aleph2 libraries: need to cache them
	    final Stream<String> context_stream = context
	    	.getAnalyticsContextLibraries(Optional.empty())
	    	.stream()
	    	.filter(jar -> !jar.equals(main_jar_path)) // (this is the service case, eg "/opt/aleph2-home/lib/aleph2_spark_analytic_services.jar")
	/**/.filter(jar -> !jar.contains("core_distributed_services"))
	    	.map(f -> new File(f))
	    	.map(f -> Tuples._2T(f, new Path(root_path + "/" + f.getName())))
	    	.map(Lambdas.wrap_u(f_p -> {
	    		final FileStatus fs = Lambdas.get(() -> {
		    		try {
		    			return fc.getFileStatus(f_p._2());
		    		}
		    		catch (Exception e) { return null; }
	    		});
	    		if (null == fs) { //cache doesn't exist
	    			// Local version
	    			try (FSDataOutputStream outer = fc.create(f_p._2(), EnumSet.of(CreateFlag.CREATE), // ie should fail if the destination file already exists 
	    					org.apache.hadoop.fs.Options.CreateOpts.createParent()))
	    			{
	    				Files.copy(f_p._1(), outer.getWrappedStream());
	    			}
	    			catch (FileAlreadyExistsException e) {//(carry on - the file is versioned so it can't be out of date)
	    			}
	    		}
	    		return f_p._2();
	    	}))
	    	.map(p -> "hdfs://" + p.toString())
	    	;
	    
	    // User libraries: this is slightly easier since one of the 2 keys
	    // is the HDFS path (the other is the _id)
	    final Stream<String> lib_stream = context
			.getAnalyticsLibraries(Optional.of(bucket), bucket.analytic_thread().jobs())
			.get()
			.entrySet()
			.stream()
			.map(kv -> kv.getKey())
	    	.filter(jar -> !jar.equals(main_jar_path)) // (this is the uploaded case, eg "/app/aleph2/library/blah.jar")
			.filter(path -> path.startsWith(root_path))
			.map(s -> "hdfs://" + s)
			;	    
			
		return Stream.concat(context_stream, lib_stream).collect(Collectors.toList());
	}
	
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
		
		final LinkedList<String> mutable_errs = new LinkedList<>();
		
		jobs.stream().forEach(job -> {
			if (null == job.config()) {
				mutable_errs.push(ErrorUtils.get(SparkErrorUtils.MISSING_PARAM, new_analytic_bucket.full_name(), job.name(), "config"));
			}
			else {
				final SparkTopologyConfigBean config = BeanTemplateUtils.from(job.config(), SparkTopologyConfigBean.class).get();
				if (null == config.entry_point())
					mutable_errs.push(ErrorUtils.get(SparkErrorUtils.MISSING_PARAM, new_analytic_bucket.full_name(), job.name(), "config.entry_point"));
			}			
			if (MasterEnrichmentType.batch != job.analytic_type())
				mutable_errs.push(ErrorUtils.get(SparkErrorUtils.CURRENTLY_BATCH_ONLY, new_analytic_bucket.full_name(), job.name()));
		});
		
		return ErrorUtils.buildMessage(mutable_errs.isEmpty(), SparkTechnologyUtils.class, "validateJobs", mutable_errs.stream().collect(Collectors.joining(";")));
	}

}
