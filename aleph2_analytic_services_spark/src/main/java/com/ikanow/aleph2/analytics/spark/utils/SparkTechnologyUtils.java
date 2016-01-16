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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import com.ikanow.aleph2.core.shared.utils.JarBuilderUtil;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputConfigBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.analytics.hadoop.services.BeJobLauncher.HadoopAccessContext;
import com.ikanow.aleph2.analytics.hadoop.utils.HadoopTechnologyUtils;
import com.ikanow.aleph2.analytics.spark.assets.BeFileInputFormat_Pure;
import com.ikanow.aleph2.analytics.spark.data_model.SparkTopologyConfigBean;

/** Utilities for building spark jobs
 * @author Alex
 */
public class SparkTechnologyUtils {
	private static final Logger _logger = LogManager.getLogger(SparkTechnologyUtils.class);

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
		final String tmp_dir = System.getProperty("java.io.tmpdir");
	    
	    // Aleph2 libraries: need to cache them
	    final Stream<String> context_stream = context
	    	.getAnalyticsContextLibraries(Optional.empty())
	    	.stream()
	    	.filter(jar -> !jar.equals(main_jar_path)) // (this is the service case, eg "/opt/aleph2-home/lib/aleph2_spark_analytic_services.jar")
	    	.map(Lambdas.wrap_u(f_str -> {
	    		
	    		final Tuple3<File, Path, FileStatus> f_p_fs = f_str.contains("core_distributed_services")
	    				? removeSparkConflictsAndCache(f_str, root_path, fc)
	    				: checkCache(f_str, root_path, fc)
	    				;
	    		
	    		if (null == f_p_fs._3()) { //cache doesn't exist
	    			// Local version
	    			try (FSDataOutputStream outer = fc.create(f_p_fs._2(), EnumSet.of(CreateFlag.CREATE), // ie should fail if the destination file already exists 
	    					org.apache.hadoop.fs.Options.CreateOpts.createParent()))
	    			{
	    				Files.copy(f_p_fs._1(), outer.getWrappedStream());
	    			}
	    			catch (FileAlreadyExistsException e) {//(carry on - the file is versioned so it can't be out of date)
	    			}
		    		if (f_p_fs._1().getPath().startsWith(tmp_dir)) { // (delete tmp files)
		    			f_p_fs._1().delete();
		    		}
	    		}
	    		
	    		return f_p_fs._2();
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
	
	/** Checks if an aleph2 JAR file is cached
	 * @param local_file
	 * @param root_path
	 * @param fc
	 * @return
	 */
	public static Tuple3<File, Path, FileStatus> checkCache(final String local_file, final String root_path, final FileContext fc) {
		final File f = new File(local_file);
		final Tuple2<File, Path> f_p =  Tuples._2T(f, new Path(root_path + "/" + f.getName()));
		final FileStatus fs = Lambdas.get(() -> {
    		try {
    			return fc.getFileStatus(f_p._2());
    		}
    		catch (Exception e) { return null; }
		});
		return Tuples._3T(f_p._1(), f_p._2(), fs);
	}
	
	/**Checks if an aleph2 JAR file with bad internal JARs is cached, does some excising if not
	 * @param local_file
	 * @param root_path
	 * @param fc
	 * @return
	 * @throws IOException
	 */
	public static Tuple3<File, Path, FileStatus> removeSparkConflictsAndCache(final String local_file, final String root_path, final FileContext fc) throws IOException {
		final String renamed_local_file = local_file.replaceFirst(".jar", "_sparkVersion.jar");
		final File f = new File(renamed_local_file);
		final Tuple2<File, Path> f_p =  Tuples._2T(f, new Path(root_path + "/" + f.getName()));
		final FileStatus fs = Lambdas.get(() -> {
    		try {
    			return fc.getFileStatus(f_p._2());
    		}
    		catch (Exception e) { return null; }
		});
		if (null == fs) { //cache doesn't exist
			// Create a new copy without akka in tmp
			File f_tmp = File.createTempFile("aleph2-temp", ".jar");
			JarBuilderUtil.mergeJars(Arrays.asList(local_file), f_tmp.toString(), 
					ImmutableSet.of("akka", "scala", "com/typesafe/conf", "reference.conf", //(these definitely need to be removed)
									"org/jboss/netty",  //(not sure about this, can't imagine i actually need it though? If so try putting back in)
									"org/apache/curator", "org/apache/zookeeper")); //(not sure about these, can try putting them back in if needed)
			
			return Tuples._3T(f_tmp, f_p._2(), null);
		}		
		else return Tuples._3T(f_p._1(), f_p._2(), fs);
	}
	
	//TODO: get processing test spec from args
	
	/** Builds objects for all the aleph2 inputs and provides a method to use them in context-dependent ways 
	 * @param context
	 * @param bucket
	 * @param job
	 * @param config
	 * @param per_input_action - user lambda that determines how they are used
	 */
	public static final void buildAleph2Inputs(
			final IAnalyticsContext context, 
			final DataBucketBean bucket, 
			final AnalyticThreadJobBean job,
			final Configuration config,
			BiConsumer<AnalyticThreadJobInputBean, Job> per_input_action
			)
	{
		
	    final Optional<Long> debug_max =  Optional.empty();
	    
		Optionals.ofNullable(job.inputs())
		.stream()
		.filter(input -> Optional.ofNullable(input.enabled()).orElse(true))
		.forEach(Lambdas.wrap_consumer_u(input -> {
			// In the debug case, transform the input to add the max record limit
			// (also ensure the name is filled in)
			final AnalyticThreadJobInputBean input_with_test_settings = BeanTemplateUtils.clone(input)
					.with(AnalyticThreadJobInputBean::name,
							Optional.ofNullable(input.name()).orElseGet(() -> {
								return Optional.ofNullable(input.resource_name_or_id()).orElse("")
										+ ":" + Optional.ofNullable(input.data_service()).orElse("");
							}))
					.with(AnalyticThreadJobInputBean::config,
							BeanTemplateUtils.clone(
									Optional.ofNullable(input.config())
									.orElseGet(() -> BeanTemplateUtils.build(AnalyticThreadJobInputConfigBean.class)
																		.done().get()
											))
								.with(AnalyticThreadJobInputConfigBean::test_record_limit_request, //(if not test, always null; else "input override" or "output default")
										debug_max.map(max -> Optionals.of(() -> input.config().test_record_limit_request()).orElse(max))
										.orElse(null)
										)
							.done()
							)
					.done();						
			
			final List<String> paths = context.getInputPaths(Optional.empty(), job, input_with_test_settings);
			
			if (!paths.isEmpty()) {
			
				_logger.info(ErrorUtils.get("Adding storage paths for bucket {0}: {1}", bucket.full_name(), paths.stream().collect(Collectors.joining(";"))));
				/**/
				System.out.println(ErrorUtils.get("Adding storage paths for bucket {0}: {1}", bucket.full_name(), paths.stream().collect(Collectors.joining(";"))));

				
			    final Job input_job = Job.getInstance(config);
			    input_job.setInputFormatClass(BeFileInputFormat_Pure.class);				
				paths.stream().forEach(Lambdas.wrap_consumer_u(path -> FileInputFormat.addInputPath(input_job, new Path(path))));
				per_input_action.accept(input, input_job);
			}
			else { // not easily available in HDFS directory format, try getting from the context
				
				Optional<HadoopAccessContext> input_format_info = context.getServiceInput(HadoopAccessContext.class, Optional.empty(), job, input_with_test_settings);
				if (!input_format_info.isPresent()) {
					_logger.warn(ErrorUtils.get("Tried but failed to get input format from {0}", BeanTemplateUtils.toJson(input_with_test_settings)));
					/**/
					System.out.println(ErrorUtils.get("Tried but failed to get input format from {0}", BeanTemplateUtils.toJson(input_with_test_settings)));
				}
				else {
					_logger.info(ErrorUtils.get("Adding data service path for bucket {0}: {1}", bucket.full_name(),
							input_format_info.get().describe()));
					/**/
					System.out.println(ErrorUtils.get("Adding data service path for bucket {0}: {1}", bucket.full_name(),
							input_format_info.get().describe()));
					
				    final Job input_job = Job.getInstance(config);
				    input_job.setInputFormatClass(input_format_info.get().getAccessService().either(l -> l.getClass(), r -> r));
				    input_format_info.get().getAccessConfig().ifPresent(map -> {
				    	map.entrySet().forEach(kv -> input_job.getConfiguration().set(kv.getKey(), kv.getValue().toString()));
				    });							    
					per_input_action.accept(input, input_job);
				}
			}
		}))
		;			
	}
	
	/** Combines all inputs into a single java RDD
	 * (this currently doesn't work because Aleph2 multi input format breaks while serializing - it's not really needed anyway)
	 * @param context
	 * @param spark_context
	 * @return
	 */
//	public static JavaPairRDD<Object, Tuple2<Long, IBatchRecord>> getCombinedInput(final IAnalyticsContext context, final JavaSparkContext spark_context) {
//		
//	    //TODO test spec
////	    		testSpec.flatMap(testSpecVals -> 
////	    							Optional.ofNullable(testSpecVals.requested_num_objects()));
////	    //then gets applied to all the inputs:
////	    debug_max.ifPresent(val -> config.set(BatchEnrichmentJob.BE_DEBUG_MAX_SIZE, val.toString()));
//				
//		final Configuration config = HadoopTechnologyUtils.getHadoopConfig(context.getServiceContext().getGlobalProperties());
//		
//		//(these exist by construction)
//		final DataBucketBean bucket = context.getBucket().get();
//		final AnalyticThreadJobBean job = context.getJob().get();
//		
//		//OK this doesn't work because of the over complex split functionality used
//		
//	    final Aleph2MultiInputFormatBuilder input_builder = new Aleph2MultiInputFormatBuilder();
//		
//		buildAleph2Inputs(context, bucket, job, config, (input, input_job) -> input_builder.addInput(UuidUtils.get().getRandomUuid(), input_job));		
//		
//		final Job dummy_hadoop_job = Lambdas.wrap_u(() -> Job.getInstance(config, "dummy")).get();
//	    input_builder.build(dummy_hadoop_job);
//
//	    //DEBUG
//	    //System.out.println("CONFIG: " + Optionals.streamOf(dummy_hadoop_job.getConfiguration().iterator(), false).map(kv -> kv.getKey() + " = " + kv.getValue()).collect(Collectors.joining("\n")));
//	    
//	    @SuppressWarnings("unchecked")
//		JavaPairRDD<Object, Tuple2<Long, IBatchRecord>> ret_val =
//	    		spark_context.newAPIHadoopRDD(dummy_hadoop_job.getConfiguration(), Aleph2MultiInputFormat.class, String.class, Tuple2.class);
//	    
//	    return ret_val;
//	}
	
	//TODO: have a getSparkSql
	
	/** Builds a multimap of named inputs
	 * @param context the analytic context retrieved from the 
	 * @return A multi map of Java RDDs against name (with input name built as resource_name:data_service if not present)
	 */
	public static Multimap<String, JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>> buildBatchSparkInputs(final IAnalyticsContext context, final JavaSparkContext spark_context) {
		
		final DataBucketBean bucket = context.getBucket().get();
		final AnalyticThreadJobBean job = context.getJob().get();
		
		final Configuration config = HadoopTechnologyUtils.getHadoopConfig(context.getServiceContext().getGlobalProperties());
		
		Multimap<String, JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>> mutable_builder = HashMultimap.create();
		
		buildAleph2Inputs(context, bucket, job, config, 
				(input, input_job) -> {
					try {
						@SuppressWarnings("unchecked")
						final JavaPairRDD<Object, Tuple2<Long, IBatchRecord>> rdd = 
								spark_context.newAPIHadoopRDD(input_job.getConfiguration(), 
										(Class<InputFormat<Object, Tuple2<Long, IBatchRecord>>>)input_job.getInputFormatClass(), 
										Object.class, 
										(Class<Tuple2<Long, IBatchRecord>>)(Class<?>)Tuple2.class);
						
						mutable_builder.put(input.name(), rdd);
					}
					catch (Throwable t) {
						System.out.println(ErrorUtils.getLongForm("ERROR: building aleph2 inputs: {0}", t));
					}
				});
		
		return mutable_builder;
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
