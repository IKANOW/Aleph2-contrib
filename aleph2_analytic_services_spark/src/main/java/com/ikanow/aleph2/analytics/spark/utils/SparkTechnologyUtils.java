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
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import kafka.serializer.StringDecoder;

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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;
import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import com.ikanow.aleph2.core.shared.utils.BatchRecordUtils;
import com.ikanow.aleph2.core.shared.utils.JarBuilderUtil;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputConfigBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.analytics.hadoop.utils.HadoopBatchEnrichmentUtils;
import com.ikanow.aleph2.analytics.hadoop.utils.HadoopTechnologyUtils;
import com.ikanow.aleph2.analytics.spark.assets.BeFileInputFormat_Pure;
import com.ikanow.aleph2.analytics.spark.data_model.SparkTopologyConfigBean;
import com.ikanow.aleph2.analytics.spark.data_model.SparkTopologyConfigBean.SparkType;
import com.ikanow.aleph2.analytics.spark.services.SparkTechnologyService;

/** Utilities for building spark jobs
 * @author Alex
 */
/**
 * @author Alex
 *
 */
public class SparkTechnologyUtils {
	private static final Logger _logger = LogManager.getLogger(SparkTechnologyUtils.class);
	private static final ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());

	public final static String SBT_SUBMIT_BINARY = "bin/spark-submit";
	
	public static interface SparkSqlAccessContext extends IAnalyticsAccessContext<DataFrame> {}
	
	/** Creates a command line call to launch spark
	 * @param spark_home
	 * @param yarn_home
	 * @param spark_master
	 * @param main_clazz
	 * @param context_signature
	 * @param main_jar
	 * @param other_jars
	 * @param spark_job_options
	 * @param spark_system_options
	 */
	public static ProcessBuilder createSparkJob(
			final String job_name,
			final String spark_home,
			final String yarn_home,
			final String spark_master,
			final Optional<String> maybe_main_clazz,
			final String context_signature,
			final Optional<String> test_signature,
			final String main_jar_or_py,
			final Collection<String> other_jars,
			final Collection<String> other_files,
			final Collection<String> other_lang_files,
			final List<String> external_jars,
			final List<String> external_files,
			final List<String> external_lang_files,
			final Optional<Map<String, Object>> spark_generic_options,
			final Map<String, String> spark_job_options,
			final Map<String, String> spark_system_options
			
			)
	{
		//https://spark.apache.org/docs/1.2.0/submitting-applications.html
		
		final List<String> command_line =
			ImmutableList.<String>builder()
				.add(SBT_SUBMIT_BINARY)
				.add("--name")
				.add(job_name)
				.addAll(maybe_main_clazz.map(main_clazz -> Arrays.asList("--class", main_clazz)).orElse(Collections.emptyList()))
				.add("--master")
				.add(spark_master)
				.add("--jars")
				.add(Stream.concat(other_jars.stream(), external_jars.stream()).collect(Collectors.joining(",")))
				.addAll( Optional.of(Stream.concat( other_files.stream(), external_files.stream())
											.collect(Collectors.joining(","))).filter(s -> !s.isEmpty()).map(s -> Arrays.asList("--files", s))
								.orElse(Collections.emptyList()))									
				//TODO (ALEPH-63): handle R in the example below
				.addAll( Optional.of(Stream.concat( other_lang_files.stream(), external_lang_files.stream())
											.collect(Collectors.joining(","))).filter(s -> !s.isEmpty()).map(s -> Arrays.asList("--py-files", s))
								.orElse(Collections.emptyList()))									
				.addAll(Optional.ofNullable(System.getProperty("hdp.version")).map(hdp_version -> { // Set HDP version from whatever I'm set to
					return (List<String>)ImmutableList.<String>of(
							"--conf", "spark.executor.extraJavaOptions=-Dhdp.version=" + hdp_version,
							"--conf", "spark.driver.extraJavaOptions=-Dhdp.version=" + hdp_version,
							"--conf", "spark.yarn.am.extraJavaOption=-Dhdp.version=" + hdp_version
							);
					})
					.orElse(Collections.emptyList())
				)
				.addAll(spark_job_options.isEmpty()
						? Collections.emptyList()
						: 							
						spark_job_options.entrySet().stream().flatMap(kv -> Stream.of("--conf", kv.getKey() + "=" + kv.getValue())).collect(Collectors.toList())
					)
				.addAll(spark_system_options.entrySet().stream().flatMap(kv -> Stream.of(kv.getKey(), kv.getValue())).collect(Collectors.toList()))
				.addAll(
					spark_generic_options.map(opts -> 
							Arrays.asList("--conf", 
									SparkTopologyConfigBean.JOB_CONFIG_KEY + "=" + BeanTemplateUtils.configureMapper(Optional.empty()).convertValue(opts, JsonNode.class))
						)
						.orElse(Collections.emptyList())
					)
				.add(main_jar_or_py)				
				.add(context_signature)
				.addAll(test_signature.map(ts -> Arrays.asList(ts)).orElse(Collections.emptyList()))
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
	    		
	    		final Tuple3<File, Path, FileStatus> f_p_fs = f_str.contains("core_distributed_services") || f_str.contains("data_model")
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
	    	.map(p -> transformFromPath(p.toString()))
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
			.map(s -> transformFromPath(s))
			;	    
			
		return Stream.concat(context_stream, lib_stream).collect(Collectors.toList());
	}
	
	/** Just provides a single mechanism for transforming from a key to an HDFS path, so it can be used centrally 
	 * @param path
	 * @return
	 */
	public static String transformFromPath(final String path) {
		return "hdfs://" + path;
	}
	
	/** Takes the command line args and builds the Aleph2 objects required to set up Spark within Aleph2 
	 * @param args
	 * @return
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static Tuple2<IAnalyticsContext, Optional<ProcessingTestSpecBean>> initializeAleph2(final String args[]) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		final IAnalyticsContext context = ContextUtils.getAnalyticsContext(new String(Base64.getDecoder().decode(args[0].getBytes())));
		
		final Optional<ProcessingTestSpecBean> test_spec =
				Optional.of(args).filter(a -> a.length > 1).map(a -> BeanTemplateUtils.from(new String(Base64.getDecoder().decode(args[1].getBytes())), ProcessingTestSpecBean.class).get());
		
		return Tuples._2T(context, test_spec);
	}
	
	/** Finalizes the aleph2 job - shouldn't be necessary to call, each individual context should call this
	 * @param context
	 */
	public static void finalizeAleph2(final IAnalyticsContext context) {
		context.flushBatchOutput(Optional.empty(), context.getJob().get());		
	}
	
	/** Optional utility to respect the test spec's timeout
	 * @param maybe_test_spec
	 * @param on_timeout - mainly for testing
	 */
	public static void registerTestTimeout(final Optional<ProcessingTestSpecBean> maybe_test_spec, Runnable on_timeout) {
		
		maybe_test_spec.map(test_spec -> test_spec.max_run_time_secs()).ifPresent(max_run_time -> {
			CompletableFuture.runAsync(Lambdas.wrap_runnable_u(() -> {
				Thread.sleep(1500L*max_run_time); // (seconds, *1.5 for safety)
				System.out.println("Test timeout - exiting");
				on_timeout.run();
			}));
		});
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
	
	/** Util function to tidy up input bean
	 * @param in
	 * @param maybe_test_spec
	 * @return
	 */
	protected static Stream<AnalyticThreadJobInputBean> transformInputBean(
			final Stream<AnalyticThreadJobInputBean> in,
			final Optional<ProcessingTestSpecBean> maybe_test_spec)
	{
	    final Optional<Long> debug_max =  maybe_test_spec.map(t -> t.requested_num_objects());
		
		return in.filter(input -> Optional.ofNullable(input.enabled()).orElse(true))
				.map(Lambdas.wrap_u(input -> {
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
					
					return input_with_test_settings;
				}));		
	}
	
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
			final Optional<ProcessingTestSpecBean> maybe_test_spec,
			final Configuration config,
			final Set<String> exclude_names,
			BiConsumer<AnalyticThreadJobInputBean, Job> per_input_action
			)
	{		
	    transformInputBean(Optionals.ofNullable(job.inputs()).stream(), maybe_test_spec)
			.filter(input -> !exclude_names.contains(input.name()))
			.forEach(Lambdas.wrap_consumer_u(input_with_test_settings -> {
				
				final Optional<IBucketLogger> a2_logger = Optional.ofNullable(context.getLogger(Optional.of(bucket)));
				
				final List<String> paths = context.getInputPaths(Optional.empty(), job, input_with_test_settings);
				
				if (!paths.isEmpty()) {
				
					_logger.info(ErrorUtils.get("Adding storage paths for bucket {0}: {1}", bucket.full_name(), paths.stream().collect(Collectors.joining(";"))));
	
					a2_logger.ifPresent(l -> l.log(Level.INFO, true, 
							() -> ErrorUtils.get("Adding storage paths for bucket {0}: {1}", bucket.full_name(), paths.stream().collect(Collectors.joining(";"))),
							() -> SparkTechnologyService.class.getSimpleName() + "." + Optional.ofNullable(job.name()).orElse("no_name"), 
							() -> "startAnalyticJobOrTest"));
					
					//DEBUG
					//System.out.println(ErrorUtils.get("Adding storage paths for bucket {0}: {1}", bucket.full_name(), paths.stream().collect(Collectors.joining(";"))));	
					
				    final Job input_job = Job.getInstance(config);
				    input_job.setInputFormatClass(BeFileInputFormat_Pure.class);				
					paths.stream().forEach(Lambdas.wrap_consumer_u(path -> FileInputFormat.addInputPath(input_job, new Path(path))));
					// (Add the input config in)
					input_job.getConfiguration().set(HadoopBatchEnrichmentUtils.BE_BUCKET_INPUT_CONFIG, BeanTemplateUtils.toJson(input_with_test_settings).toString());
					per_input_action.accept(input_with_test_settings, input_job);
				}
				else { // not easily available in HDFS directory format, try getting from the context
					
					Optional<HadoopBatchEnrichmentUtils.HadoopAccessContext> input_format_info = context.getServiceInput(HadoopBatchEnrichmentUtils.HadoopAccessContext.class, Optional.empty(), job, input_with_test_settings);
					if (!input_format_info.isPresent()) {
						_logger.warn(ErrorUtils.get("Tried but failed to get input format from {0}", BeanTemplateUtils.toJson(input_with_test_settings)));
	
						a2_logger.ifPresent(l -> l.log(Level.WARN, true, 
								() -> ErrorUtils.get("Tried but failed to get input format from {0}", BeanTemplateUtils.toJson(input_with_test_settings)),
								() -> SparkTechnologyService.class.getSimpleName() + "." + Optional.ofNullable(job.name()).orElse("no_name"), 
								() -> "startAnalyticJobOrTest"));						
						
						//DEBUG
						//System.out.println(ErrorUtils.get("Tried but failed to get input format from {0}", BeanTemplateUtils.toJson(input_with_test_settings)));
					}
					else {
						_logger.info(ErrorUtils.get("Adding data service path for bucket {0}: {1}", bucket.full_name(), input_format_info.get().describe()));
	
						a2_logger.ifPresent(l -> l.log(Level.INFO, true, 
								() -> ErrorUtils.get("Adding data service path for bucket {0}: {1}", bucket.full_name(), input_format_info.get().describe()),
								() -> SparkTechnologyService.class.getSimpleName() + "." + Optional.ofNullable(job.name()).orElse("no_name"), 
								() -> "startAnalyticJobOrTest"));
						
						//DEBUG
						//System.out.println(ErrorUtils.get("Adding data service path for bucket {0}: {1}", bucket.full_name(),input_format_info.get().describe()));
						
					    final Job input_job = Job.getInstance(config);
					    input_job.setInputFormatClass(input_format_info.get().getAccessService().either(l -> l.getClass(), r -> r));
					    input_format_info.get().getAccessConfig().ifPresent(map -> {
					    	map.entrySet().forEach(kv -> input_job.getConfiguration().set(kv.getKey(), kv.getValue().toString()));
					    });							    
						per_input_action.accept(input_with_test_settings, input_job);
					}
				}
			}))
			;			
	}
	
	/** Builds a map of streaming spark inputs
	 * @param context
	 * @param maybe_test_spec
	 * @param streaming_context
	 * @param exclude_names
	 * @return
	 */
	public static Multimap<String, JavaPairDStream<String, Tuple2<Long, IBatchRecord>>> buildStreamingSparkInputs(
			final IAnalyticsContext context, 
			final Optional<ProcessingTestSpecBean> maybe_test_spec,
			final JavaStreamingContext streaming_context,
			final Set<String> exclude_names
			)
	{
		final AnalyticThreadJobBean job = context.getJob().get();
		
		final Multimap<String, JavaPairDStream<String, Tuple2<Long, IBatchRecord>>> mutable_builder = HashMultimap.create();
		
	    transformInputBean(Optionals.ofNullable(job.inputs()).stream(), maybe_test_spec)
	    	.filter(job_input -> !exclude_names.contains(job_input.name()))
	    	.forEach(job_input -> {
	    		final List<String> topics = context.getInputTopics(context.getBucket(), job, job_input);
	    		final JavaPairInputDStream<String, String> k_stream = 
	    				KafkaUtils.createDirectStream(streaming_context, String.class, String.class, 
	    						StringDecoder.class, StringDecoder.class, 
	    						com.ikanow.aleph2.distributed_services.utils.KafkaUtils.getProperties(), 
	    						ImmutableSet.<String>builder().addAll(topics).build());
	    		
	    		mutable_builder.put(job_input.name(), k_stream.mapToPair(t2 -> Tuples._2T(t2._1(), Tuples._2T(0L, new BatchRecordUtils.JsonBatchRecord(_mapper.readTree(t2._2()))))));
	    	});
		
		return mutable_builder;
	}
	
	/** Builds a multimap of named SQL inputs
	 * @param context the analytic context retrieved from the 
	 * @return A multi map of Java RDDs against name (with input name built as resource_name:data_service if not present)
	 */
	@SuppressWarnings("unchecked")
	public static Multimap<String, DataFrame> buildBatchSparkSqlInputs(
			final IAnalyticsContext context, 
			final Optional<ProcessingTestSpecBean> maybe_test_spec,
			final SQLContext spark_sql_context,
			final Set<String> exclude_names
			)
	{		
		final AnalyticThreadJobBean job = context.getJob().get();
		
		final Multimap<String, DataFrame> mutable_builder = HashMultimap.create();
		
	    transformInputBean(Optionals.ofNullable(job.inputs()).stream(), maybe_test_spec)
			.filter(input -> !exclude_names.contains(input.name()))
			.forEach(Lambdas.wrap_consumer_u(input -> {
				Optional<SparkSqlAccessContext> maybe_input_format_info = context.getServiceInput(SparkSqlAccessContext.class, Optional.empty(), job, input);
				maybe_input_format_info
					.flatMap(input_format_info -> input_format_info.getAccessConfig())
					.map(info_objects -> (Function<SQLContext, DataFrame>)info_objects.get(input.name()))
					.ifPresent(rdd_getter -> {
						mutable_builder.put(input.name(), rdd_getter.apply(spark_sql_context));
					});
			}))
			;

		return mutable_builder;
	}
	
	/** Builds a multimap of named inputs
	 * @param context the analytic context retrieved from the 
	 * @return A multi map of Java RDDs against name (with input name built as resource_name:data_service if not present)
	 */
	public static Multimap<String, JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>> buildBatchSparkInputs(
			final IAnalyticsContext context, 
			final Optional<ProcessingTestSpecBean> maybe_test_spec,
			final JavaSparkContext spark_context,
			final Set<String> exclude_names
			)
	{		
		final DataBucketBean bucket = context.getBucket().get();
		final AnalyticThreadJobBean job = context.getJob().get();
		
		final Configuration config = HadoopTechnologyUtils.getHadoopConfig(context.getServiceContext().getGlobalProperties());
		config.set(HadoopBatchEnrichmentUtils.BE_BUCKET_SIGNATURE, BeanTemplateUtils.toJson(bucket).toString());
		maybe_test_spec.ifPresent(test_spec -> Optional.ofNullable(test_spec.requested_num_objects()).ifPresent(num -> config.set(HadoopBatchEnrichmentUtils.BE_DEBUG_MAX_SIZE, Long.toString(num))));
		
		final Multimap<String, JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>> mutable_builder = HashMultimap.create();
		
		buildAleph2Inputs(context, bucket, job, maybe_test_spec, config, exclude_names,
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
		
		//TODO (ALEPH-63): validate batch enrichment				
		
		final LinkedList<String> mutable_errs = new LinkedList<>();
		
		jobs.stream().forEach(job -> {
			if (null == job.config()) {
				mutable_errs.push(ErrorUtils.get(SparkErrorUtils.MISSING_PARAM, new_analytic_bucket.full_name(), job.name(), "config"));
			}
			else {
				final SparkTopologyConfigBean config = BeanTemplateUtils.from(job.config(), SparkTopologyConfigBean.class).get();
				if (SparkType.jvm == Optional.ofNullable(config.language()).orElse(SparkType.jvm)) { // JVM validation
					if (null == config.entry_point()) {
						mutable_errs.push(ErrorUtils.get(SparkErrorUtils.MISSING_PARAM, new_analytic_bucket.full_name(), job.name(), "config.entry_point"));
					}
				}
				else if (SparkType.python == Optional.ofNullable(config.language()).orElse(SparkType.jvm)) { // JVM validation
					if ((null == config.entry_point()) && (null == config.script())) {
						mutable_errs.push(ErrorUtils.get(SparkErrorUtils.MISSING_PARAM, new_analytic_bucket.full_name(), job.name(), "config.entry_point|config.script"));
					}
				}
			}			
		});
		
		return ErrorUtils.buildMessage(mutable_errs.isEmpty(), SparkTechnologyUtils.class, "validateJobs", mutable_errs.stream().collect(Collectors.joining(";")));
	}

}
