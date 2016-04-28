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

package com.ikanow.aleph2.analytics.spark.services;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.google.inject.Module;
import com.ikanow.aleph2.analytics.hadoop.utils.HadoopTechnologyUtils;
import com.ikanow.aleph2.analytics.spark.assets.SparkJsInterpreterTopology;
import com.ikanow.aleph2.analytics.spark.data_model.GlobalSparkConfigBean;
import com.ikanow.aleph2.analytics.spark.data_model.SparkTopologyConfigBean;
import com.ikanow.aleph2.analytics.spark.data_model.SparkTopologyConfigBean.SparkType;
import com.ikanow.aleph2.analytics.spark.utils.SparkPyTechnologyUtils;
import com.ikanow.aleph2.analytics.spark.utils.SparkTechnologyUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_import.BucketDiffBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.ProcessUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.SetOnce;

import fj.data.Either;
import fj.data.Validation;

/** Hadoop analytic technology module - provides the interface between Hadoop and Aleph2
 * @author Alex
 */
public class SparkTechnologyService implements IAnalyticsTechnologyService, IExtraDependencyLoader {
	protected static final Logger _logger = LogManager.getLogger();	

	/** Spark currently uses external processes to run, so need to lock to a single node
	 *  Fix this longer term by using direct YARN interface
	 * @param completed_bucket
	 * @param context
	 * @return
	 */
	@Override
	public boolean applyNodeAffinity(final DataBucketBean completed_bucket, final IAnalyticsContext context) {
		return true;
	}
	
	//DEBUG SETTINGS:
	//
	private static Level DEBUG_LEVEL = Level.DEBUG;
	//private static Level DEBUG_LEVEL = Level.INFO;
	
	private Optional<String> DEBUG_LOG_FILE =
			Optional.empty();
			//Optional.of(System.getProperty("java.io.tmpdir"));	
	//(end DEBUG SETTINGS)
	
	protected final SetOnce<GlobalSparkConfigBean> _global_spark_config = new SetOnce<>();
	
	/** This service needs to load some additional classes via Guice. Here's the module that defines the bindings
	 * @return
	 */
	public static List<Module> getExtraDependencyModules() {
		//TODO (ALEPH-63): need to build global spark config from the config if in guice mode
		return Collections.emptyList();
		
	}
	
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		//(done see above)
	}

	@Override
	public void onInit(IAnalyticsContext context) {

		// Set up global config in normal mode
		_global_spark_config.set(
				BeanTemplateUtils.from(Optional.ofNullable(context.getTechnologyConfig().library_config()).orElse(Collections.emptyMap()), GlobalSparkConfigBean.class).get()
				);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#canRunOnThisNode(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public boolean canRunOnThisNode(DataBucketBean analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs, IAnalyticsContext context) {				
		
		// Here's a simple check if someone's made a token effort to install Spark on this node:		
		return new File(_global_spark_config.get().spark_home() + SparkTechnologyUtils.SBT_SUBMIT_BINARY).canExecute();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onNewThread(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext, boolean)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onNewThread(
			DataBucketBean new_analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs, IAnalyticsContext context,
			boolean enabled) {
		
		return CompletableFuture.completedFuture(SparkTechnologyUtils.validateJobs(new_analytic_bucket, jobs));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onUpdatedThread(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, boolean, java.util.Optional, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onUpdatedThread(
			DataBucketBean old_analytic_bucket,
			DataBucketBean new_analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs, boolean is_enabled,
			Optional<BucketDiffBean> diff, IAnalyticsContext context) {
		
		return CompletableFuture.completedFuture(SparkTechnologyUtils.validateJobs(new_analytic_bucket, jobs));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onDeleteThread(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onDeleteThread(
			DataBucketBean to_delete_analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs, IAnalyticsContext context) {
		// Nothing to do here
		return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(this, "onDeleteThread", ""));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#checkCustomTrigger(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public ManagementFuture<Boolean> checkCustomTrigger(
			DataBucketBean analytic_bucket,
			AnalyticThreadComplexTriggerBean trigger, IAnalyticsContext context) {
		// No custom triggers supported
		return FutureUtils.createManagementFuture(
				CompletableFuture.completedFuture(false)
				,
				CompletableFuture.completedFuture(
						Arrays.asList(
							ErrorUtils.buildErrorMessage(this, "checkCustomTrigger", "No custom triggers supported"))
						)
				);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onThreadExecute(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, java.util.Collection, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onThreadExecute(
			DataBucketBean new_analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs,
			Collection<AnalyticThreadComplexTriggerBean> matching_triggers,
			IAnalyticsContext context) {
		// Nothing to do here
		return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(this, "onThreadExecute", ""));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onThreadComplete(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onThreadComplete(
			DataBucketBean completed_analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs, IAnalyticsContext context) {
		// Nothing to do here
		return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(this, "onThreadComplete", ""));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onPurge(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onPurge(
			DataBucketBean purged_analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs, IAnalyticsContext context) {
		// Nothing to do here
		return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(this, "onPurge", ""));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onPeriodicPoll(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onPeriodicPoll(
			DataBucketBean polled_analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs, IAnalyticsContext context) {
		// Nothing to do here
		return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(this, "onPeriodicPoll", ""));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onTestThread(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onTestThread(
			DataBucketBean test_bucket, Collection<AnalyticThreadJobBean> jobs,
			ProcessingTestSpecBean test_spec, IAnalyticsContext context) {
		return CompletableFuture.completedFuture(SparkTechnologyUtils.validateJobs(test_bucket, jobs));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onTestThread(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> startAnalyticJob(
			DataBucketBean analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs,
			AnalyticThreadJobBean job_to_start, IAnalyticsContext context)
	{
		return CompletableFuture.completedFuture(
				startAnalyticJobOrTest(analytic_bucket, jobs, job_to_start, context, Optional.empty()).validation(
						fail -> ErrorUtils.buildErrorMessage
											(this.getClass().getName(), "startAnalyticJob", fail)
						,
						success -> ErrorUtils.buildSuccessMessage
												(this.getClass().getName(), "startAnalyticJob", success.toString())
						));
	}
	
	/**
	 * @param analytic_bucket
	 * @param jobs
	 * @param job_to_start
	 * @param context
	 * @param test_spec
	 * @return
	 */
	public Validation<String, String> startAnalyticJobOrTest(
			DataBucketBean analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs,
			AnalyticThreadJobBean job_to_start, IAnalyticsContext context,
			Optional<ProcessingTestSpecBean> test_spec
			)
	{
		try {
			final GlobalPropertiesBean globals = ModuleUtils.getGlobalProperties();
			final String bucket_signature = BucketUtils.getUniqueSignature(analytic_bucket.full_name(), Optional.of(job_to_start.name()));
			

			final ProcessBuilder pb = serviceLevelProcessBuild(analytic_bucket, jobs, job_to_start, context, test_spec, globals, bucket_signature);
			
			DEBUG_LOG_FILE.map(x -> x + "/" + bucket_signature).ifPresent(name -> {
				pb.inheritIO().redirectErrorStream(true).redirectOutput(new File(name));				
			});
			
			_logger.log(DEBUG_LEVEL, "spark_submit_env = " + pb.environment().toString());						
			_logger.log(DEBUG_LEVEL, "spark_submit_command_line = " + pb.command().stream().collect(Collectors.joining(" ")));						
			
			final String run_path = globals.local_root_dir() + ProcessUtils.DEFAULT_RUN_PATH_SUFFIX;
			
			// (stop the process first, in case it's running...)
			final Tuple2<String, Boolean> stop_res = ProcessUtils.stopProcess(this.getClass().getSimpleName(), analytic_bucket, run_path, Optional.empty());
			
			_logger.log(DEBUG_LEVEL, "stop_job_before_starting = " + stop_res);			
			
			final Tuple2<String, String> err_pid = ProcessUtils.launchProcess(pb, this.getClass().getSimpleName(), analytic_bucket, run_path, Optional.empty());
			// (killing this process doesn't stop the job, so don't bother adding a max time)

			_logger.log(DEBUG_LEVEL, "start_job_results = " + err_pid);						
			
			if (null != err_pid._1()) {
				return Validation.fail("Failed to launch Spark client: " + err_pid._1());
			}
			else {
				return Validation.success("Launched Spark client: " + err_pid._2());
			}
		}
		catch (Throwable e) {
			return Validation.fail(ErrorUtils.getLongForm("startAnalyticJobOrTest: {0}", e));
		}
	}

	/** Separation of logic from startAnalyticJobOrTest to simplify testing
	 * @param analytic_bucket
	 * @param jobs
	 * @param job_to_start
	 * @param context
	 * @param test_spec
	 * @return
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws IllegalArgumentException 
	 */
	public ProcessBuilder serviceLevelProcessBuild(
			final DataBucketBean analytic_bucket,
			final Collection<AnalyticThreadJobBean> jobs,
			final AnalyticThreadJobBean job_to_start, IAnalyticsContext context,
			final Optional<ProcessingTestSpecBean> test_spec,
			final GlobalPropertiesBean globals,
			final String bucket_signature
			) throws IllegalArgumentException, InterruptedException, ExecutionException, IOException
	{
		// Firstly, precalculate the inputs to ensure the right classes are copied across
		final Configuration hadoop_config = HadoopTechnologyUtils.getHadoopConfig(context.getServiceContext().getGlobalProperties());			
		SparkTechnologyUtils.buildAleph2Inputs(context, analytic_bucket, job_to_start, test_spec, hadoop_config, Collections.emptySet(), (input, input_job) -> {}); //(mutate the context)
		
		// Now do the spark stuff:
				
		_logger.log(DEBUG_LEVEL, "spark_home = " + _global_spark_config.get().spark_home());			
		
		final SparkTopologyConfigBean spark_job_config = BeanTemplateUtils.from(job_to_start.config(), SparkTopologyConfigBean.class).get();

		_logger.log(DEBUG_LEVEL, "entry_point = " + spark_job_config.entry_point());			
		
		final String main_jar_or_script = Patterns.match(spark_job_config.language()).<String>andReturn()
				.when(l -> l == SparkType.python, Lambdas.wrap_u(__ ->	{
					// 3 cases:
					if (Optional.ofNullable(spark_job_config.entry_point()).filter(s -> !s.isEmpty()).isPresent()) { // 1) external override set 
						return spark_job_config.entry_point();
					}
					else { // 2+3 internal
						// 2) the internal method points to a python file on the classpath
						if (spark_job_config.script().endsWith(".py")) {
							return SparkPyTechnologyUtils.writeUserPythonScriptTmpFile(bucket_signature, Either.right(spark_job_config.script()));
						}
						else { // 3) script
							return SparkPyTechnologyUtils.writeUserPythonScriptTmpFile(bucket_signature, Either.left(spark_job_config.script()));
						}
					}
				}))
				.when(l -> l == SparkType.r, __ ->	spark_job_config.entry_point())
				.otherwise(__ ->					
					Lambdas.get(() -> {
						try {
							return "hdfs:///" + context.getTechnologyConfig().path_name();
						}
						catch (Exception e) { // (service mode)
							//TODO (ALEPH-63): derive this from "this" using the core shared lib util
							return globals.local_root_dir() + "/lib/aleph2_spark_analytics_services.jar";
						}
					}))
					;

		_logger.log(DEBUG_LEVEL, "main_jars = " + main_jar_or_script);
		
		final List<String> other_resources = SparkTechnologyUtils.getCachedJarList(analytic_bucket, main_jar_or_script, context);
		
		_logger.log(DEBUG_LEVEL, "other_jars = " + other_resources.stream().collect(Collectors.joining(";")));
		
		final Stream<String> aleph2_py_driver = (SparkType.python == spark_job_config.language())
				? Stream.of(SparkPyTechnologyUtils.writeAleph2DriverZip(bucket_signature))
				: Stream.empty()
				;
		final Set<String> requested_files = spark_job_config.uploaded_files().stream().map(s -> SparkTechnologyUtils.transformFromPath(s)).collect(Collectors.toSet());
		final Set<String> requested_lang_files = spark_job_config.uploaded_lang_files().stream().map(s -> SparkTechnologyUtils.transformFromPath(s)).collect(Collectors.toSet());
		
		final ProcessBuilder pb =
				SparkTechnologyUtils.createSparkJob(
						bucket_signature,
						_global_spark_config.get().spark_home(),
						globals.local_yarn_config_dir(), 
						spark_job_config.cluster_mode(),
						Patterns.match(spark_job_config.language()).<Optional<String>>andReturn()
								.when(l -> SparkType.jvm == l, __ -> Optional.ofNullable(spark_job_config.entry_point()))
								.when(l -> SparkType.js == l, __ -> Optional.of(SparkJsInterpreterTopology.class.getName()))
								.otherwise(__ -> Optional.empty()),
						new String(Base64.getEncoder().encode(context.getAnalyticsContextSignature(Optional.of(analytic_bucket), Optional.empty()).getBytes())),
						test_spec.map(ts -> new String(Base64.getEncoder().encode(BeanTemplateUtils.toJson(ts).toString().getBytes()))),
						main_jar_or_script,  
						other_resources.stream().filter(j -> j.endsWith(".jar")).collect(Collectors.toList()), 
						other_resources.stream().filter(f -> requested_files.contains(f)).collect(Collectors.toList()),
						Stream.concat(aleph2_py_driver, other_resources.stream().filter(f -> requested_lang_files.contains(f))).collect(Collectors.toList()),
						spark_job_config.external_jars(),
						spark_job_config.external_files(),
						spark_job_config.external_lang_files(),
						Optional.ofNullable(spark_job_config.job_config()).filter(__ -> spark_job_config.include_job_config_in_spark_config()),
						spark_job_config.spark_config().entrySet().stream().map(kv -> Tuples._2T(kv.getKey().replace(":", "."), kv.getValue())).collect(Collectors.toMap(t2 -> t2._1(), t2 -> t2._2())), 
						spark_job_config.system_config()
						);


		return pb;
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#stopAnalyticJob(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> stopAnalyticJob(
			DataBucketBean analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs,
			AnalyticThreadJobBean job_to_stop, IAnalyticsContext context)
	{
		final GlobalPropertiesBean globals = ModuleUtils.getGlobalProperties();
		final String run_path = globals.local_root_dir() + ProcessUtils.DEFAULT_RUN_PATH_SUFFIX;
		
		final Tuple2<String, Boolean> err_pid = ProcessUtils.stopProcess(this.getClass().getSimpleName(), analytic_bucket, run_path, Optional.empty());
		if (!err_pid._2()) {
			//failed to stop, try to cleanup script file and bail out
			return CompletableFuture.completedFuture(
					ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "stopAnalyticJob", "Error stopping Spark submit script (can result in script continuing to run on server, need to manually kill perhaps): "+err_pid._1, Optional.empty())
					);
		}		
		return CompletableFuture.completedFuture(
				ErrorUtils.buildSuccessMessage(this.getClass().getSimpleName(), "stopAnalyticJob", "Spark submit script stopped.")
				);		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#resumeAnalyticJob(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> resumeAnalyticJob(
			DataBucketBean analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs,
			AnalyticThreadJobBean job_to_resume, IAnalyticsContext context)
	{
		// (no specific resume function, just use start)
		return startAnalyticJob(analytic_bucket, jobs, job_to_resume, context);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#suspendAnalyticJob(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> suspendAnalyticJob(
			DataBucketBean analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs,
			AnalyticThreadJobBean job_to_suspend, IAnalyticsContext context)
	{
		// (no specific suspend function, just use stop)
		return stopAnalyticJob(analytic_bucket, jobs, job_to_suspend, context);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#startAnalyticJobTest(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> startAnalyticJobTest(
			DataBucketBean analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs,
			AnalyticThreadJobBean job_to_test,
			ProcessingTestSpecBean test_spec, IAnalyticsContext context)
	{
		return CompletableFuture.completedFuture(
				startAnalyticJobOrTest(analytic_bucket, jobs, job_to_test, context, Optional.of(test_spec)).validation(
						fail -> ErrorUtils.buildErrorMessage
											(this.getClass().getName(), "startAnalyticJobTest", fail)
						,
						success -> ErrorUtils.buildSuccessMessage
												(this.getClass().getName(), "startAnalyticJobTest", success)
						));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#checkAnalyticJobProgress(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public ManagementFuture<Boolean> checkAnalyticJobProgress(
			DataBucketBean analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs,
			AnalyticThreadJobBean job_to_check, IAnalyticsContext context)
	{
		final GlobalPropertiesBean globals = ModuleUtils.getGlobalProperties();
		final String run_path = globals.local_root_dir() + ProcessUtils.DEFAULT_RUN_PATH_SUFFIX;

		final boolean is_running = ProcessUtils.isProcessRunning(this.getClass().getSimpleName(), analytic_bucket, run_path);
		
		_logger.log(DEBUG_LEVEL, "check_completion_results = " + is_running);						
		
		return FutureUtils.createManagementFuture(CompletableFuture.completedFuture(!is_running));		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Arrays.asList(this);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return Optional.empty();
	}

}
