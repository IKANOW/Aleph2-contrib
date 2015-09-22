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
package com.ikanow.aleph2.analytics.storm.services;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import backtype.storm.generated.StormTopology;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.analytics.storm.assets.PassthroughTopology;
import com.ikanow.aleph2.analytics.storm.data_model.IStormController;
import com.ikanow.aleph2.analytics.storm.modules.StormAnalyticTechnologyModule;
import com.ikanow.aleph2.analytics.storm.utils.StormAnalyticTechnologyUtils;
import com.ikanow.aleph2.analytics.storm.utils.StormControllerUtil;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyService;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingTopology;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_import.BucketDiffBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

import java.util.Arrays;

import scala.Tuple2;

/** Storm analytic technology module - provides the interface between Storm and Aleph2
 * @author Alex
 */
public class StormAnalyticTechnologyService implements IAnalyticsTechnologyService, IExtraDependencyLoader {

	////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////
	
	// GENERAL INITIALIZATION
	
	protected final IStormController _storm_controller;	
	
	/** Guice constructor
	 */
	@Inject
	public StormAnalyticTechnologyService(final IStormController storm_controller) {
		_storm_controller = storm_controller;
	}	

	/** User constructor
	 */
	public StormAnalyticTechnologyService() {
		_storm_controller = StormAnalyticTechnologyModule.getController();
	}
	
	@Override
	public void onInit(final IAnalyticsContext context) {
		//(nothing to do currently)		
	}

	@Override
	public boolean canRunOnThisNode(
					final DataBucketBean analytic_bucket,
					final Collection<AnalyticThreadJobBean> jobs, 
					final IAnalyticsContext context)
	{
		return (null != _storm_controller) && !NoStormController.class.isAssignableFrom(_storm_controller.getClass());
	}

	////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////
	
	// THREAD SPECIFIC CALLBACKS
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onNewThread(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext, boolean)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onNewThread(
												final DataBucketBean new_analytic_bucket,
												final Collection<AnalyticThreadJobBean> jobs, 
												final IAnalyticsContext context,
												final boolean enabled)
	{
		return CompletableFuture.completedFuture(StormAnalyticTechnologyUtils.validateJobs(new_analytic_bucket, jobs));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onUpdatedThread(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, boolean, java.util.Optional, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onUpdatedThread(
												final DataBucketBean old_analytic_bucket,
												final DataBucketBean new_analytic_bucket,
												final Collection<AnalyticThreadJobBean> jobs, 
												final boolean is_enabled,
												final Optional<BucketDiffBean> diff, 
												final IAnalyticsContext context)
	{
		return CompletableFuture.completedFuture(StormAnalyticTechnologyUtils.validateJobs(new_analytic_bucket, jobs));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onDeleteThread(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onDeleteThread(
												final DataBucketBean to_delete_analytic_bucket,
												final Collection<AnalyticThreadJobBean> jobs, 
												final IAnalyticsContext context)
	{
		// Nothing to do here
		return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(this, "onDeleteThread", "(Noted)"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#checkCustomTrigger(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public ManagementFuture<Boolean> checkCustomTrigger(
										final DataBucketBean analytic_bucket,
										final AnalyticThreadComplexTriggerBean trigger, 
										final IAnalyticsContext context)
	{
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
												final DataBucketBean new_analytic_bucket,
												final Collection<AnalyticThreadJobBean> jobs,
												final Collection<AnalyticThreadComplexTriggerBean> matching_triggers,
												final IAnalyticsContext context)
	{
		// Nothing to do here
		return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(this, "onThreadExecute", "(Noted)"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onThreadComplete(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onThreadComplete(
												final DataBucketBean completed_analytic_bucket,
												final Collection<AnalyticThreadJobBean> jobs, 
												final IAnalyticsContext context)
	{
		// Nothing to do here
		return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(this, "onThreadComplete", "(Noted)"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onPurge(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onPurge(
												final DataBucketBean purged_analytic_bucket,
												final Collection<AnalyticThreadJobBean> jobs, 
												final IAnalyticsContext context)
	{
		// Nothing to do here
		return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(this, "onPurge", "(Noted)"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onPeriodicPoll(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onPeriodicPoll(
												final DataBucketBean polled_analytic_bucket,
												final Collection<AnalyticThreadJobBean> jobs, 
												final IAnalyticsContext context)
	{
		// Nothing to do here
		return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(this, "onPeriodicPoll", "(Noted)"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#onTestThread(com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> onTestThread(
												final DataBucketBean test_bucket,
												final Collection<AnalyticThreadJobBean> jobs,
												final ProcessingTestSpecBean test_spec, 
												final IAnalyticsContext context)
	{
		return CompletableFuture.completedFuture(StormAnalyticTechnologyUtils.validateJobs(test_bucket, jobs));
	}

	////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////
	
	// JOB SPECIFIC CALLBACKS
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#startAnalyticJob(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> startAnalyticJob(
												final DataBucketBean analytic_bucket,
												final Collection<AnalyticThreadJobBean> jobs,
												final AnalyticThreadJobBean job_to_start, 
												final IAnalyticsContext context)
	{
		// (job already validated)
		try {
			final Collection<String> user_lib_paths = context.getAnalyticsLibraries(Optional.of(analytic_bucket), jobs).join().values();
			
			final String entry_point = Optional.ofNullable(job_to_start.entry_point()).orElse(PassthroughTopology.class.getName());
			
			//TODO (ALEPH-12): check built-in: eg javascript
			// (other, future, cases: enrichment module format, harvest module format; related, built-in modules: javascript)
			
			//(note this only works because the analytic manager has set the thread classpath)
			final Class<?> module_type = Class.forName(entry_point);
			
			if (IEnrichmentStreamingTopology.class.isAssignableFrom(module_type)) {
				
				final String cached_jars_dir = context.getServiceContext().getGlobalProperties().local_cached_jar_dir();
				
				// CASE 1) ENRICHMENT TOPOLOGY FORMAT
				
				final IEnrichmentStreamingTopology generic_topology = (IEnrichmentStreamingTopology) module_type.newInstance();
				final StreamingEnrichmentContextService wrapped_context = new StreamingEnrichmentContextService(context);
				wrapped_context.setUserTopology(generic_topology);
				wrapped_context.setJob(job_to_start);
				//(it's set up this way for testability)
				
				// Create a pretend bucket that has this job as the (sole) enrichment topology...
				final DataBucketBean converted_bucket = 
						(null != analytic_bucket.streaming_enrichment_topology()) 
						? analytic_bucket
						: BeanTemplateUtils.clone(analytic_bucket)
											.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.streaming)
											.with(DataBucketBean::streaming_enrichment_topology, 
													BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
														.with(EnrichmentControlMetadataBean::enabled, true)
														.with(EnrichmentControlMetadataBean::name, job_to_start.name())
														.with(EnrichmentControlMetadataBean::config, job_to_start.config())
													.done().get()
													)
											.done();
				
				wrapped_context.setBucket(converted_bucket);
				
				//... then use that to "fool" the getTopologyAndConfiguration:
				final Tuple2<Object,Map<String,String>> storm_topology = generic_topology.getTopologyAndConfiguration(converted_bucket, wrapped_context);
				
				//add in all the underlying artefacts file paths (now that the user has had the chance to override the defaults inside the getTopologyAndConfig call above)
				final Collection<Object> underlying_artefacts = Lambdas.get(() -> {
					// Check if the user has overridden the context, and set to the defaults if not
					try {
						return context.getUnderlyingArtefacts();
					}
					catch (Exception e) {
						// This is OK, it just means that the top. developer hasn't overridden the services, so we just use the default ones:
						context.getAnalyticsContextSignature(Optional.of(converted_bucket), Optional.empty());
						return context.getUnderlyingArtefacts();
					}			
				});				
				
				//(generic topology submit):
				return StormControllerUtil.startJob(_storm_controller, analytic_bucket, underlying_artefacts, user_lib_paths, (StormTopology) storm_topology._1(), storm_topology._2(), cached_jars_dir);
			}			
			// (no other options -currently- possible because of validation that has taken place)
			
			return CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage(this, "startAnalyticJob", ErrorUtils.get("Bucket={0} Job={1} Error=Module_class_not_recognized: {2}", analytic_bucket.full_name(), job_to_start.name(), job_to_start.entry_point())));
		}
		catch (Throwable t) {
			return CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage(this, "startAnalyticJob", ErrorUtils.getLongForm("Bucket={1} Job={2} Error={0}", t, analytic_bucket, job_to_start.name())));
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#stopAnalyticJob(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> stopAnalyticJob(
												final DataBucketBean analytic_bucket,
												final Collection<AnalyticThreadJobBean> jobs,
												final AnalyticThreadJobBean job_to_stop, 
												final IAnalyticsContext context)
	{
		return StormControllerUtil.stopJob(_storm_controller, analytic_bucket);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#resumeAnalyticJob(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> resumeAnalyticJob(
												final DataBucketBean analytic_bucket,
												final Collection<AnalyticThreadJobBean> jobs,
												final AnalyticThreadJobBean job_to_resume, 
												final IAnalyticsContext context)
	{
		// (no specific resume function, just use start)
		return startAnalyticJob(analytic_bucket, jobs, job_to_resume, context);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#suspendAnalyticJob(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> suspendAnalyticJob(
												final DataBucketBean analytic_bucket,
												final Collection<AnalyticThreadJobBean> jobs,
												final AnalyticThreadJobBean job_to_suspend, 
												final IAnalyticsContext context)			
	{
		// (no specific suspend function, just use stop)
		return stopAnalyticJob(analytic_bucket, jobs, job_to_suspend, context);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#startAnalyticJobTest(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> startAnalyticJobTest(
												final DataBucketBean analytic_bucket,
												final Collection<AnalyticThreadJobBean> jobs,
												final AnalyticThreadJobBean job_to_test, 
												final ProcessingTestSpecBean test_spec, 
												final IAnalyticsContext context)
	{
		//TODO (ALEPH-12): longer term this should run using a local storm controller (maybe spawned in a separate process?!)
		//TODO: maybe need some more "test" infrastructure set up here 
		return startAnalyticJob(analytic_bucket, jobs, job_to_test, context);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#checkAnalyticJobProgress(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public ManagementFuture<Boolean> checkAnalyticJobProgress(
										final DataBucketBean analytic_bucket,
										final Collection<AnalyticThreadJobBean> jobs,
										final AnalyticThreadJobBean job_to_check, 
										final IAnalyticsContext context)
	{
		// Streaming job never completes
		return FutureUtils.createManagementFuture(
				CompletableFuture.completedFuture(true)
				,
				CompletableFuture.completedFuture(
						Arrays.asList(
							ErrorUtils.buildSuccessMessage(this, "checkAnalyticJobProgress", "Streaming job")
						)
				)
			);
	}


	/** This service needs to load some additional classes via Guice. Here's the module that defines the bindings
	 * @return
	 */
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new StormAnalyticTechnologyModule());
	}
	
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		//(done see above)
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Collections.emptyList();
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
