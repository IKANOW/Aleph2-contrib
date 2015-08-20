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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.ikanow.aleph2.analytics.storm.utils.StormAnalyticTechnologyUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_import.BucketDiffBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

import java.util.Arrays;

/** Storm analytic technology module - provides the interface between Storm and Aleph2
 * @author Alex
 */
public class StormAnalyticTechnologyService implements IAnalyticsTechnologyModule {

	////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////
	
	// GENERAL CALLBACKS
	
	@Override
	public void onInit(final IAnalyticsContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean canRunOnThisNode(
					final DataBucketBean analytic_bucket,
					final Collection<AnalyticThreadJobBean> jobs, 
					final IAnalyticsContext context)
	{
		// TODO Auto-generated method stub
		return false;
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
		// (already validated)
		
		// If it's an IEnrichmentModule then need to wrap the streaming enrichment context 
		// If it's a harvest module then wrap the harvest context
		
		// TODO Auto-generated method stub
		return null;
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
		// TODO Auto-generated method stub
		return null;
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
		//TODO: longer term this should run using a local storm controller (maybe spawned in a separate process?!)
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
				CompletableFuture.completedFuture(false)
				,
				CompletableFuture.completedFuture(
						Arrays.asList(
							ErrorUtils.buildSuccessMessage(this, "checkAnalyticJobProgress", "Streaming job")
						)
				)
			);
	}

}
