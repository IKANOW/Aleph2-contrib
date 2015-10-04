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
package com.ikanow.aleph2.analytics.hadoop.services;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.mapreduce.Job;

import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

/** Storm analytic technology service using standalone local Storm instead of full remote service - otherwise should behave the same
 * @author Alex
 */
public class MockHadoopTechnologyService extends HadoopTechnologyService {

	public static final String DISABLE_FILE = "mock_hadoop_disable";
	
	protected static Map<String, Job> _jobs = new ConcurrentHashMap<>();
	
	/** Utility to make local jobs check-able
	 * @param job
	 */
	protected void handleLocalJob(Job job) {
		_jobs.put(job.getJobName(), job);
	}
	
	/** User constructor
	 */
	public MockHadoopTechnologyService() {
		super();
	}
		
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.analytics.hadoop.services.HadoopTechnologyService#canRunOnThisNode(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public boolean canRunOnThisNode(
					final DataBucketBean analytic_bucket,
					final Collection<AnalyticThreadJobBean> jobs, 
					final IAnalyticsContext context)
	{
		// For testing, if file "disable_storm" is present in yarn config then return false:
		
		return !(new File(ModuleUtils.getGlobalProperties().local_yarn_config_dir() + File.separator + DISABLE_FILE).exists());
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
						success -> {
							handleLocalJob(success);
							return ErrorUtils.buildSuccessMessage
												(this.getClass().getName(), "startAnalyticJobTest", success.getJobID().toString());
						}));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule#checkAnalyticJobProgress(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Collection, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext)
	 */
	@Override
	public ManagementFuture<Boolean> checkAnalyticJobProgress(
			DataBucketBean analytic_bucket,
			Collection<AnalyticThreadJobBean> jobs,
			AnalyticThreadJobBean job_to_check, IAnalyticsContext context) {
		
		final String job_name = BucketUtils.getUniqueSignature(analytic_bucket.full_name(), Optional.ofNullable(job_to_check.name()));	
		return FutureUtils.createManagementFuture(
				CompletableFuture.completedFuture(
					Optional.ofNullable(_jobs.get(job_name))
							.map(Lambdas.wrap_u(job -> job.isComplete()))
							.orElse(true)));
	}
	
	/** This service needs to load some additional classes via Guice. Here's the module that defines the bindings
	 * @return
	 */
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList(); //(nothing for now)
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.analytics.hadoop.services.HadoopTechnologyService#youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules()
	 */
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		//(done see above)
	}
}
