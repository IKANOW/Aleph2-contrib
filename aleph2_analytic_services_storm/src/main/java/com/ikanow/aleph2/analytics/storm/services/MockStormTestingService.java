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
package com.ikanow.aleph2.analytics.storm.services;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;

/** Simple service for submitting test jobs
 * @author Alex
 */
public class MockStormTestingService {

	protected final IServiceContext _service_context;

	/** User c'tor
	 * @param service_context
	 */
	public MockStormTestingService(IServiceContext service_context) {
		_service_context = service_context;
	}
	
	/** Submit a test bucket with exactly one analytic job
	 * @param test_bucket
	 * @param service_context
	 */
	public CompletableFuture<BasicMessageBean> testAnalyticModule(final DataBucketBean test_bucket) {
		
		final Optional<AnalyticThreadJobBean> job = Optionals.of(() -> test_bucket.analytic_thread().jobs().get(0));
		if (!job.isPresent()) {
			throw new RuntimeException("Bucket must have one analytic thread");
		}
		
		//create dummy libary:
		final SharedLibraryBean library = BeanTemplateUtils.build(SharedLibraryBean.class)
		.with(SharedLibraryBean::path_name, "/test/lib")
		.done().get();
								
		// Context		
		final MockAnalyticsContext test_analytics_context = new MockAnalyticsContext(_service_context);
		test_analytics_context.setBucket(test_bucket);
		test_analytics_context.setTechnologyConfig(library);
				
		//PHASE 2: CREATE TOPOLOGY AND SUBMit
		
		final StormAnalyticTechnologyService analytic_tech = new StormAnalyticTechnologyService(new LocalStormController());
		return analytic_tech.startAnalyticJob(test_bucket, Arrays.asList(job.get()), job.get(), test_analytics_context);
	}
	
}
