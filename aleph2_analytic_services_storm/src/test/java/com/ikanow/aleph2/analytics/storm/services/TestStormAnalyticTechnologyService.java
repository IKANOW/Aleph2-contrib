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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.junit.Test;

import com.ikanow.aleph2.analytics.storm.modules.StormAnalyticTechnologyModule;
import com.ikanow.aleph2.analytics.storm.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

public class TestStormAnalyticTechnologyService {

	@Test
	public void test_misc() throws InterruptedException, ExecutionException {
		
		final MockAnalyticsContext test_context = new MockAnalyticsContext();
		final StormAnalyticTechnologyService tech_service = new StormAnalyticTechnologyService(); // (will look for a controller then back-off to NoStormController)		
		assertTrue("Found storm controller: " + tech_service._storm_controller, tech_service._storm_controller instanceof NoStormController);
		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class).done().get();
		final Collection<AnalyticThreadJobBean> jobs = Collections.emptyList();
		
		tech_service.onInit(test_context); // (just make sure it doesnt' crash)
		assertFalse("Should return can't run on node", tech_service.canRunOnThisNode(test_bucket, jobs, test_context));
		
		// OK knock off all the boring calls that don't do anything:
		
		{
			final CompletableFuture<BasicMessageBean> res = tech_service.onNewThread(test_bucket, jobs, test_context, true);
			assertTrue("Trivial service returned true", res.get().success());
			assertEquals("", res.get().message());
		}
		{
			final CompletableFuture<BasicMessageBean> res = tech_service.onUpdatedThread(test_bucket, test_bucket, jobs, true, Optional.empty(), test_context);
			assertTrue("Trivial service returned true", res.get().success());
			assertEquals("", res.get().message());
		}
		{
			final CompletableFuture<BasicMessageBean> res = tech_service.onDeleteThread(test_bucket, jobs, test_context);
			assertTrue("Trivial service returned true", res.get().success());
			assertEquals("", res.get().message());
		}
		{
			final ManagementFuture<Boolean> res = tech_service.checkCustomTrigger(test_bucket, null, test_context);
			assertFalse("Trivial service returned false", res.get());
			assertTrue("Side channel with 1 error", 1 == res.getManagementResults().get().size());
			assertFalse("Side channel with 1 error: " + res.getManagementResults().get().stream().map(m->m.message()).collect(Collectors.joining(";")), 
					res.getManagementResults().get().iterator().next().success());
			assertEquals("Side channel with 1 error", "No custom triggers supported", res.getManagementResults().get().iterator().next().message());
		}
		{
			final CompletableFuture<BasicMessageBean> res = tech_service.onThreadExecute(test_bucket, jobs, Collections.emptyList(), test_context);
			assertTrue("Trivial service returned true", res.get().success());
			assertEquals("", res.get().message());
		}
		{
			final CompletableFuture<BasicMessageBean> res = tech_service.onPurge(test_bucket, jobs, test_context);
			assertTrue("Trivial service returned true", res.get().success());
			assertEquals("", res.get().message());
		}
		{
			final CompletableFuture<BasicMessageBean> res = tech_service.onPeriodicPoll(test_bucket, jobs, test_context);
			assertTrue("Trivial service returned true", res.get().success());
			assertEquals("", res.get().message());
		}
		{
			final CompletableFuture<BasicMessageBean> res = tech_service.onTestThread(test_bucket, jobs, null, test_context);
			assertTrue("Trivial service returned true", res.get().success());
			assertEquals("", res.get().message());
		}
		{
			final ManagementFuture<Boolean> res = tech_service.checkAnalyticJobProgress(test_bucket, jobs, null, test_context);
			assertTrue("Trivial service returned true", res.get());
			assertTrue("Side channel with 1 error", 1 == res.getManagementResults().get().size());
			assertTrue("Side channel with 1 error: " + res.getManagementResults().get().stream().map(m->m.message()).collect(Collectors.joining(";")), 
					res.getManagementResults().get().iterator().next().success());
			assertEquals("Side channel with 1 error", "Streaming job", res.getManagementResults().get().iterator().next().message());
		}
		
		// And finally for completeness sake:
		
		assertEquals(1, StormAnalyticTechnologyService.getExtraDependencyModules().size());
		assertTrue(StormAnalyticTechnologyService.getExtraDependencyModules().iterator().next() instanceof StormAnalyticTechnologyModule);
		tech_service.youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules();
	}
	
	@Test
	public void test_startJob() throws InterruptedException, ExecutionException {
		
		final MockAnalyticsContext test_context = new MockAnalyticsContext();
		final StormAnalyticTechnologyService tech_service = new StormAnalyticTechnologyService(new NoStormController());
		
		// The main thread logic is currently tested by the TestPassthoughtTopology class, so we're just testing some boring/trivial things here
		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
											.with(DataBucketBean::full_name, "test_bucket")
											.done().get();
		
		tech_service.onInit(test_context); // (just make sure it doesnt' crash)
		
		// 2 different error types and vs 3 different ways of getting at the code
		{
			final AnalyticThreadJobBean job_to_start = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
													.with(AnalyticThreadJobBean::name, "test_job")
													.with(AnalyticThreadJobBean::entry_point, "unknown_test")
													.done().get();
			
			final CompletableFuture<BasicMessageBean> res = tech_service.startAnalyticJob(test_bucket, Arrays.asList(job_to_start), job_to_start, test_context);
			
			assertFalse("Call fails", res.get().success());
			assertTrue(res.get().message().contains("Error="));
		}
		{
			final AnalyticThreadJobBean job_to_start = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
													.with(AnalyticThreadJobBean::name, "test_job")
													.with(AnalyticThreadJobBean::entry_point, "com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean")
													.done().get();
			
			final CompletableFuture<BasicMessageBean> res = tech_service.resumeAnalyticJob(test_bucket, Arrays.asList(job_to_start), job_to_start, test_context);
			
			assertFalse("Call fails", res.get().success());
			assertEquals(ErrorUtils.get("Bucket={0} Job={1} Error=Module_class_not_recognized: {2}", "test_bucket", "test_job", "com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean"), res.get().message());
		}
		//(duplicate just to follow the test path)
		{
			final AnalyticThreadJobBean job_to_start = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
													.with(AnalyticThreadJobBean::name, "test_job")
													.with(AnalyticThreadJobBean::entry_point, "com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean")
													.done().get();
			
			final CompletableFuture<BasicMessageBean> res = tech_service.startAnalyticJobTest(test_bucket, Arrays.asList(job_to_start), job_to_start, null, test_context);
			
			assertFalse("Call fails", res.get().success());
			assertEquals(ErrorUtils.get("Bucket={0} Job={1} Error=Module_class_not_recognized: {2}", "test_bucket", "test_job", "com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean"), res.get().message());
		}
		
	}


	@Test
	public void test_stopJob() throws InterruptedException, ExecutionException {
		
		// (all the logic is encapsulated within the storm controller, so there are just trivial tests for coverage sake)
		
		final MockAnalyticsContext test_context = new MockAnalyticsContext();
		final StormAnalyticTechnologyService tech_service = new StormAnalyticTechnologyService(new NoStormController());
		
		// The main thread logic is currently tested by the TestPassthoughtTopology class, so we're just testing some boring/trivial things here
		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
											.with(DataBucketBean::full_name, "test_bucket")
											.done().get();
		
		tech_service.onInit(test_context); // (just make sure it doesnt' crash)

		{
			final AnalyticThreadJobBean job_to_stop = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::name, "test_job")
					.with(AnalyticThreadJobBean::entry_point, "unknown_test")
					.done().get();
			
			final CompletableFuture<BasicMessageBean> res = tech_service.stopAnalyticJob(test_bucket, Arrays.asList(job_to_stop), job_to_stop, test_context);
			
			assertTrue("Call fails", res.get().success());
			assertEquals("Stopped storm job succesfully", res.get().message());
		}
		{
			final AnalyticThreadJobBean job_to_stop = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::name, "test_job")
					.with(AnalyticThreadJobBean::entry_point, "unknown_test")
					.done().get();
			
			final CompletableFuture<BasicMessageBean> res = tech_service.suspendAnalyticJob(test_bucket, Arrays.asList(job_to_stop), job_to_stop, test_context);
			
			assertTrue("Call fails", res.get().success());
			assertEquals("Stopped storm job succesfully", res.get().message());
		}
		
	}

}
