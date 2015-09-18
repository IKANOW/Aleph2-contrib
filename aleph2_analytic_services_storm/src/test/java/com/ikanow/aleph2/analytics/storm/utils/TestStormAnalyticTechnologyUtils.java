/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package com.ikanow.aleph2.analytics.storm.utils;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

public class TestStormAnalyticTechnologyUtils {

	@Test
	public void test_globalValidation() {

		// Fail: both enabled and present
		{
			final DataBucketBean test_bucket1 = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::full_name, "/test")
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.streaming)
					.with(DataBucketBean::streaming_enrichment_topology, BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get())
					.with(DataBucketBean::analytic_thread, 
							BeanTemplateUtils.build(AnalyticThreadBean.class)
							.with(AnalyticThreadBean::jobs, Arrays.asList(BeanTemplateUtils.build(AnalyticThreadJobBean.class).done().get())
									)
									.done().get()
							)
							.done().get();
			
			
			final BasicMessageBean res1 = StormAnalyticTechnologyUtils.validateJobs(test_bucket1, Collections.emptyList());
			
			assertFalse("Validation should fail", res1.success());
			assertEquals("Correct error message: " + res1.message(), ErrorUtils.get(ErrorUtils.TEMP_MIXED_ANALYTICS_AND_ENRICHMENT, "/test"), res1.message());
			
		}
		// Pass: both present but streaming not being used
		{
			final DataBucketBean test_bucket1 = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::full_name, "/test")
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.batch)
					.with(DataBucketBean::streaming_enrichment_topology, BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).with(EnrichmentControlMetadataBean::enabled, false).done().get())
					.with(DataBucketBean::analytic_thread, 
							BeanTemplateUtils.build(AnalyticThreadBean.class)
							.with(AnalyticThreadBean::jobs, Arrays.asList(BeanTemplateUtils.build(AnalyticThreadJobBean.class).with(AnalyticThreadJobBean::enabled, false).done().get())
									)
									.done().get()
							)
							.done().get();
						
			final BasicMessageBean res1 = StormAnalyticTechnologyUtils.validateJobs(test_bucket1, Collections.emptyList());
			
			assertTrue("Validation should pass", res1.success());
			assertEquals("Correct error message: " + res1.message(), "", res1.message());
			
		}
		// Pass: both present but disabled 
		{
			final DataBucketBean test_bucket1 = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::full_name, "/test")
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.streaming)
					.with(DataBucketBean::streaming_enrichment_topology, BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).with(EnrichmentControlMetadataBean::enabled, false).done().get())
					.with(DataBucketBean::analytic_thread, 
							BeanTemplateUtils.build(AnalyticThreadBean.class)
							.with(AnalyticThreadBean::jobs, Arrays.asList(BeanTemplateUtils.build(AnalyticThreadJobBean.class).with(AnalyticThreadJobBean::enabled, false).done().get())
									)
									.done().get()
							)
							.done().get();
						
			final BasicMessageBean res1 = StormAnalyticTechnologyUtils.validateJobs(test_bucket1, Collections.emptyList());
			
			assertTrue("Validation should pass", res1.success());
			assertEquals("Correct error message: " + res1.message(), "", res1.message());
			
		}
		// Pass: only one present (enrichment) 
		{
			final DataBucketBean test_bucket1 = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::full_name, "/test")
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.streaming)
					.with(DataBucketBean::streaming_enrichment_topology, BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).with(EnrichmentControlMetadataBean::enabled, true).done().get())
					.done().get();
						
			final BasicMessageBean res1 = StormAnalyticTechnologyUtils.validateJobs(test_bucket1, Collections.emptyList());
			
			assertTrue("Validation should pass", res1.success());
			assertEquals("Correct error message: " + res1.message(), "", res1.message());
			
		}
		// Pass: only one present (analytics) 
		{
			final DataBucketBean test_bucket1 = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::full_name, "/test")
					.with(DataBucketBean::master_enrichment_type, DataBucketBean.MasterEnrichmentType.streaming)
					.with(DataBucketBean::analytic_thread, 
							BeanTemplateUtils.build(AnalyticThreadBean.class)
							.with(AnalyticThreadBean::jobs, Arrays.asList(BeanTemplateUtils.build(AnalyticThreadJobBean.class).with(AnalyticThreadJobBean::enabled, true).done().get())
									)
									.done().get()
							)
					.done().get();
						
			final BasicMessageBean res1 = StormAnalyticTechnologyUtils.validateJobs(test_bucket1, Collections.emptyList());
			
			assertTrue("Validation should pass", res1.success());
			assertEquals("Correct error message: " + res1.message(), "", res1.message());
			
		}
	}

	@Test
	public void test_localValidation() {
		
		// Base bucket:
		
		final DataBucketBean test_bucket1 = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "test")
				.with(DataBucketBean::full_name, "/test")
				.with(DataBucketBean::analytic_thread, 
						BeanTemplateUtils.build(AnalyticThreadBean.class)
						.with(AnalyticThreadBean::jobs, Arrays.asList(BeanTemplateUtils.build(AnalyticThreadJobBean.class).with(AnalyticThreadJobBean::enabled, true).done().get())
								)
								.done().get()
						)
				.done().get();
				
		// Test error case 1:
		
		final AnalyticThreadJobBean.AnalyticThreadJobInputBean analytic_input1 =  BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobInputBean.class)
				.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::data_service, "search_index_service")
				.done().get();

		final AnalyticThreadJobBean analytic_job1 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
				.with(AnalyticThreadJobBean::name, "analytic_job_1")
				.with(AnalyticThreadJobBean::analytic_technology_name_or_id, "test_analytic_tech_id")
				.with(AnalyticThreadJobBean::inputs, Arrays.asList(analytic_input1))
				.with(AnalyticThreadJobBean::module_names_or_ids, Arrays.asList("id1", "name2"))
				.done().get();

		{
			final BasicMessageBean res1 = StormAnalyticTechnologyUtils.validateJob(test_bucket1, Collections.emptyList(), analytic_job1);			
			assertFalse("Validation should fail", res1.success());
			assertEquals("Correct error message: " + res1.message(), ErrorUtils.get(ErrorUtils.TEMP_INPUTS_MUST_BE_STREAMING, "/test", "analytic_job_1", "search_index_service"), res1.message());
		}
		
		// Test error case 2:

		final AnalyticThreadJobBean.AnalyticThreadJobOutputBean analytic_output1 =  BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobOutputBean.class)
				.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::is_transient, true)
				.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::transient_type, DataBucketBean.MasterEnrichmentType.batch)
				.done().get();

		final AnalyticThreadJobBean analytic_job2 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
				.with(AnalyticThreadJobBean::name, "analytic_job_2")
				.with(AnalyticThreadJobBean::analytic_technology_name_or_id, "test_analytic_tech_id")
				.with(AnalyticThreadJobBean::output, analytic_output1)
				.with(AnalyticThreadJobBean::module_names_or_ids, Arrays.asList("id1", "name2"))
				.done().get();
		
		{
			final BasicMessageBean res1 = StormAnalyticTechnologyUtils.validateJob(test_bucket1, Collections.emptyList(), analytic_job2);			
			assertFalse("Validation should fail", res1.success());
			assertEquals("Correct error message: " + res1.message(), ErrorUtils.get(ErrorUtils.TEMP_TRANSIENT_OUTPUTS_MUST_BE_STREAMING, "/test", "analytic_job_2", "batch"), res1.message());
		}
				
		// Test pass case 1:
		
		final AnalyticThreadJobBean.AnalyticThreadJobInputBean analytic_input2 =  BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobInputBean.class)
				.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::data_service, "stream")
				.done().get();
		
		final AnalyticThreadJobBean.AnalyticThreadJobOutputBean analytic_output2 =  BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobOutputBean.class)
				.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::is_transient, false)
				.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::transient_type, DataBucketBean.MasterEnrichmentType.batch)
				.done().get();

		final AnalyticThreadJobBean analytic_job3 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
				.with(AnalyticThreadJobBean::name, "analytic_job_1")
				.with(AnalyticThreadJobBean::analytic_technology_name_or_id, "test_analytic_tech_id")
				.with(AnalyticThreadJobBean::inputs, Arrays.asList(analytic_input2))
				.with(AnalyticThreadJobBean::output, analytic_output2)
				.with(AnalyticThreadJobBean::module_names_or_ids, Arrays.asList("id1", "name2"))
				.done().get();
		
		{
			final BasicMessageBean res1 = StormAnalyticTechnologyUtils.validateJob(test_bucket1, Collections.emptyList(), analytic_job3);			
			assertTrue("Validation should pass", res1.success());
			assertEquals("Correct error message: " + res1.message(), "", res1.message());
		}
		
		// Check that they get merged together by global validation
		
		{
			final BasicMessageBean res2 = StormAnalyticTechnologyUtils.validateJobs(test_bucket1, Arrays.asList(analytic_job1, analytic_job2, analytic_job3));
			assertFalse("Validation should fail", res2.success());
			final String[] messages = res2.message().split("\n");
			assertEquals(2, messages.length);
			assertEquals("Correct error message 1: " + messages[0], ErrorUtils.get(ErrorUtils.TEMP_INPUTS_MUST_BE_STREAMING, "/test", "analytic_job_1", "search_index_service"), messages[0]);
			assertEquals("Correct error message 2: " + messages[1], ErrorUtils.get(ErrorUtils.TEMP_TRANSIENT_OUTPUTS_MUST_BE_STREAMING, "/test", "analytic_job_2", "batch"), messages[1]);
		}
	}

	//TODO: (write!) then test the code to copy the streaming enrichment into the analytic thread

}
