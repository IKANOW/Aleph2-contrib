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
package com.ikanow.aleph2.analytics.storm.utils;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.Optionals;

/** Contains some utility logic for the Storm Analytic Technology Service
 * @author Alex
 */
public class StormAnalyticTechnologyUtils {

	/** Validate a single job for this analytic technology in the context of the bucket/other jobs
	 * @param analytic_bucket - the bucket (just for context)
	 * @param jobs - the entire list of jobs
	 * @return the validated bean (check for success:false)
	 */
	public static BasicMessageBean validateJobs(final DataBucketBean analytic_bucket, final Collection<AnalyticThreadJobBean> jobs) {
		
		// Global validation:
		
		// Here we'll check:
		// (currently no global validation)
		
		// (Else graduate to per job validation)
		
		// Per-job validation:
		
		final List<BasicMessageBean> res = 
				jobs.stream()
					.map(job -> validateJob(analytic_bucket, jobs, job))
					.collect(Collectors.toList());
		
		final boolean success = res.stream().allMatch(msg -> msg.success());

		final String message = res.stream().map(msg -> msg.message()).collect(Collectors.joining("\n"));
		
		return ErrorUtils.buildMessage(success, StormAnalyticTechnologyUtils.class, "validateJobs", message);
	}
	
	/** Validate a single job for this analytic technology in the context of the bucket/other jobs
	 * @param analytic_bucket - the bucket (just for context)
	 * @param jobs - the entire list of jobs (not normally required)
	 * @param job - the actual job
	 * @return the validated bean (check for success:false)
	 */
	public static BasicMessageBean validateJob(final DataBucketBean analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final AnalyticThreadJobBean job) {
		
		final LinkedList<String> errors = new LinkedList<>();
		
		// This is for Storm specific validation
		// The core validation checks most of the "boilerplate" type requirements
		
		// Temporary limitations we'll police
		// - currently can only handle streaming inputs
		// - currently transient outputs have to be streaming
		
		// inputs
		
		Optionals.ofNullable(job.inputs()).stream().forEach(input -> {
			if (!"stream".equals(input.data_service())) {
				errors.add(ErrorUtils.get(ErrorUtils.TEMP_INPUTS_MUST_BE_STREAMING, analytic_bucket.full_name(), job.name(), input.data_service()));
			}
		});
		
		// output:
		
		if (null != job.output()) {
			if (Optional.ofNullable(job.output().is_transient()).orElse(false)) {
				final MasterEnrichmentType output_type = Optional.ofNullable(job.output().transient_type()).orElse(MasterEnrichmentType.none);
				if (MasterEnrichmentType.streaming != output_type) {
					errors.add(ErrorUtils.get(ErrorUtils.TEMP_TRANSIENT_OUTPUTS_MUST_BE_STREAMING, analytic_bucket.full_name(), job.name(), output_type));					
				}
			}
		}
		
		final boolean success = errors.isEmpty();
		
		return ErrorUtils.buildMessage(success, StormAnalyticTechnologyUtils.class, "validateJobs", errors.stream().collect(Collectors.joining(";")));
	}	
}
