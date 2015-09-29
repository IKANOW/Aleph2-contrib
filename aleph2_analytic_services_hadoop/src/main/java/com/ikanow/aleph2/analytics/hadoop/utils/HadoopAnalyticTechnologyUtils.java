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
package com.ikanow.aleph2.analytics.hadoop.utils;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/** A collection of utilities for Hadoop related validation etc
 * @author Alex
 */
public class HadoopAnalyticTechnologyUtils {

	/** Validate a single job for this analytic technology in the context of the bucket/other jobs
	 * @param analytic_bucket - the bucket (just for context)
	 * @param jobs - the entire list of jobs
	 * @return the validated bean (check for success:false)
	 */
	public static BasicMessageBean validateJobs(final DataBucketBean analytic_bucket, final Collection<AnalyticThreadJobBean> jobs) {
		final List<BasicMessageBean> res = 
				jobs.stream()
					.map(job -> validateJob(analytic_bucket, jobs, job))
					.collect(Collectors.toList());
		
		final boolean success = res.stream().allMatch(msg -> msg.success());

		final String message = res.stream().map(msg -> msg.message()).collect(Collectors.joining("\n"));
		
		return ErrorUtils.buildMessage(success, HadoopAnalyticTechnologyUtils.class, "validateJobs", message);
	}
	
	/** Validate a single job for this analytic technology in the context of the bucket/other jobs
	 * @param analytic_bucket - the bucket (just for context)
	 * @param jobs - the entire list of jobs (not normally required)
	 * @param job - the actual job
	 * @return the validated bean (check for success:false)
	 */
	public static BasicMessageBean validateJob(final DataBucketBean analytic_bucket, final Collection<AnalyticThreadJobBean> jobs, final AnalyticThreadJobBean job) {
		//TODO bucket validation - check "names" for simpleness (alphanum + _ only)
		//TODO here - check for unimplemented functions
		//TOOD here - check for non batch operations
		return null;
	}
	
	public static Configuration getHadoopConfig(final GlobalPropertiesBean globals) {
		final Configuration configuration = new Configuration(false);
		
		if (new File(globals.local_yarn_config_dir()).exists()) {
			configuration.addResource(new Path(globals.local_yarn_config_dir() +"/core-site.xml"));
			configuration.addResource(new Path(globals.local_yarn_config_dir() +"/yarn-site.xml"));
			configuration.addResource(new Path(globals.local_yarn_config_dir() +"/hdfs-site.xml"));
			configuration.addResource(new Path(globals.local_yarn_config_dir() +"/hadoop-site.xml"));
			configuration.addResource(new Path(globals.local_yarn_config_dir() +"/mapred-site.xml"));
		}
		// These are not added by Hortonworks, so add them manually
		configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");									
		configuration.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
		// Some other config defaults:
		// (not sure if these are actually applied, or derived from the defaults - for some reason they don't appear in CDH's client config)
		configuration.set("mapred.reduce.tasks.speculative.execution", "false");
		
		return configuration;
	}
}
