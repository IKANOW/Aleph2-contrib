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
package com.ikanow.aleph2.analytics.hadoop.services;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import scala.Tuple2;

import com.ikanow.aleph2.analytics.hadoop.data_model.BasicHadoopConfigBean;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;

/** Starts and stops hadoop jobs
 * @author alex
 */
public class HadoopControllerService {

	/** Launches the specified Hadoop job
	 * @param job_name
	 * @param config
	 * @return
	 */
	public BasicMessageBean startJob(final String job_name, final BasicHadoopConfigBean config, final IAnalyticsContext context, boolean local_mode_override)
	{
		//TODO: setup InputFormat/OutputFormat classes
		
		
		
		return null;
	}
	
	public BasicMessageBean stopJob(final String job_name) {
		return null;
	}
	
	public Tuple2<Integer, BasicMessageBean> checkJob(final String job_name) {
		return null;
	}

	//////////////////////////////////////////////////////////////////
	
	// UTILITIES
	
	/** Get the general configuration object before job specific variables
	 * @param context
	 * @return
	 */
	protected Configuration getConfig(final IAnalyticsContext context){
		final GlobalPropertiesBean globals = context.getServiceContext().getGlobalProperties();
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
