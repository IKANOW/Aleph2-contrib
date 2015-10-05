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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Inject;
import com.ikanow.aleph2.analytics.hadoop.services.BatchEnrichmentContext;
import com.ikanow.aleph2.analytics.hadoop.services.BeJobLauncher;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;

/** NOT CURRENTLY USED - MIGHT GET PORTED ACROSS INTO A LocalHadoopController IF THAT MAKES SENSE LATER ON
 * @author Alex
 */
public class LocalBeJobLauncher extends BeJobLauncher {
    @SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(LocalBeJobLauncher.class);


    
	@Inject 
	public LocalBeJobLauncher(GlobalPropertiesBean globals, BatchEnrichmentContext batchEnrichmentContext) {
		super(globals, batchEnrichmentContext);
	}


	@Override
	public Configuration getHadoopConfig() {
		if (_configuration == null) {

			this._configuration = new Configuration(true);
			_configuration.setBoolean("mapred.used.genericoptionsparser", true); // (just stops an annoying warning from appearing)
			_configuration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");									
			_configuration.set("mapred.job.tracker", "local");
			_configuration.set("fs.defaultFS", "file:///");
			_configuration.unset("mapreduce.framework.name");
		}
		return _configuration;
	}


	@Override
	public void launch(Job job) throws ClassNotFoundException, IOException, InterruptedException {
		job.waitForCompletion(true);
		//super.launch(job);
	}


	
}
