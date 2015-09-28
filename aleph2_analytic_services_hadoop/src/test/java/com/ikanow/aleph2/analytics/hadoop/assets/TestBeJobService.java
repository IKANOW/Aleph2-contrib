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
package com.ikanow.aleph2.analytics.hadoop.assets;

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Injector;
import com.ikanow.aleph2.analytics.hadoop.services.IBeJobService;
import com.ikanow.aleph2.analytics.hadoop.services.MiniClusterBeJobLauncher;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestBeJobService {
    private static final Logger logger = LogManager.getLogger(TestBeJobService.class);

	protected IBeJobService beJobService;

	protected static boolean useMiniCluster = false;
	
	@Before
	public void setupDependencies() throws Exception {
		try{
			final String temp_dir = System.getProperty("java.io.tmpdir");

			// OK we're going to use guice, it was too painful doing this by hand...				
			Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("/test_hadoop_services.properties")))
					.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
					.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
					.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
					.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));

			Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));	
			app_injector.injectMembers(this);

			this.beJobService = app_injector.getInstance(IBeJobService.class);		
		}
		catch (Throwable t) {
			System.out.println(ErrorUtils.getLongForm("{0}", t));
			throw t; 
		}
	} // setup dependencies

	//TODO: this test is currently failing because of the guava/hadoop-2.6.x issue
	
	@org.junit.Ignore
	@Test
	public void testBeJobService() throws Exception{
		try {
			
			//BeBucketActor.launchReadyJobs(fileContext, buckeFullName1, bucketPath1, beJobService, _management_db, null);
			//TODO: launch something else....
			
			logger.info("Stopping service");
			
			if(beJobService instanceof MiniClusterBeJobLauncher){
				((MiniClusterBeJobLauncher)beJobService).stop();
			}
		} catch (Throwable t) {
			logger.error("testBeJobService caught exception",t);
			throw t;
		}
		logger.info("Stopped service");		
	}
}
