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

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.analytics.storm.data_model.IStormController;
import com.ikanow.aleph2.analytics.storm.modules.MockStormAnalyticTechnologyModule;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;

public class MockStormAnalyticTechnologyService extends StormAnalyticTechnologyService {

	public static final String DISABLE_FILE = "mock_storm_disable";
	
	/** Guice constructor
	 */
	@Inject
	public MockStormAnalyticTechnologyService(final IStormController storm_controller) {
		super(storm_controller);
	}		
	
	@Override
	public boolean canRunOnThisNode(
					final DataBucketBean analytic_bucket,
					final Collection<AnalyticThreadJobBean> jobs, 
					final IAnalyticsContext context)
	{
		// For testing, if file "disable_storm" is present in yarn config then return false:
		
		return !(new File(ModuleUtils.getGlobalProperties().local_yarn_config_dir() + File.separator + DISABLE_FILE).exists());
	}
	
	
	/** This service needs to load some additional classes via Guice. Here's the module that defines the bindings
	 * @return
	 */
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new MockStormAnalyticTechnologyModule());
	}
	
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		//(done see above)
	}
}
