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
package com.ikanow.aleph2.analytics.storm.assets;

import java.io.File;
import java.util.Arrays;
import java.util.Optional;

import org.junit.Before;

import backtype.storm.LocalCluster;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.analytics.storm.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestPassthroughBase {

	static LocalCluster _local_cluster;
	
	protected Injector _app_injector;
	
	@Inject IServiceContext _temp_service_context;
	static IServiceContext _service_context;
	
	@Before
	public void injectModules() throws Exception {
		if (null != _service_context)
			return;
		
		@SuppressWarnings("unused")
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		final Config config = ConfigFactory.parseFile(new File("./example_config_files/context_local_test.properties"))
				//these seeem to cause storm.kafka to break - it isn't clear why
//				.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
//				.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
//				.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
//				.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir))
				;
		
		try {
			_app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));
		}
		catch (Exception e) {
			try {
				e.printStackTrace();
			}
			catch (Exception ee) {
				System.out.println(ErrorUtils.getLongForm("{0}", e));
			}
		}
		
		_app_injector.injectMembers(this);
		_local_cluster = new LocalCluster();
		
		_service_context = _temp_service_context;
	}
}
