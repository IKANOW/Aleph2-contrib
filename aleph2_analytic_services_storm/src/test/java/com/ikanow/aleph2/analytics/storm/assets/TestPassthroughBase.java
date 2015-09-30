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
