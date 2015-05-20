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
package com.ikanow.aleph2.storage_service_hdfs.services;
import java.util.Optional;

import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.conf.Configuration;

public class MockHdfsStorageService implements IStorageService {

	private static final Logger logger = Logger.getLogger(MockHdfsStorageService.class);

	final protected GlobalPropertiesBean _globals;
	
	@Inject 
	public MockHdfsStorageService(GlobalPropertiesBean globals) {
		_globals = globals;	
	}
	
	@Override
	public String getRootPath() {		
		return _globals.distributed_root_dir();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> @NonNull T getUnderlyingPlatformDriver(
			@NonNull Class<T> driver_class, Optional<String> driver_options) {
		T driver = null;
		try {
		if(driver_class!=null){
			if(driver_class.isAssignableFrom(FileContext.class)){
				FileContext fs = FileContext.getLocalFSFileContext(new Configuration());
				return (@NonNull T) fs;
			}
			try {
				driver = driver_class.newInstance();
			} catch (Exception e) {
				logger.error("Error instanciating driver class",e);
			}
		} // !=null
		} catch (Exception e) {
			logger.error("Caught Exception:",e);
		}
		return driver;
	}

}
