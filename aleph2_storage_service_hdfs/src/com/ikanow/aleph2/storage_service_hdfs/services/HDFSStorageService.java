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
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HDFSStorageService implements IStorageService {
	private static final Logger _logger = LogManager.getLogger();	
	
	final protected GlobalPropertiesBean _globals;
	
	@Inject 
	HDFSStorageService(GlobalPropertiesBean globals) {
		_globals = globals;	
	}
	
	@Override
	public String getRootPath() {		
		return _globals.distributed_root_dir();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(
			Class<T> driver_class, Optional<String> driver_options) {
		T driver = null;
		try {
			if(driver_class!=null){
				final Configuration config = getConfiguration();
				URI uri = driver_options.isPresent() ? new URI(driver_options.get()) : getUri(config);
				
				if (driver_class.isAssignableFrom(AbstractFileSystem.class)) {
	
					AbstractFileSystem fs = AbstractFileSystem.createFileSystem(uri, config);
					
					return (Optional<T>) Optional.of(fs);
				}			
				else if(driver_class.isAssignableFrom(FileContext.class)){				
					FileContext fs = FileContext.getFileContext(AbstractFileSystem.createFileSystem(uri, config), config);
					return (Optional<T>) Optional.of(fs);
				}
				else if(driver_class.isAssignableFrom(RawLocalFileSystem.class)){
					return Optional.of(driver_class.newInstance());
				}
			} // !=null
		} 
		catch (Exception e) {
			_logger.error("Caught Exception:",e);
		}
		return Optional.ofNullable(driver);
	}

	/** 
	 * Override this function with system specific configuration
	 * @return
	 */
	protected Configuration getConfiguration(){
		Configuration config = new Configuration(false);
		
		if (new File(_globals.local_yarn_config_dir()).exists()) {
			config.addResource(new Path(_globals.local_yarn_config_dir() +"/yarn-site.xml"));
			config.addResource(new Path(_globals.local_yarn_config_dir() +"/core-site.xml"));
			config.addResource(new Path(_globals.local_yarn_config_dir() +"/hdfs-site.xml"));
		}
		else {
			config.addResource("default_fs.xml");						
		}
		// These are not added by Hortonworks, so add them manually
		config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");									
		config.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
		
		return config;
		
	}

	protected URI getUri(Configuration configuration){
		URI uri = null;
		try {			
			String uriStr = configuration.get("fs.default.name", configuration.get("fs.defaultFS"));
			if(uriStr==null){
				// default with localhost
				uriStr = "hdfs://localhost:8020";
			}
			uri = new URI(uriStr);
		} catch (URISyntaxException e) {
			_logger.error("Caught Exception:",e);
		}
		return uri;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean)
	 */
	@Override
	public List<BasicMessageBean> validateSchema(final StorageSchemaBean schema, final DataBucketBean bucket) {
		return Collections.emptyList();
	}

	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Arrays.asList(this);
	}

}
