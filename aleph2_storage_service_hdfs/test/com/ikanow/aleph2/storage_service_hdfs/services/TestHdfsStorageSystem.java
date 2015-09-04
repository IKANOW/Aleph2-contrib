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

import static org.junit.Assert.*;

import java.util.Optional;

import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;

import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

public class TestHdfsStorageSystem {
	
	@Test
	public void test(){
			GlobalPropertiesBean globals = BeanTemplateUtils.build(GlobalPropertiesBean.class)
												.with(GlobalPropertiesBean::local_yarn_config_dir, System.getenv("HADOOP_CONF_DIR")).done().get();
		
			HdfsStorageService storageService = new HdfsStorageService(globals);
			
			assertEquals(globals.distributed_root_dir(), storageService.getRootPath());
			assertEquals(1, storageService.getUnderlyingArtefacts().size());
						
			FileContext fs1 = storageService.getUnderlyingPlatformDriver(FileContext.class, Optional.<String>empty()).get();
			assertNotNull(fs1);
			FileContext fs1b = storageService.getUnderlyingPlatformDriver(FileContext.class, Optional.<String>empty()).get();
			assertEquals(fs1, fs1b);

			RawLocalFileSystem fs2 = storageService.getUnderlyingPlatformDriver(RawLocalFileSystem.class,Optional.<String>empty()).get();
			assertNotNull(fs2); 
			RawLocalFileSystem fs2b = storageService.getUnderlyingPlatformDriver(RawLocalFileSystem.class,Optional.<String>empty()).get();
			assertEquals(fs2, fs2b);

			AbstractFileSystem fs3 = storageService.getUnderlyingPlatformDriver(AbstractFileSystem.class,Optional.<String>empty()).get();
			assertNotNull(fs3); 
			AbstractFileSystem fs3b = storageService.getUnderlyingPlatformDriver(AbstractFileSystem.class,Optional.<String>empty()).get();
			assertEquals(fs3, fs3b);
			
			try {
				storageService.getUnderlyingPlatformDriver(null, Optional.empty());
				fail("Should have thrown, NPE");
			}
			catch (Exception e) {} // don't care what
			assertFalse("Not found", storageService.getUnderlyingPlatformDriver(String.class, Optional.empty()).isPresent());			
	}
	
	//(All the functional tests are over in MockHdfsStorageSystemTest)
}
