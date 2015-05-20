package com.ikanow.aleph2.storage_service_hdfs.services;

import static org.junit.Assert.*;

import java.util.Optional;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;

import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

public class HDFSStorageSystemTest {
	
	@Test
	public void test(){
			GlobalPropertiesBean globals = BeanTemplateUtils.build(GlobalPropertiesBean.class)
												.with(GlobalPropertiesBean::local_yarn_config_dir, System.getenv("HADOOP_CONF_DIR")).done().get();
		
			HDFSStorageService storageService = new HDFSStorageService(globals);
			
			FileContext fs1 = storageService.getUnderlyingPlatformDriver(FileContext.class, Optional.<String>empty());
			assertNotNull(fs1);

			RawLocalFileSystem fs2 = storageService.getUnderlyingPlatformDriver(org.apache.hadoop.fs.RawLocalFileSystem.class,Optional.<String>empty());
			assertNotNull(fs2); 
			
	}

}
