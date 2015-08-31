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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;

public class MockHdfsStorageSystemTest {
	
	@Test
	public void test(){
			GlobalPropertiesBean globals = BeanTemplateUtils.build(GlobalPropertiesBean.class)
												.with(GlobalPropertiesBean::local_yarn_config_dir, System.getenv("HADOOP_CONF_DIR")).done().get();
		
			MockHdfsStorageService storageService = new MockHdfsStorageService(globals);
		
			assertEquals(globals.distributed_root_dir(), storageService.getRootPath());
			assertEquals(1, storageService.getUnderlyingArtefacts().size());
			assertEquals(0, storageService.validateSchema(null, null)._2().size());
			
			FileContext fs1 = storageService.getUnderlyingPlatformDriver(FileContext.class, Optional.<String>empty()).get();
			assertNotNull(fs1);

			RawLocalFileSystem fs2 = storageService.getUnderlyingPlatformDriver(org.apache.hadoop.fs.RawLocalFileSystem.class,Optional.<String>empty()).get();
			assertNotNull(fs2); 

			AbstractFileSystem fs3 = storageService.getUnderlyingPlatformDriver(AbstractFileSystem.class,Optional.<String>empty()).get();
			assertNotNull(fs3); 			
			
			assertFalse("Not found", storageService.getUnderlyingPlatformDriver(null, Optional.empty()).isPresent());
			assertFalse("Not found", storageService.getUnderlyingPlatformDriver(String.class, Optional.empty()).isPresent());			
	}

	@Test
	public void test_handleBucketDeletionRequest() throws InterruptedException, ExecutionException, IOException {
		// 0) Setup
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		final GlobalPropertiesBean globals = BeanTemplateUtils.build(GlobalPropertiesBean.class)
				.with(GlobalPropertiesBean::local_yarn_config_dir, temp_dir)
				.with(GlobalPropertiesBean::distributed_root_dir, temp_dir)
				.with(GlobalPropertiesBean::local_root_dir, temp_dir)
				.with(GlobalPropertiesBean::distributed_root_dir, temp_dir)
				.done().get();
		
		final MockHdfsStorageService storage_service = new MockHdfsStorageService(globals);
		
		// 1) Set up bucket (code taken from management_db_service)
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test/delete/bucket").done().get();
		FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + bucket.full_name()));		
		setup_bucket(storage_service, bucket);
		final String bucket_path = System.getProperty("java.io.tmpdir") + File.separator + bucket.full_name();
		assertTrue("The file path has been created", new File(bucket_path + "/managed_bucket").exists());
		FileUtils.writeStringToFile(new File(bucket_path + IStorageService.STORED_DATA_SUFFIX + "/test"), "");
		assertTrue("The extra file path has been created", new File(bucket_path + IStorageService.STORED_DATA_SUFFIX + "/test").exists());		
		
		final CompletableFuture<BasicMessageBean> res1 = storage_service.getDataService().get().handleBucketDeletionRequest(bucket, Optional.empty(), false);
		assertEquals(true, res1.get().success());
		
		// Test:
		
		// Full filesystem exists
		assertTrue("The file path has *not* been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + bucket.full_name() + "/managed_bucket").exists());
		
		// Data directories no longer exist
		assertFalse("The data path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + bucket.full_name() + IStorageService.STORED_DATA_SUFFIX + "/test").exists());
				
		// Check nothing goes wrong when bucket doesn't exist
		final DataBucketBean bucket2 = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test/delete/bucket_not_exist").done().get();
		FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + bucket2.full_name()));
		assertFalse("The file path for bucket2 does not exist", new File(System.getProperty("java.io.tmpdir") + File.separator + bucket2.full_name()).exists());
		
		final CompletableFuture<BasicMessageBean> res2 = storage_service.getDataService().get().handleBucketDeletionRequest(bucket2, Optional.empty(), false);
		assertEquals(true, res2.get().success());
		
		//(check didn't create anything)
		
		// Full filesystem exists
		assertFalse("No bucket2 paths were created", new File(System.getProperty("java.io.tmpdir") + File.separator + bucket2.full_name()).exists());		
	}
	
	
	protected void setup_bucket(MockHdfsStorageService storage_service, final DataBucketBean bucket) {
		final FileContext dfs = storage_service.getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
		
		final String bucket_root = storage_service.getRootPath() + "/" + bucket.full_name();		
		
		Arrays.asList(
				bucket_root + "/managed_bucket",
				bucket_root + "/managed_bucket/logs",
				bucket_root + "/managed_bucket/logs/harvest",
				bucket_root + "/managed_bucket/logs/enrichment",
				bucket_root + "/managed_bucket/logs/storage",
				bucket_root + "/managed_bucket/assets",
				bucket_root + "/managed_bucket/import",
				bucket_root + "/managed_bucket/temp",
				bucket_root + "/managed_bucket/stored",
				bucket_root + "/managed_bucket/stored/raw",
				bucket_root + "/managed_bucket/stored/json",
				bucket_root + "/managed_bucket/stored/processed",
				bucket_root + "/managed_bucket/ready"
				)
				.stream()
				.map(s -> new Path(s))
				.forEach(Lambdas.wrap_consumer_u(p -> dfs.mkdir(p, FsPermission.getDefault(), true)));
		
	}
	
	@Test
	public void test_ageOut() {
		//TODO:
	}
}
