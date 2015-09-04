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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

public class TestHdfsDataWriteService {

	/** Get some easy testing out the way
	 * 		HfdsDataWriteService.getSuffix
	 */
	@Test
	public void test_utilityMethods_getSuffix() {
		
		final Date then = new Date(1441311160000L); // Thu, 03 Sep 2015 20:12:40 GMT
		
		// No storage schema
		{
			final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/static")
				.done().get();

			assertEquals("", HfdsDataWriteService.getSuffix(then, bucket, IStorageService.StorageStage.raw));			
		}
		// No grouping time
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/static")
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::storage_schema,
									BeanTemplateUtils.build(StorageSchemaBean.class)
										.with(StorageSchemaBean::raw, 
												BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
												.done().get())
									.done().get()
								)
							.done().get())
					.done().get();
			assertEquals("", HfdsDataWriteService.getSuffix(then, test_bucket, IStorageService.StorageStage.raw));			
		}
		// Malformed grouping time
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/static")
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::storage_schema,
									BeanTemplateUtils.build(StorageSchemaBean.class)
										.with(StorageSchemaBean::json, 
												BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
													.with(StorageSchemaBean.StorageSubSchemaBean::grouping_time_period, "bananas")
												.done().get())
									.done().get()
								)
							.done().get())
					.done().get();
			
			assertEquals("", HfdsDataWriteService.getSuffix(then, test_bucket, IStorageService.StorageStage.json));			
		}
		// Valid grouping time
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/static")
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::storage_schema,
									BeanTemplateUtils.build(StorageSchemaBean.class)
										.with(StorageSchemaBean::processed, 
												BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
													.with(StorageSchemaBean.StorageSubSchemaBean::grouping_time_period, "1month")
												.done().get())
									.done().get()
								)
							.done().get())
					.done().get();
			
			assertEquals("2015-09", HfdsDataWriteService.getSuffix(then, test_bucket, IStorageService.StorageStage.processed));			
		}
	}
	
	/** Get some easy testing out the way
	 * 		HfdsDataWriteService.getBasePath
	 */
	@Test
	public void test_utilityMethods_getBasePath() {
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/static")
			.done().get();

		assertEquals("/root/test/static/managed_bucket/import/stored/raw/", HfdsDataWriteService.getBasePath("/root", bucket, IStorageService.StorageStage.raw));
		assertEquals("/root/test/static/managed_bucket/import/stored/json/", HfdsDataWriteService.getBasePath("/root", bucket, IStorageService.StorageStage.json));
		assertEquals("/root/test/static/managed_bucket/import/stored/processed/", HfdsDataWriteService.getBasePath("/root", bucket, IStorageService.StorageStage.processed));
		
	}
		
	/** Get some easy testing out the way
	 * 		HfdsDataWriteService.getExtension
	 */
	@Test
	public void test_utilityMethods_getExtension() {
		assertEquals("", HfdsDataWriteService.getExtension(IStorageService.StorageStage.raw));
		assertEquals(".json", HfdsDataWriteService.getExtension(IStorageService.StorageStage.json));
		assertEquals(".json", HfdsDataWriteService.getExtension(IStorageService.StorageStage.processed));
	}	
	
	/** Get some easy testing out the way
	 * 		HfdsDataWriteService.getCanonicalCodec
	 * 		HfdsDataWriteService.wrapOutputInCodec
	 */
	@Test
	public void test_utilityMethods_codecs() {
		
		// No codec
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/static")
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::storage_schema,
									BeanTemplateUtils.build(StorageSchemaBean.class)
										.with(StorageSchemaBean::processed, 
												BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
													.with(StorageSchemaBean.StorageSubSchemaBean::codec, "gzip")
												.done().get())
									.done().get()
								)
							.done().get())
					.done().get();
		
			OutputStream out_in = new ByteArrayOutputStream();
			
			Optional<String> test = HfdsDataWriteService.getCanonicalCodec(test_bucket.data_schema().storage_schema(), IStorageService.StorageStage.raw);
			assertEquals(Optional.empty(), test);
			
			final OutputStream out_out = HfdsDataWriteService.wrapOutputInCodec(test, out_in);
			assertEquals(out_in, out_out);
		}		
		// Malformed codec
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/static")
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::storage_schema,
									BeanTemplateUtils.build(StorageSchemaBean.class)
										.with(StorageSchemaBean::json, 
												BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
													.with(StorageSchemaBean.StorageSubSchemaBean::codec, "banana")
												.done().get())
									.done().get()
								)
							.done().get())
					.done().get();
		
			OutputStream out_in = new ByteArrayOutputStream();
			
			Optional<String> test = HfdsDataWriteService.getCanonicalCodec(test_bucket.data_schema().storage_schema(), IStorageService.StorageStage.json);
			assertEquals(Optional.of("banana"), test);
			
			final OutputStream out_out = HfdsDataWriteService.wrapOutputInCodec(test, out_in);
			assertEquals(out_in, out_out);
		}		
		// gz
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/static")
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::storage_schema,
									BeanTemplateUtils.build(StorageSchemaBean.class)
										.with(StorageSchemaBean::json, 
												BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
													.with(StorageSchemaBean.StorageSubSchemaBean::codec, "gzip")
												.done().get())
									.done().get()
								)
							.done().get())
					.done().get();
		
			OutputStream out_in = new ByteArrayOutputStream();
			
			Optional<String> test = HfdsDataWriteService.getCanonicalCodec(test_bucket.data_schema().storage_schema(), IStorageService.StorageStage.json);
			assertEquals(Optional.of("gz"), test);
			
			final OutputStream out_out = HfdsDataWriteService.wrapOutputInCodec(test, out_in);
			assertTrue("Stream is gzip: " + out_out.getClass().getSimpleName(), out_out instanceof java.util.zip.GZIPOutputStream);
		}		
		//gzip
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/static")
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::storage_schema,
									BeanTemplateUtils.build(StorageSchemaBean.class)
										.with(StorageSchemaBean::json, 
												BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
													.with(StorageSchemaBean.StorageSubSchemaBean::codec, "gz")
												.done().get())
									.done().get()
								)
							.done().get())
					.done().get();
		
			OutputStream out_in = new ByteArrayOutputStream();
			
			Optional<String> test = HfdsDataWriteService.getCanonicalCodec(test_bucket.data_schema().storage_schema(), IStorageService.StorageStage.json);
			assertEquals(Optional.of("gz"), test);
			
			final OutputStream out_out = HfdsDataWriteService.wrapOutputInCodec(test, out_in);
			assertTrue("Stream is gzip: " + out_out.getClass().getSimpleName(), out_out instanceof java.util.zip.GZIPOutputStream);
		}		
		//fr.sn
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/static")
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::storage_schema,
									BeanTemplateUtils.build(StorageSchemaBean.class)
										.with(StorageSchemaBean::json, 
												BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
													.with(StorageSchemaBean.StorageSubSchemaBean::codec, "fr.sz")
												.done().get())
									.done().get()
								)
							.done().get())
					.done().get();
		
			OutputStream out_in = new ByteArrayOutputStream();
			
			Optional<String> test = HfdsDataWriteService.getCanonicalCodec(test_bucket.data_schema().storage_schema(), IStorageService.StorageStage.json);
			assertEquals(Optional.of("fr.sz"), test);
			
			final OutputStream out_out = HfdsDataWriteService.wrapOutputInCodec(test, out_in);
			assertTrue("Stream is snappy framed: " + out_out.getClass().getSimpleName(), out_out instanceof org.xerial.snappy.SnappyFramedOutputStream);
		}		
		//snappy_frame
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/static")
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::storage_schema,
									BeanTemplateUtils.build(StorageSchemaBean.class)
										.with(StorageSchemaBean::json, 
												BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
													.with(StorageSchemaBean.StorageSubSchemaBean::codec, "snappy_framed")
												.done().get())
									.done().get()
								)
							.done().get())
					.done().get();
		
			OutputStream out_in = new ByteArrayOutputStream();
			
			Optional<String> test = HfdsDataWriteService.getCanonicalCodec(test_bucket.data_schema().storage_schema(), IStorageService.StorageStage.json);
			assertEquals(Optional.of("fr.sz"), test);
			
			final OutputStream out_out = HfdsDataWriteService.wrapOutputInCodec(test, out_in);
			assertTrue("Stream is snappy framed: " + out_out.getClass().getSimpleName(), out_out instanceof org.xerial.snappy.SnappyFramedOutputStream);
		}		
		//sn
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/static")
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::storage_schema,
									BeanTemplateUtils.build(StorageSchemaBean.class)
										.with(StorageSchemaBean::json, 
												BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
													.with(StorageSchemaBean.StorageSubSchemaBean::codec, "sz")
												.done().get())
									.done().get()
								)
							.done().get())
					.done().get();
		
			OutputStream out_in = new ByteArrayOutputStream();
			
			Optional<String> test = HfdsDataWriteService.getCanonicalCodec(test_bucket.data_schema().storage_schema(), IStorageService.StorageStage.json);
			assertEquals(Optional.of("sz"), test);
			
			final OutputStream out_out = HfdsDataWriteService.wrapOutputInCodec(test, out_in);
			assertTrue("Stream is snappy: " + out_out.getClass().getSimpleName(), out_out instanceof org.xerial.snappy.SnappyOutputStream);
		}		
		//snappy
		{
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/static")
					.with(DataBucketBean::data_schema,
							BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::storage_schema,
									BeanTemplateUtils.build(StorageSchemaBean.class)
										.with(StorageSchemaBean::json, 
												BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
													.with(StorageSchemaBean.StorageSubSchemaBean::codec, "snappy")
												.done().get())
									.done().get()
								)
							.done().get())
					.done().get();
		
			OutputStream out_in = new ByteArrayOutputStream();
			
			Optional<String> test = HfdsDataWriteService.getCanonicalCodec(test_bucket.data_schema().storage_schema(), IStorageService.StorageStage.json);
			assertEquals(Optional.of("sz"), test);
			
			final OutputStream out_out = HfdsDataWriteService.wrapOutputInCodec(test, out_in);
			assertTrue("Stream is snappy: " + out_out.getClass().getSimpleName(), out_out instanceof org.xerial.snappy.SnappyOutputStream);
		}		
		
		
	}	
	
	public static class TestBean {
		public String _id;
		public String value;
	}
	
	@Test
	public void test_workerUtilities() {
		
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		GlobalPropertiesBean globals = BeanTemplateUtils.build(GlobalPropertiesBean.class)
											.with(GlobalPropertiesBean::distributed_root_dir, temp_dir)
											.with(GlobalPropertiesBean::local_yarn_config_dir, System.getenv("HADOOP_CONF_DIR")).done().get();
	
		MockHdfsStorageService storage_service = new MockHdfsStorageService(globals);

		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/static")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::storage_schema,
								BeanTemplateUtils.build(StorageSchemaBean.class)
									.with(StorageSchemaBean::processed, 
											BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
												.with(StorageSchemaBean.StorageSubSchemaBean::codec, "snappy")
											.done().get())
								.done().get()
							)
						.done().get())
				.done().get();
		
		HfdsDataWriteService<TestBean> write_service = new HfdsDataWriteService<>(test_bucket, IStorageService.StorageStage.processed, storage_service);
		
		// First off a bunch of top level trivial calls
		{			
			try {
				write_service.deleteDatastore();
				fail("Should have errored on deleteDatastore");
			}
			catch (Exception e) {}
			
			try {
				write_service.getCrudService();
				fail("Should have errored on getCrudService");
			}
			catch (Exception e) {}
			
			CompletableFuture<Long> cf = write_service.countObjects();
			try {
				cf.get();
				fail("Should have errored on getCrudService");
			}
			catch (Exception e) {}
			
			HfdsDataWriteService<JsonNode> write_service_json = (HfdsDataWriteService<JsonNode> )write_service.getRawService();
			assertEquals(write_service_json._bucket, write_service._bucket);
			
			assertEquals(Optional.empty(), write_service.getUnderlyingPlatformDriver(String.class, Optional.empty()));
		}

		//TODO: now the worker stuff:
	}
	
	//TODO: test the writing logic
	
	//TODO: test setting the number of threads
}
