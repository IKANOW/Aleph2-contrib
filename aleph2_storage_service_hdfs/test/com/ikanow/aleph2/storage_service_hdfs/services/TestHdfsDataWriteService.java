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
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService.IBatchSubservice;
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

			assertEquals(IStorageService.NO_TIME_SUFFIX, HfdsDataWriteService.getSuffix(then, bucket, IStorageService.StorageStage.raw));			
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
			assertEquals(IStorageService.NO_TIME_SUFFIX, HfdsDataWriteService.getSuffix(then, test_bucket, IStorageService.StorageStage.raw));			
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
			
			assertEquals(IStorageService.NO_TIME_SUFFIX, HfdsDataWriteService.getSuffix(then, test_bucket, IStorageService.StorageStage.json));			
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

		assertEquals("/root/test/static/managed_bucket/import/stored/raw/current/", HfdsDataWriteService.getBasePath("/root", bucket, IStorageService.StorageStage.raw, Optional.empty(), "current/"));
		assertEquals("/root/test/static/managed_bucket/import/stored/raw/ping", HfdsDataWriteService.getBasePath("/root", bucket, IStorageService.StorageStage.raw, Optional.empty(), "ping"));
		assertEquals("/root/test/static/managed_bucket/import/stored/json/current/", HfdsDataWriteService.getBasePath("/root", bucket, IStorageService.StorageStage.json, Optional.empty(), "current/"));
		assertEquals("/root/test/static/managed_bucket/import/stored/json/pong", HfdsDataWriteService.getBasePath("/root", bucket, IStorageService.StorageStage.json, Optional.empty(), "pong"));
		assertEquals("/root/test/static/managed_bucket/import/stored/processed/current/", HfdsDataWriteService.getBasePath("/root", bucket, IStorageService.StorageStage.processed, Optional.empty(), "current/"));
		assertEquals("/root/test/static/managed_bucket/import/stored/processed/other", HfdsDataWriteService.getBasePath("/root", bucket, IStorageService.StorageStage.processed, Optional.empty(), "other"));
	
		//TODO (ALEPH-12): transient output
	}
		
	//TODO (ALEPH-12): simple duplicate of end-end for transient output
	
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
		public TestBean(String a, String b) { _id = a; value = b; }
		public String _id;
		public String value;
	}
	
	protected HfdsDataWriteService<TestBean> getWriter(String name) {
		return getWriter(name, Optional.empty());
	}
	protected HfdsDataWriteService<TestBean> getWriter(String name, Optional<String> secondary) {
		
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		GlobalPropertiesBean globals = BeanTemplateUtils.build(GlobalPropertiesBean.class)
											.with(GlobalPropertiesBean::distributed_root_dir, temp_dir)
											.with(GlobalPropertiesBean::local_yarn_config_dir, System.getenv("HADOOP_CONF_DIR")).done().get();
	
		MockHdfsStorageService storage_service = new MockHdfsStorageService(globals);

		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, name)
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::storage_schema,
								BeanTemplateUtils.build(StorageSchemaBean.class)
									.with(StorageSchemaBean::processed, 
											BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
												//(no compression)
												//.with(StorageSchemaBean.StorageSubSchemaBean::codec, "snappy")
											.done().get())
								.done().get()
							)
						.done().get())
				.done().get();
		
		HfdsDataWriteService<TestBean> write_service = new HfdsDataWriteService<>(test_bucket, IStorageService.StorageStage.processed, Optional.empty(), storage_service, secondary);
		
		return write_service;
	}
	
	@Test
	public void test_writerService_basics() {
		
		HfdsDataWriteService<TestBean> write_service = getWriter("/test/writer/basics");
		
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

		// Check the batch service isn't loaded
		assertTrue("Writer not set", !write_service._writer.isSet());		
	}
	
	@Test
	public void test_writerService_worker() throws Exception {
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;		
		
		HfdsDataWriteService<TestBean> write_service = getWriter("/test/writer/worker");

		//(Tidy up)
		try { FileUtils.deleteDirectory(new File(temp_dir + "/data/" + write_service._bucket.full_name())); } catch (Exception e) {}
		
		HfdsDataWriteService<TestBean>.WriterWorker worker = write_service.new WriterWorker();
		
		// (no codec because this is called first)
		assertEquals(HfdsDataWriteService._process_id + "_" + worker._thread_id + "_1.json", worker.getFilename());
		
		// Check complete segment does nothing if no data has been written
		{
			worker.new_segment();
			
			File f = new File(
					(temp_dir + "/data/" + write_service._bucket.full_name() + "/managed_bucket/import/stored/processed/current/.spooldir/" + worker.getFilename())
					.replace("/", File.separator)
					);
			assertTrue("File should exist: " + f, f.exists());
			assertTrue("Expected segment: ", f.toString().endsWith("_1.json"));
					
			worker.complete_segment();
			
			assertTrue("File should not have moved: " + f, f.exists());
	
			File f2 = new File(
					(temp_dir + "/data/" + write_service._bucket.full_name() + "/managed_bucket/import/stored/processed/current/all_time/" + worker.getFilename())
					.replace("/", File.separator)
					);
			assertTrue("File should not exist: " + f2, !f2.exists());
		}
		// Check writes + non-empty segment
		{
			TestBean t1 = new TestBean("t1", "v1");
			TestBean t2 = new TestBean("t2", "v2");
			
			worker.new_segment();
			
			File f = new File(
					(temp_dir + "/data/" + write_service._bucket.full_name() + "/managed_bucket/import/stored/processed/current/.spooldir/" + worker.getFilename())
					.replace("/", File.separator)
					);
			assertTrue("File should exist: " + f, f.exists());
			assertTrue("Expected segment: ", f.toString().endsWith("_1.json"));
					
			// Write some objects out:
			
			worker.write("TEST1");
			worker.write("TEST2\n");
			worker.write(t1);
			worker.write(BeanTemplateUtils.toJson(t2));
			
			worker.complete_segment();
			
			assertTrue("File should have moved: " + f, !f.exists());
	
			File f2 = new File(
					(temp_dir + "/data/" + write_service._bucket.full_name() + "/managed_bucket/import/stored/processed/current/all_time/" + f.getName())
					.replace("/", File.separator)
					);
			assertTrue("File should exist: " + f2, f2.exists());
			assertTrue("Expected segment: ", f2.toString().endsWith("_1.json"));
			
			assertEquals("TEST1\nTEST2\n{\"_id\":\"t1\",\"value\":\"v1\"}\n{\"_id\":\"t2\",\"value\":\"v2\"}\n", FileUtils.readFileToString(f2));
		}		
		// Check basic write + second segment (list)
		{
			TestBean t1 = new TestBean("t1b", "v1b");
			TestBean t2 = new TestBean("t2b", "v2b");			
			
			worker.new_segment();
			
			File f = new File(
					(temp_dir + "/data/" + write_service._bucket.full_name() + "/managed_bucket/import/stored/processed/current/.spooldir/" + worker.getFilename())
					.replace("/", File.separator)
					);
			assertTrue("File should exist: " + f, f.exists());
			assertTrue("Expected segment: ", f.toString().endsWith("_2.json"));
					
			// Write some object out:
			
			worker.write(Arrays.asList("TEST1b", "TEST2b\n", t1, BeanTemplateUtils.toJson(t2)
					));
			
			worker.complete_segment();
			
			assertTrue("File should have moved: " + f, !f.exists());
	
			File f2 = new File(
					(temp_dir + "/data/" + write_service._bucket.full_name() + "/managed_bucket/import/stored/processed/current/all_time/" + f.getName())
					.replace("/", File.separator)
					);
			assertTrue("File should exist: " + f2, f2.exists());
			assertTrue("Expected segment: ", f2.toString().endsWith("_2.json"));
						
			assertEquals("TEST1b\nTEST2b\n{\"_id\":\"t1b\",\"value\":\"v1b\"}\n{\"_id\":\"t2b\",\"value\":\"v2b\"}\n", FileUtils.readFileToString(f2));
		}
	}		

	@Test
	public void test_writerService_segmentationCriteria() throws Exception {
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;		
		
		HfdsDataWriteService<TestBean> write_service = getWriter("/test/writer/segmentation");
		
		//(Tidy up)
		try { FileUtils.deleteDirectory(new File(temp_dir + "/data/" + write_service._bucket.full_name())); } catch (Exception e) {}
		
		HfdsDataWriteService<TestBean>.WriterWorker worker = write_service.new WriterWorker();
		
		worker.new_segment();
		
		assertTrue("No new segment", !worker.check_segment(100, 100, 1000));
		
		worker._state.curr_objects = 101;
		assertTrue("New segment on num", worker.check_segment(100, 100, 1000));
		assertTrue("New segment on num b", !worker.check_segment(102, 100, 1000));
		
		worker._state.curr_size_b = 101;
		assertTrue("New segment on size", worker.check_segment(102, 100, 1000));
		assertTrue("New segment on size b", !worker.check_segment(102, 102, 1000));
		
		Thread.sleep(100);
		assertTrue("New segment on time", worker.check_segment(102, 102, 50));
		
		worker._state.last_segmented = System.currentTimeMillis() + 1000;
		assertTrue("New segment on time b", worker.check_segment(102, 102, 100000L));
	}	
	
	@Test
	public void test_writerService_end2end_primary() throws InterruptedException, ExecutionException {
		test_writerService_end2end(Optional.empty());
	}
	@Test
	public void test_writerService_end2end_secondary() throws InterruptedException, ExecutionException {
		test_writerService_end2end(Optional.of("secondary_test"));
	}
	
	public void test_writerService_end2end(Optional<String> secondary) throws InterruptedException, ExecutionException {
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;		
		HfdsDataWriteService<TestBean> write_service = getWriter("/test/writer/end2end/" + secondary.orElse("current") + "/", secondary);

		//(Tidy up)
		try { FileUtils.deleteDirectory(new File(temp_dir + "/data/" + write_service._bucket.full_name())); } catch (Exception e) {}
		
		// Check lazy initialization only kicks in once		
		Optional<IBatchSubservice<TestBean>> x = write_service.getBatchWriteSubservice();
		assertEquals(x.get(), write_service._writer.get());		
		Optional<IBatchSubservice<TestBean>> y = write_service.getBatchWriteSubservice();
		assertEquals(x.get(), y.get());		

		IBatchSubservice<TestBean> batch = x.get();
		
		// Set up properties for testing:
		batch.setBatchProperties(Optional.of(1000), Optional.of(1000L), Optional.of(Duration.ofSeconds(2L)), Optional.of(3));
		
		Thread.sleep(1000L);
		// Check there are now 3 threads
		assertEquals(3, write_service._writer.get()._state._workers.getActiveCount());
	
		for (int i = 0; i < 20; ++i) {
			TestBean emit = new TestBean("id" + i, "val" + i);
			if (0 == (i % 2)) {
				if (0 == ((i/2) % 2)) {
					batch.storeObject(emit);
				}
				else {
					CompletableFuture<Supplier<Object>> cf = write_service.storeObject(emit);
					assertEquals(null, cf.get().get());
				}
			}
			else {
				if (0 == ((i/2) % 2)) {
					batch.storeObjects(Arrays.asList(emit));
				}
				else {
					CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> cf = write_service.storeObjects(Arrays.asList(emit));	
					assertEquals(Collections.emptyList(), cf.get()._1().get());
					assertEquals(1L, cf.get()._2().get().longValue());
				}
			}
		}
		Thread.sleep(500L);
		// Check that initially the files are stored locally
		File init_dir = new File(
				(temp_dir + "/data/" + write_service._bucket.full_name() + "/managed_bucket/import/stored/processed/"+secondary.orElse("current")+"/.spooldir/")
				.replace("/", File.separator)
				);
		File final_dir = new File(
				(temp_dir + "/data/" + write_service._bucket.full_name() + "/managed_bucket/import/stored/processed/"+secondary.orElse("current")+"/all_time/")
				.replace("/", File.separator)
				);
		assertEquals("Needs to have 6 files, including 3x .crc: " + Arrays.toString(init_dir.list()), 6, init_dir.list().length); //*2 because CRC
		assertTrue("Nothing in final dir: " + (final_dir.exists()?Arrays.toString(final_dir.list()):"(non-exist)"), !final_dir.exists()|| final_dir.list().length == 0);

		int ii = 1;
		for (; ii <= 50; ++ii) {
			Thread.sleep(2500L);
			if (0 == init_dir.list().length) {
				break;
			}
		}
		System.out.println("(exited from file system check after " + ii*2.5 + " s)");

		assertEquals(0, init_dir.list().length); //*2 because CRC
		assertEquals(6, final_dir.list().length); //*2 because CRC		
		
		// Change batch properties so that will segment (also check number of threads reduces)
		batch.setBatchProperties(Optional.of(10), Optional.of(1000L), Optional.of(Duration.ofSeconds(5L)), Optional.of(1));
		List<TestBean> l1 = IntStream.range(0, 8).boxed().map(i -> new TestBean("id" + i, "val" + i)).collect(Collectors.toList());
		List<TestBean> l2 = IntStream.range(8, 15).boxed().map(i -> new TestBean("id" + i, "val" + i)).collect(Collectors.toList());
		
		batch.storeObjects(l1);
		Thread.sleep(750L);
		assertEquals(6, final_dir.list().length); //*2 because CRC		
		System.out.println("Found: 6 files: " + Arrays.stream(final_dir.list()).collect(Collectors.joining(";")));		
		
		batch.storeObjects(l2);
		System.out.println("Added 7 more objects at " + new Date());
		for (int jj = 0; jj < 5; ++jj) {
			Thread.sleep(1500L);
			if (final_dir.list().length > 6) break;
		}
		System.out.println("(Check init dir cleared: " + Arrays.stream(init_dir.list()).collect(Collectors.joining(";")) + ")");
		assertEquals("Should have 8 files: " + Arrays.stream(final_dir.list()).collect(Collectors.joining(";")), 8, final_dir.list().length); //*2 because CRC		
	}
}
