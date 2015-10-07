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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.http.impl.cookie.DateUtils;
import org.junit.Test;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.TimeUtils;

public class TestMockHdfsStorageSystem {
	
	@Test
	public void test(){
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		GlobalPropertiesBean globals = BeanTemplateUtils.build(GlobalPropertiesBean.class)
											.with(GlobalPropertiesBean::distributed_root_dir, temp_dir)
											.with(GlobalPropertiesBean::local_yarn_config_dir, System.getenv("HADOOP_CONF_DIR")).done().get();
	
		MockHdfsStorageService storageService = new MockHdfsStorageService(globals);
	
		assertEquals(globals.distributed_root_dir(), storageService.getRootPath());
		assertEquals(1, storageService.getUnderlyingArtefacts().size());
		
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
	public void test_validate(){
		
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		GlobalPropertiesBean globals = BeanTemplateUtils.build(GlobalPropertiesBean.class)
											.with(GlobalPropertiesBean::distributed_root_dir, temp_dir)
											.with(GlobalPropertiesBean::local_yarn_config_dir, System.getenv("HADOOP_CONF_DIR")).done().get();
	
		MockHdfsStorageService storageService = new MockHdfsStorageService(globals);
	
		// Works
		{
			final DataBucketBean bucket = 
					BeanTemplateUtils.build(DataBucketBean.class)
						.with(DataBucketBean::full_name, "/test/validate/bucket")
					.done().get();
			
			Tuple2<String, List<BasicMessageBean>> res = storageService.validateSchema(null, bucket);
			assertEquals("Validation: " + res._2().stream().map(BasicMessageBean::message).collect(Collectors.joining("\n")), 0, res._2().size());
			assertEquals((temp_dir.replace(File.separator,  "/") + "/data/test/validate/bucket/managed_bucket/").replaceAll("//", "/"), 
							res._1().replace(File.separator, "/").replaceAll("//", "/"));
		}
		// Works some more
		
		Arrays.asList("gz", "gzip", "sz", "snappy", "fr.sz", "snappy_framed")
				.stream()
				.map(s -> buildBucketWithCodec(s))
				.forEach(bucket -> {
					Tuple2<String, List<BasicMessageBean>> res = storageService.validateSchema(bucket.data_schema().storage_schema(), bucket);
					assertEquals("Validation: " + res._2().stream().map(BasicMessageBean::message).collect(Collectors.joining("\n")), 0, res._2().size());
					assertEquals((temp_dir.replace(File.separator,  "/") + "/data/" + bucket.full_name() + IStorageService.BUCKET_SUFFIX).replaceAll("//", "/"), 
							res._1().replace(File.separator, "/").replaceAll("//", "/"));
				});
		
		// Fails
		
		Arrays.asList("banana")
				.stream()
				.map(s -> buildBucketWithCodec(s))
				.forEach(bucket -> {
					Tuple2<String, List<BasicMessageBean>> res = storageService.validateSchema(bucket.data_schema().storage_schema(), bucket);
					assertEquals("Validation: " + res._2().stream().map(BasicMessageBean::message).collect(Collectors.joining("\n")), 1, res._2().size());
					assertEquals("", res._1());
				});
	}

	private DataBucketBean buildBucketWithCodec(String codec) {
		return BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/bucket/codec/" + codec)
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::storage_schema,
								BeanTemplateUtils.build(StorageSchemaBean.class)
									.with(StorageSchemaBean::json, 
											BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
												.with(StorageSchemaBean.StorageSubSchemaBean::codec, codec)
											.done().get())
								.done().get()
							)
						.done().get())
				.done().get();
	}	
	
	@Test
	public void test_handleBucketDeletionRequest() throws InterruptedException, ExecutionException, IOException {
		// 0) Setup
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		final GlobalPropertiesBean globals = BeanTemplateUtils.build(GlobalPropertiesBean.class)
				.with(GlobalPropertiesBean::local_yarn_config_dir, temp_dir)
				.with(GlobalPropertiesBean::distributed_root_dir, temp_dir)
				.with(GlobalPropertiesBean::local_root_dir, temp_dir)
				.with(GlobalPropertiesBean::local_cached_jar_dir, temp_dir)
				.done().get();
		
		final MockHdfsStorageService storage_service = new MockHdfsStorageService(globals);
		
		// 1) Set up bucket (code taken from management_db_service)
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test/delete/bucket").done().get();
		FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + "/data/" + File.separator + bucket.full_name()));		
		setup_bucket(storage_service, bucket, Collections.emptyList());
		final String bucket_path = System.getProperty("java.io.tmpdir") + File.separator + "/data/" + File.separator + bucket.full_name();
		assertTrue("The file path has been created", new File(bucket_path + "/managed_bucket").exists());
		FileUtils.writeStringToFile(new File(bucket_path + IStorageService.STORED_DATA_SUFFIX + "/test"), "");
		assertTrue("The extra file path has been created", new File(bucket_path + IStorageService.STORED_DATA_SUFFIX + "/test").exists());		
		
		final CompletableFuture<BasicMessageBean> res1 = storage_service.getDataService().get().handleBucketDeletionRequest(bucket, Optional.empty(), false);
		assertEquals(true, res1.get().success());
		
		// Test:
		
		// Full filesystem exists
		assertTrue("The file path has *not* been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "/data/" + File.separator + bucket.full_name() + "/managed_bucket").exists());
		
		// Data directories no longer exist
		assertFalse("The data path has been deleted", new File(System.getProperty("java.io.tmpdir") + File.separator + "/data/" + File.separator + bucket.full_name() + IStorageService.STORED_DATA_SUFFIX + "/test").exists());
				
		// Check nothing goes wrong when bucket doesn't exist
		final DataBucketBean bucket2 = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test/delete/bucket_not_exist").done().get();
		FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + "/data/" + File.separator + bucket2.full_name()));
		assertFalse("The file path for bucket2 does not exist", new File(System.getProperty("java.io.tmpdir") + File.separator + "/data/" + File.separator + bucket2.full_name()).exists());
		
		final CompletableFuture<BasicMessageBean> res2 = storage_service.getDataService().get().handleBucketDeletionRequest(bucket2, Optional.empty(), false);
		assertEquals(true, res2.get().success());
		
		//(check didn't create anything)
		
		// Full filesystem exists
		assertFalse("No bucket2 paths were created", new File(System.getProperty("java.io.tmpdir") + File.separator + "/data/" + File.separator + bucket2.full_name()).exists());		
	}
	
	@Test
	public void test_ageOut() throws IOException, InterruptedException, ExecutionException {
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
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
											.with(DataBucketBean::full_name, "/test/age/out/bucket")
											.with(DataBucketBean::data_schema,
													BeanTemplateUtils.build(DataSchemaBean.class)
														.with(DataSchemaBean::storage_schema,
															BeanTemplateUtils.build(StorageSchemaBean.class)
																.with(StorageSchemaBean::raw, 
																		BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
																			.with(StorageSchemaBean.StorageSubSchemaBean::exist_age_max, "9 days")
																		.done().get())
																.with(StorageSchemaBean::json, 
																		BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
																			.with(StorageSchemaBean.StorageSubSchemaBean::exist_age_max, "6 days")
																		.done().get())
																.with(StorageSchemaBean::processed, 
																		BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
																			.with(StorageSchemaBean.StorageSubSchemaBean::exist_age_max, "1 week")
																		.done().get())
															.done().get()
														)
													.done().get())
										.done().get();
		
		FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + File.separator + "/data/" + File.separator + bucket.full_name()));		
		setup_bucket(storage_service, bucket, Collections.emptyList());
		final String bucket_path = System.getProperty("java.io.tmpdir") + File.separator + "/data/" + File.separator + bucket.full_name();
		assertTrue("The file path has been created", new File(bucket_path + "/managed_bucket").exists());

		final long now = new Date().getTime();
		IntStream.range(4, 10).boxed().map(i -> now - (i*1000L*3600L*24L))
			.forEach(Lambdas.wrap_consumer_u(n -> {
				final String pattern = TimeUtils.getTimeBasedSuffix(TimeUtils.getTimePeriod("1 day").success(), Optional.empty());
				final String dir = DateUtils.formatDate(new Date(n), pattern);
				
				FileUtils.forceMkdir(new File(bucket_path + "/" + IStorageService.STORED_DATA_SUFFIX_RAW + "/" + dir));
				FileUtils.forceMkdir(new File(bucket_path + "/" + IStorageService.STORED_DATA_SUFFIX_JSON + "/" + dir));
				FileUtils.forceMkdir(new File(bucket_path + "/" + IStorageService.STORED_DATA_SUFFIX_PROCESSED + "/" + dir));
			}));
		
		// (7 cos includes root)
		assertEquals(7, FileUtils.listFilesAndDirs(new File(bucket_path + "/" + IStorageService.STORED_DATA_SUFFIX_RAW), DirectoryFileFilter.DIRECTORY, TrueFileFilter.INSTANCE).size());
		assertEquals(7, FileUtils.listFilesAndDirs(new File(bucket_path + "/" + IStorageService.STORED_DATA_SUFFIX_JSON), DirectoryFileFilter.DIRECTORY, TrueFileFilter.INSTANCE).size());
		assertEquals(7, FileUtils.listFilesAndDirs(new File(bucket_path + "/" + IStorageService.STORED_DATA_SUFFIX_PROCESSED), DirectoryFileFilter.DIRECTORY, TrueFileFilter.INSTANCE).size());
		
		// 1) Normal run:
		
		CompletableFuture<BasicMessageBean> cf = storage_service.getDataService().get().handleAgeOutRequest(bucket);
		
		BasicMessageBean res = cf.get();
		
		assertEquals(true, res.success());
		assertTrue("sensible message: " + res.message(), res.message().contains("raw: deleted 1 "));
		assertTrue("sensible message: " + res.message(), res.message().contains("json: deleted 4 "));
		assertTrue("sensible message: " + res.message(), res.message().contains("processed: deleted 3 "));

		assertTrue("Message marked as loggable: " + res.details(), Optional.ofNullable(res.details()).filter(m -> m.containsKey("loggable")).isPresent());
		
		System.out.println("Return from to delete: " + res.message());		
		
		//(+1 including root)
		assertEquals(6, FileUtils.listFilesAndDirs(new File(bucket_path + "/" + IStorageService.STORED_DATA_SUFFIX_RAW), DirectoryFileFilter.DIRECTORY, TrueFileFilter.INSTANCE).size());
		assertEquals(3, FileUtils.listFilesAndDirs(new File(bucket_path + "/" + IStorageService.STORED_DATA_SUFFIX_JSON), DirectoryFileFilter.DIRECTORY, TrueFileFilter.INSTANCE).size());
		assertEquals(4, FileUtils.listFilesAndDirs(new File(bucket_path + "/" + IStorageService.STORED_DATA_SUFFIX_PROCESSED), DirectoryFileFilter.DIRECTORY, TrueFileFilter.INSTANCE).size());
		
		// 2) Run it again, returns success but not loggable:
		
		CompletableFuture<BasicMessageBean> cf2 = storage_service.getDataService().get().handleAgeOutRequest(bucket);
		
		BasicMessageBean res2 = cf2.get();
		
		assertEquals(true, res2.success());
		assertTrue("sensible message: " + res2.message(), res2.message().contains("raw: deleted 0 "));
		assertTrue("sensible message: " + res2.message(), res2.message().contains("json: deleted 0 "));
		assertTrue("sensible message: " + res2.message(), res2.message().contains("processed: deleted 0 "));
		assertTrue("Message _not_ marked as loggable: " + res2.details(), !Optional.ofNullable(res2.details()).map(m -> m.get("loggable")).isPresent());
				
		// 3) No temporal settings
		
		final DataBucketBean bucket3 = BeanTemplateUtils.build(DataBucketBean.class)
				.with("full_name", "/test/handle/age/out/delete/not/temporal")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
						.done().get())
				.done().get();
		
		CompletableFuture<BasicMessageBean> cf3 = storage_service.getDataService().get().handleAgeOutRequest(bucket3);		
		BasicMessageBean res3 = cf3.get();
		// no temporal settings => returns success
		assertEquals(true, res3.success());
		
		// 4) Unparseable temporal settings (in theory won't validate but we can test here)

		final DataBucketBean bucket4 = BeanTemplateUtils.build(DataBucketBean.class)
				.with("full_name", "/test/handle/age/out/delete/temporal/malformed")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::storage_schema,
								BeanTemplateUtils.build(StorageSchemaBean.class)
									.with(StorageSchemaBean::json, 
											BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
												.with(StorageSchemaBean.StorageSubSchemaBean::exist_age_max, "bananas")
											.done().get())
								.done().get()
							)
						.done().get())
				.done().get();
		
		CompletableFuture<BasicMessageBean> cf4 = storage_service.getDataService().get().handleAgeOutRequest(bucket4);		
		BasicMessageBean res4 = cf4.get();
		// no temporal settings => returns success
		assertEquals(false, res4.success());
		
	}
	
	@Test
	public void test_getDataWriter() {
		// 0) Setup
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		final GlobalPropertiesBean globals = BeanTemplateUtils.build(GlobalPropertiesBean.class)
				.with(GlobalPropertiesBean::local_yarn_config_dir, temp_dir)
				.with(GlobalPropertiesBean::distributed_root_dir, temp_dir)
				.with(GlobalPropertiesBean::local_root_dir, temp_dir)
				.with(GlobalPropertiesBean::distributed_root_dir, temp_dir)
				.done().get();
		
		final MockHdfsStorageService storage_service = new MockHdfsStorageService(globals);
		
		// 1) Set up buckets
		
		final DataBucketBean bucket_no_storage = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/storage/bucket")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
					.done().get())
				.done().get();
		
		
		final DataBucketBean bucket_full = BeanTemplateUtils.build(DataBucketBean.class)
											.with(DataBucketBean::full_name, "/test/storage/bucket")
											.with(DataBucketBean::data_schema,
													BeanTemplateUtils.build(DataSchemaBean.class)
														.with(DataSchemaBean::storage_schema,
															BeanTemplateUtils.build(StorageSchemaBean.class)
																//(no enabled defaults to true)
																.with(StorageSchemaBean::raw, 
																		BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
																			.with(StorageSchemaBean.StorageSubSchemaBean::enabled, false)
																		.done().get())
																.with(StorageSchemaBean::json, 
																		BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
																			.with(StorageSchemaBean.StorageSubSchemaBean::enabled, false)
																		.done().get())
																.with(StorageSchemaBean::processed, 
																		BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
																			// (no enabled, defaults to true)
																		.done().get())
															.done().get()
														)
													.done().get())
										.done().get();
		
		final DataBucketBean bucket_disabled = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/storage/bucket")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
							.with(DataSchemaBean::storage_schema,
								BeanTemplateUtils.build(StorageSchemaBean.class)
									.with(StorageSchemaBean::enabled, false)
									.with(StorageSchemaBean::raw, 
											BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
												.with(StorageSchemaBean.StorageSubSchemaBean::enabled, true)
											.done().get())
									.with(StorageSchemaBean::json, 
											BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
												.with(StorageSchemaBean.StorageSubSchemaBean::enabled, true)
											.done().get())
									.with(StorageSchemaBean::processed, 
											BeanTemplateUtils.build(StorageSchemaBean.StorageSubSchemaBean.class)
												// (no enabled, defaults to true)
											.done().get())
								.done().get()
							)
						.done().get())
			.done().get();
		
		// Try with invalid stage:

		try {
			storage_service.getDataService().flatMap(ds -> ds.getWritableDataService(JsonNode.class, bucket_full, Optional.of("banana"), Optional.empty()));
			fail("Should have thrown exception");
		}
		catch (Exception e) {}
		
		// Check some disabled cases:
		
		assertEquals(Optional.empty(), storage_service.getDataService().flatMap(ds -> ds.getWritableDataService(JsonNode.class, bucket_no_storage, Optional.of("processed"), Optional.empty())));
		assertEquals(Optional.empty(), storage_service.getDataService().flatMap(ds -> ds.getWritableDataService(JsonNode.class, bucket_disabled, Optional.of("processed"), Optional.empty())));
		assertEquals(Optional.empty(), storage_service.getDataService().flatMap(ds -> ds.getWritableDataService(JsonNode.class, bucket_full, Optional.of("json"), Optional.empty())));
		
		// Check the default case:
		
		assertTrue("Returns processing",
				storage_service.getDataService().flatMap(ds -> ds.getWritableDataService(JsonNode.class, bucket_full, Optional.of("processed"), Optional.empty())).isPresent()
				);
		assertTrue("No JSON",
				!storage_service.getDataService().flatMap(ds -> ds.getWritableDataService(JsonNode.class, bucket_full, Optional.of("json"), Optional.empty())).isPresent()
				);
		assertTrue("No raw",
				!storage_service.getDataService().flatMap(ds -> ds.getWritableDataService(JsonNode.class, bucket_full, Optional.of("raw"), Optional.empty())).isPresent()
				);
		assertTrue("Default returns processing",
				storage_service.getDataService().flatMap(ds -> ds.getWritableDataService(JsonNode.class, bucket_full, Optional.empty(), Optional.empty())).isPresent()
				);
	}
	
	@Test
	public void test_basic_secondaryBuffers() throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, IllegalArgumentException, IOException {
		// 0) Setup
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		final GlobalPropertiesBean globals = BeanTemplateUtils.build(GlobalPropertiesBean.class)
				.with(GlobalPropertiesBean::local_yarn_config_dir, temp_dir)
				.with(GlobalPropertiesBean::distributed_root_dir, temp_dir)
				.with(GlobalPropertiesBean::local_root_dir, temp_dir)
				.with(GlobalPropertiesBean::distributed_root_dir, temp_dir)
				.done().get();
		
		final MockHdfsStorageService storage_service = new MockHdfsStorageService(globals);
		
		// Some buckets
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/storage/bucket")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
					.done().get())
				.done().get();		
		
		setup_bucket(storage_service, bucket, Collections.emptyList());
		
		// Get primary buffer doesn't work:
		
		assertFalse(storage_service.getDataService().get().getPrimaryBufferName(bucket).isPresent());		
		
		// Add some secondary buffers and check they get picked up
		
		final FileContext dfs = storage_service.getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
		final String bucket_root = storage_service.getBucketRootPath() + "/" + bucket.full_name();		
		dfs.mkdir(new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_RAW_SECONDARY + "test1"), FsPermission.getDirDefault(), true);
		//(skip the current dir once just to check it doesn't cause problems)
		dfs.mkdir(new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_JSON_SECONDARY + "test2"), FsPermission.getDirDefault(), true);
		dfs.mkdir(new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_JSON), FsPermission.getDirDefault(), true);
		dfs.mkdir(new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_PROCESSED_SECONDARY + "test3"), FsPermission.getDirDefault(), true);
		dfs.mkdir(new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_PROCESSED), FsPermission.getDirDefault(), true);
		
		assertEquals(Arrays.asList("test1", "test2", "test3"), storage_service.getDataService().get().getSecondaryBufferList(bucket).stream().sorted().collect(Collectors.toList()));

		//(check dedups)
		dfs.mkdir(new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_JSON_SECONDARY + "test1"), FsPermission.getDirDefault(), true);
		
		assertEquals(Arrays.asList("test1", "test2", "test3"), storage_service.getDataService().get().getSecondaryBufferList(bucket).stream().sorted().collect(Collectors.toList()));
		
		try {
			dfs.delete(new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_PROCESSED_SECONDARY + "test3"), true);
		}
		catch (Exception e) {}
		
		assertEquals(Arrays.asList("test1", "test2"), storage_service.getDataService().get().getSecondaryBufferList(bucket).stream().sorted().collect(Collectors.toList()));
		
		try {
			dfs.delete(new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_RAW_SECONDARY + "test1"), true);
		}
		catch (Exception e) {}
		
		assertEquals(Arrays.asList("test1", "test2"), storage_service.getDataService().get().getSecondaryBufferList(bucket).stream().sorted().collect(Collectors.toList()));
	}
	
	@Test
	public void test_switching_secondaryBuffers() throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, IllegalArgumentException, IOException, InterruptedException, ExecutionException {
		// 0) Setup
		final String temp_dir = System.getProperty("java.io.tmpdir") + File.separator;
		
		final GlobalPropertiesBean globals = BeanTemplateUtils.build(GlobalPropertiesBean.class)
				.with(GlobalPropertiesBean::local_yarn_config_dir, temp_dir)
				.with(GlobalPropertiesBean::distributed_root_dir, temp_dir)
				.with(GlobalPropertiesBean::local_root_dir, temp_dir)
				.with(GlobalPropertiesBean::distributed_root_dir, temp_dir)
				.done().get();
		
		final MockHdfsStorageService storage_service = new MockHdfsStorageService(globals);
		
		// Some buckets
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/storage/bucket")
				.with(DataBucketBean::data_schema,
						BeanTemplateUtils.build(DataSchemaBean.class)
					.done().get())
				.done().get();		
		
		setup_bucket(storage_service, bucket, Collections.emptyList());
		
		final FileContext dfs = storage_service.getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
		final String bucket_root = storage_service.getBucketRootPath() + "/" + bucket.full_name();		
		dfs.mkdir(new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_RAW_SECONDARY + "test1"), FsPermission.getDirDefault(), true);
		//(skip the current dir once just to check it doesn't cause problems)
		dfs.mkdir(new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_JSON_SECONDARY + "test2"), FsPermission.getDirDefault(), true);
		dfs.create(new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_JSON_SECONDARY + "test2/test2.json"), EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)).close();
		dfs.mkdir(new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_JSON), FsPermission.getDirDefault(), true);
		dfs.mkdir(new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_PROCESSED_SECONDARY + "test3"), FsPermission.getDirDefault(), true);
		dfs.mkdir(new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_PROCESSED), FsPermission.getDirDefault(), true);
		
		// (delete the primary, copy test2 across)
		BasicMessageBean res1 = storage_service.getDataService().get().switchCrudServiceToPrimaryBuffer(bucket, "test2", Optional.empty()).get();
		System.out.println("(res1 = " + res1.message() + ")");
		assertTrue("Request returns: " + res1.message(), res1.success());
		
		assertTrue(doesDirExist(dfs, new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_JSON)));
		assertTrue(doesFileExist(dfs, new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_JSON + "test2.json")));
		assertTrue(doesDirExist(dfs, new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_RAW)));
		assertTrue(doesDirExist(dfs, new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_PROCESSED)));
		assertFalse(doesDirExist(dfs, new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_JSON_SECONDARY + "test2")));
		assertFalse(doesDirExist(dfs, new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_RAW_SECONDARY + "test2")));
		assertFalse(doesDirExist(dfs, new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_PROCESSED_SECONDARY + "test2")));
		
		BasicMessageBean res2 = storage_service.getDataService().get().switchCrudServiceToPrimaryBuffer(bucket, "test3", Optional.of("ex_primary")).get();
		System.out.println("(res2 = " + res2.message() + ")");
		assertTrue("Request returns: " + res2.message(), res2.success());
		
		assertTrue(doesDirExist(dfs, new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_JSON_SECONDARY + "ex_primary")));
		assertTrue(doesDirExist(dfs, new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_RAW_SECONDARY + "ex_primary")));
		assertTrue(doesDirExist(dfs, new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_PROCESSED_SECONDARY + "ex_primary")));
		assertTrue(doesFileExist(dfs, new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX_JSON_SECONDARY + "ex_primary/test2.json")));
	}
	
	/////////////////////////////////////////////////////////////////
	
	// UTILS
	
	protected boolean doesDirExist(FileContext fc, Path p) {
		try {
			return fc.getFileStatus(p).isDirectory();
		}
		catch (Exception e) { return false; }
	}
	protected boolean doesFileExist(FileContext fc, Path p) {
		try {
			return fc.getFileStatus(p).isFile();
		}
		catch (Exception e) { return false; }
	}
	
	protected void setup_bucket(MockHdfsStorageService storage_service, final DataBucketBean bucket, List<String> extra_suffixes) {
		final FileContext dfs = storage_service.getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
				
		final String bucket_root = storage_service.getBucketRootPath() + "/" + bucket.full_name();		

		//(first delete root path)
		try {
			dfs.delete(new Path(bucket_root), true);
		}
		catch (Exception e) {}
		
		Stream.concat(
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
			,
			extra_suffixes.stream())
				.map(s -> new Path(s))
				.forEach(Lambdas.wrap_consumer_u(p -> dfs.mkdir(p, FsPermission.getDefault(), true)));
		
	}
	
}
