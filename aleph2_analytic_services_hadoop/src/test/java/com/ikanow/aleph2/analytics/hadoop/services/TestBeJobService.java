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
package com.ikanow.aleph2.analytics.hadoop.services;

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.analytics.hadoop.services.BatchEnrichmentContext;
import com.ikanow.aleph2.analytics.hadoop.services.BeJobLauncher;
import com.ikanow.aleph2.analytics.services.AnalyticsContext;
import com.ikanow.aleph2.core.shared.utils.DirUtils;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import fj.Unit;
import fj.data.Validation;

public class TestBeJobService {
    private static final Logger logger = LogManager.getLogger(TestBeJobService.class);

	@Inject
	protected GlobalPropertiesBean _globals = null;

	@Inject
	protected IServiceContext _service_context = null;
	
	@Before
	public void setupDependencies() throws Exception {
		try{
			final String temp_dir = System.getProperty("java.io.tmpdir");

			FileUtils.forceMkdir(new File(temp_dir + "/lib"));
			
			// OK we're going to use guice, it was too painful doing this by hand...				
			Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("/context_local_test.properties")))
					.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
					.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
					.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
					.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));

			Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));	
			app_injector.injectMembers(this);

		}
		catch (Throwable t) {
			System.out.println(ErrorUtils.getLongForm("{0}", t));
			throw t; 
		}
	} // setup dependencies

	@Test
	public void testBeJobService_simple() throws Exception{
		final AnalyticThreadJobInputBean test_analytic_input = 
				BeanTemplateUtils.build(AnalyticThreadJobInputBean.class)
					.with(AnalyticThreadJobInputBean::data_service, "batch")
					.with(AnalyticThreadJobInputBean::resource_name_or_id, "")
				.done().get();
		
		// Set passthrough topology
		final AnalyticThreadJobBean test_analytic = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
				.with(AnalyticThreadJobBean::name, "simplejob")
				.with(AnalyticThreadJobBean::module_name_or_id, "_module_test")
				.with(AnalyticThreadJobBean::inputs, Arrays.asList(test_analytic_input))
			.done().get();
		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
												.with(DataBucketBean::full_name, "/test/simple/analytics")
												.with(DataBucketBean::analytic_thread,
														BeanTemplateUtils.build(AnalyticThreadBean.class)
															.with(AnalyticThreadBean::jobs, Arrays.asList(test_analytic))
														.done().get()
														)
												.with(DataBucketBean::data_schema,
														BeanTemplateUtils.build(DataSchemaBean.class)
															.with(DataSchemaBean::storage_schema,
																BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.class)
																	.with(DataSchemaBean.StorageSchemaBean::raw,
																			BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean.class)
																			.done().get()
																			)
																.done().get()
															)
															.with(DataSchemaBean::search_index_schema,
																BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class).done().get()
															)
														.done().get()
														)
											.done().get();
	
		
		testBeJobService_thisBucket(test_bucket, true);
	}
	
	@Test
	public void testBeJobService_multiStage() throws Exception{
		
		final AnalyticThreadJobInputBean test_analytic_input = 
				BeanTemplateUtils.build(AnalyticThreadJobInputBean.class)
					.with(AnalyticThreadJobInputBean::data_service, "batch")
					.with(AnalyticThreadJobInputBean::resource_name_or_id, "")
				.done().get();
		
		// Set passthrough topology
		final AnalyticThreadJobBean test_analytic = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
				.with(AnalyticThreadJobBean::name, "simplejob")
				.with(AnalyticThreadJobBean::module_name_or_id, "_module_test")
				.with(AnalyticThreadJobBean::inputs, Arrays.asList(test_analytic_input))
			.done().get();
		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
												.with(DataBucketBean::full_name, "/test/simple/analytics")
												.with(DataBucketBean::analytic_thread,
														BeanTemplateUtils.build(AnalyticThreadBean.class)
															.with(AnalyticThreadBean::jobs, Arrays.asList(test_analytic))
														.done().get()
														)
												.with(DataBucketBean::batch_enrichment_configs, 
														Arrays.asList(
																BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
																.done().get()
																,
																BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
																.done().get()
																))
												.with(DataBucketBean::data_schema,
														BeanTemplateUtils.build(DataSchemaBean.class)
															.with(DataSchemaBean::search_index_schema,
																BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class).done().get()
															)
														.done().get()
														)
											.done().get();
	
		
		testBeJobService_thisBucket(test_bucket, false);
	}
	
	//////////////////////////////////////////////
	
	// HIGH LEVEL UTILS:
	
	public void testBeJobService_thisBucket(DataBucketBean test_bucket, boolean expect_archive) throws Exception {
		// Only run this test on linux systems, Hadoop local mode not reliably working on Windows
		if (File.separator.equals("\\")) { // windows mode!
			logger.info("WINDOWS MODE SKIPPING THIS TEST");
			return;
		}		
		final AnalyticsContext analytics_context = new AnalyticsContext(_service_context);
		final BatchEnrichmentContext batch_context = new BatchEnrichmentContext(analytics_context);
		
		final SharedLibraryBean technology = BeanTemplateUtils.build(SharedLibraryBean.class).with(SharedLibraryBean::path_name, "/tech/test").done().get();
		final SharedLibraryBean passthrough = BeanTemplateUtils.build(SharedLibraryBean.class)
													.with(SharedLibraryBean::_id, "_module_test")
													.with(SharedLibraryBean::path_name, "/module/test")
												.done().get();
		analytics_context.setTechnologyConfig(technology);
		analytics_context.resetLibraryConfigs(							
				ImmutableMap.<String, SharedLibraryBean>builder()
					.put(passthrough.path_name(), passthrough)
					.put(passthrough._id(), passthrough)
					.build());
		analytics_context.setBucket(test_bucket);
		batch_context.setJob(test_bucket.analytic_thread().jobs().get(0));
		analytics_context.resetJob(test_bucket.analytic_thread().jobs().get(0));
		
		// Set up directory:
		createFolderStructure(test_bucket);
		assertTrue("File1 should exist", new File(_service_context.getStorageService().getBucketRootPath()+test_bucket.full_name() + IStorageService.TO_IMPORT_DATA_SUFFIX + "bucket1data.txt").exists());
		assertTrue("File2 should exist", new File(_service_context.getStorageService().getBucketRootPath()+test_bucket.full_name() + IStorageService.TO_IMPORT_DATA_SUFFIX + "bucket1data.json").exists());
		// (Clear ES index)
		final IDataWriteService<JsonNode> es_index = 
				_service_context.getSearchIndexService().get().getDataService().get().getWritableDataService(JsonNode.class, test_bucket, Optional.empty(), Optional.empty()).get();
		es_index.deleteDatastore().get();
		Thread.sleep(1000L);
		assertEquals(0, es_index.countObjects().get().intValue());
		
		final BeJobLauncher beJobService = new BeJobLauncher(_globals, batch_context);		

		final Validation<String, Job> result = beJobService.runEnhancementJob(test_bucket, Optional.empty());
		
		result.validation(
				fail -> { logger.info("Launch FAIL " + fail); return Unit.unit(); }
				,
				success -> { logger.info("Launched " + success.getJobName()); return Unit.unit(); }
				);

		
		assertTrue("Job worked: " + result.f().toOption().orSome(""), result.isSuccess());
		
		for (int ii = 0; (ii < 60) && !result.success().isComplete(); ++ii) {
			Thread.sleep(1000L);
		}
		logger.info("Job has finished: " + result.success().isSuccessful());
		assertTrue("Job successful", result.success().isSuccessful());
		
		// Check the processed  file has vanished
		
		assertFalse("File1 shouldn't exist", new File(_service_context.getStorageService().getBucketRootPath()+test_bucket.full_name() + IStorageService.TO_IMPORT_DATA_SUFFIX + "bucket1data.txt").exists());
		assertFalse("File2 shouldn't exist", new File(_service_context.getStorageService().getBucketRootPath()+test_bucket.full_name() + IStorageService.TO_IMPORT_DATA_SUFFIX + "bucket1data.json").exists());
		
		// Check the object got written into ES
		for (int ii = 0; ii < 20; ++ii) {
			Thread.sleep(500L);
			if (es_index.countObjects().get().intValue() >= 2) {
				break;
			}
		}			
		assertEquals(3, es_index.countObjects().get().intValue());			
		
		String[] subpaths = new File(_service_context.getStorageService().getBucketRootPath()+test_bucket.full_name() + IStorageService.STORED_DATA_SUFFIX_RAW + IStorageService.NO_TIME_SUFFIX).list();
		//(note all lengths are *2 because of .crc file)
		if (expect_archive) {
			if (null == subpaths) {
				fail("No subpaths of: " + new File(_service_context.getStorageService().getBucketRootPath()+test_bucket.full_name() + IStorageService.STORED_DATA_SUFFIX_RAW + IStorageService.NO_TIME_SUFFIX));
			}
			else if (2 == subpaths.length) {
				//TODO (ALEPH-12): add check for files
			}
			else {
				assertEquals(4, subpaths.length);
				assertTrue("File1 should exist", new File(_service_context.getStorageService().getBucketRootPath()+test_bucket.full_name() + IStorageService.STORED_DATA_SUFFIX_RAW + IStorageService.NO_TIME_SUFFIX + "bucket1data.txt").exists());
				assertTrue("File2 should exist", new File(_service_context.getStorageService().getBucketRootPath()+test_bucket.full_name() + IStorageService.STORED_DATA_SUFFIX_RAW + IStorageService.NO_TIME_SUFFIX + "bucket1data.json").exists());
			}
		}
		else {
			assertEquals("Dir empty or doesn't exist", 0, Optionals.of(() -> subpaths.length).orElse(0).intValue());
		}
	}
	
	//////////////////////////////////////////////
	
	// LOW LEVEL UTILS:
	
	protected void createFolderStructure(final DataBucketBean bucket){
		// create folder structure if it does not exist for testing.		
		final FileContext fileContext = _service_context.getStorageService().getUnderlyingPlatformDriver(FileContext.class,Optional.empty()).get();
		logger.info("Root dir:"+_service_context.getStorageService().getBucketRootPath());
		final String bucketPath1 = _service_context.getStorageService().getBucketRootPath() + bucket.full_name();
		FileUtils.deleteQuietly(new File(bucketPath1)); // (cleanse the dir to start with)
		final String bucketReadyPath1 = bucketPath1+"/managed_bucket/import/ready";
		DirUtils.createDirectory(fileContext,bucketReadyPath1);
		DirUtils.createDirectory(fileContext,_service_context.getStorageService().getBucketRootPath()+"/data/misc/bucket2/managed_bucket/import/ready");
		DirUtils.createDirectory(fileContext,_service_context.getStorageService().getBucketRootPath()+"/data/misc/bucket3/managed_bucket/import/ready");
		StringBuffer sb = new StringBuffer();
		sb.append("bucket1data\r\n");
		DirUtils.createUTF8File(fileContext,bucketReadyPath1+"/bucket1data.txt", sb);
		StringBuffer sb2 = new StringBuffer();
		sb2.append("{\"testField\":\"test1\"}\n{\"testField\":\"test2\"}");
		DirUtils.createUTF8File(fileContext,bucketReadyPath1+"/bucket1data.json", sb2);
	}
	
	
}
