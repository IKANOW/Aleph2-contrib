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
package com.ikanow.aleph2.analytics.storm.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Injector;
import com.ikanow.aleph2.analytics.storm.assets.SampleStormStreamTopology1;
import com.ikanow.aleph2.analytics.storm.data_model.IStormController;
import com.ikanow.aleph2.analytics.storm.services.MockAnalyticsContext;
import com.ikanow.aleph2.analytics.storm.services.StreamingEnrichmentContextService;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingTopology;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestStormControllerUtil {
	static final Logger _logger = LogManager.getLogger(); 
	private static IStormController storm_cluster;
	protected Injector _app_injector;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		storm_cluster = StormControllerUtil.getLocalStormController();
	}
	
	@Before
	public void injectModules() throws Exception {
		final Config config = ConfigFactory.parseFile(new File("./example_config_files/context_local_test.properties"));
		
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
	}
	
	@Test
	public void test_createTopologyName() {
		assertEquals(BucketUtils.getUniqueSignature("/path/to/bucket", Optional.empty()),
				StormControllerUtil.bucketPathToTopologyName(BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/path/to/bucket").done().get(), Optional.empty()));
		assertEquals(BucketUtils.getUniqueSignature("/path/to/bucket", Optional.of("test")), 
				StormControllerUtil.bucketPathToTopologyName(BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/path/to/bucket").done().get(), Optional.of("test")));
	}
	
	/**
	 * Tests that caching doesn't cause jobs to fail because they restart too fast
	 * https://github.com/IKANOW/Aleph2/issues/26
	 * @throws Exception 
	 */
	@Test
	public void testQuickCache() throws Exception {
		//Submit a job, causes jar to cache
		final DataBucketBean bucket = createBucket();
		final SharedLibraryBean library_tech = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::path_name, "/test/lib")
				.done().get();
		final SharedLibraryBean library_mod = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::path_name, "/test/module")
				.done().get();
		
		final MockAnalyticsContext test_analytics_context = _app_injector.getInstance(MockAnalyticsContext.class);
		final StreamingEnrichmentContextService context = new StreamingEnrichmentContextService(test_analytics_context);
		test_analytics_context.setBucket(bucket);
		test_analytics_context.setTechnologyConfig(library_tech);
		test_analytics_context.setModuleConfig(library_mod);
		
		final IEnrichmentStreamingTopology enrichment_topology = new SampleStormStreamTopology1();
		final StormTopology storm_top = (StormTopology) enrichment_topology.getTopologyAndConfiguration(bucket, context)._1();
		final String cached_jar_dir = System.getProperty("java.io.tmpdir");
		final ISearchIndexService index_service = context.getServiceContext().getService(ISearchIndexService.class, Optional.empty()).get();
		final ICrudService<JsonNode> crud_service = 
				index_service.getDataService()
					.flatMap(s -> s.getWritableDataService(JsonNode.class, bucket, Optional.empty(), Optional.empty()))
					.flatMap(IDataWriteService::getCrudService)
					.get();
		crud_service.deleteDatastore().get();
		StormControllerUtil.startJob(storm_cluster, bucket, Optional.empty(), Collections.emptyList(), Collections.emptyList(), storm_top, Collections.emptyMap(), cached_jar_dir);
		
		//debug only, let's the job finish
		//Thread.sleep(5000);
		final TopologyInfo info = StormControllerUtil.getJobStats(storm_cluster, StormControllerUtil.bucketPathToTopologyName(bucket, Optional.empty()));
		_logger.debug("Status is: " + info.get_status());
		assertTrue(info.get_status().equals("ACTIVE"));
		
		//Restart same job (should use cached jar)
		StormControllerUtil.restartJob(storm_cluster, bucket, Optional.empty(), Collections.emptyList(), Collections.emptyList(), storm_top, Collections.emptyMap(), cached_jar_dir);
		
		final TopologyInfo info1 = StormControllerUtil.getJobStats(storm_cluster, StormControllerUtil.bucketPathToTopologyName(bucket, Optional.empty()));
		_logger.debug("Status is: " + info.get_status());
		assertTrue(info1.get_status().equals("ACTIVE"));	
		
		// Stop job and wait for result
		StormControllerUtil.stopJob(storm_cluster, bucket, Optional.empty()).get();
	}

	protected DataBucketBean createBucket() {		
		return BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "test_quickcache")
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::full_name, "/test/quickcache")
				.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
						.with("search_index_schema", BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
								.done().get())
						.done().get())
				.done().get();
	}
	
	
	
}
