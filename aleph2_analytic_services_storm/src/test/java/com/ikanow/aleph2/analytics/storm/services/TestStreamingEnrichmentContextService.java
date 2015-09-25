/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.analytics.storm.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;
import backtype.storm.spout.ISpout;
import backtype.storm.task.IBolt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.analytics.storm.assets.OutputBolt;
import com.ikanow.aleph2.analytics.storm.assets.TransientStreamingOutputBolt;
import com.ikanow.aleph2.analytics.storm.services.StreamingEnrichmentContextService;
import com.ikanow.aleph2.analytics.storm.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingTopology;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestStreamingEnrichmentContextService {
	static final Logger _logger = LogManager.getLogger(); 

	protected ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	protected Injector _app_injector;
	
	// All the services
	@Inject IServiceContext _service_context;
	IManagementDbService _core_management_db;
	ICoreDistributedServices _distributed_services;
	ISearchIndexService _index_service;
	GlobalPropertiesBean _globals;	
	
	MockAnalyticsContext _mock_analytics_context;
	
	
	@Before
	public void injectModules() throws Exception {
		_logger.info("run injectModules");
		
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
		
		
		_app_injector.injectMembers(this);
		_mock_analytics_context = new MockAnalyticsContext(_service_context);
		_core_management_db = _mock_analytics_context._core_management_db;
		_distributed_services = _mock_analytics_context._distributed_services;
		_index_service = _mock_analytics_context._index_service;
		_globals = _mock_analytics_context._globals;
	}
	
	private Tuple2<MockAnalyticsContext, StreamingEnrichmentContextService> getContextPair() {
		final MockAnalyticsContext analytics_context = new MockAnalyticsContext(_service_context);
		final StreamingEnrichmentContextService stream_context = new StreamingEnrichmentContextService(analytics_context);
		return Tuples._2T(analytics_context, stream_context);
	}
	
	@Test
	public void test_basicContextCreation() throws Exception {
		_logger.info("run test_basicContextCreation");
		try {
			assertTrue("Injector created", _app_injector != null);
		
			Tuple2<MockAnalyticsContext, StreamingEnrichmentContextService> context_pair = getContextPair();
			final StreamingEnrichmentContextService test_context = context_pair._2();
						
			assertTrue("Find service", test_context.getServiceContext().getService(ISearchIndexService.class, Optional.empty()).isPresent());
			
			// Check that multiple calls to create harvester result in different contexts but with the same injection:
			Tuple2<MockAnalyticsContext, StreamingEnrichmentContextService> context_pair2 = getContextPair();
			final StreamingEnrichmentContextService test_context2 = context_pair2._2();
			assertTrue("StreamingEnrichmentContextService created", test_context2 != null);
			assertTrue("StreamingEnrichmentContextServices different", test_context2 != test_context);
			assertEquals(test_context.getServiceContext(), test_context.getServiceContext());
			
			// Check calls that require that bucket/endpoint be set
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::_id, "test")
					.with(DataBucketBean::full_name, "/test/basicContextCreation")
					.with(DataBucketBean::modified, new Date())
					.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
							.with("search_index_schema", BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
									.done().get())
							.done().get())
					.done().get();
			
			final AnalyticThreadJobBean.AnalyticThreadJobOutputBean analytic_output1 =  
					BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobOutputBean.class)
						.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::transient_type, MasterEnrichmentType.streaming)
						.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::is_transient, false)
					.done().get();		

			final AnalyticThreadJobBean.AnalyticThreadJobOutputBean analytic_output2 =  
					BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobOutputBean.class)
						.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::transient_type, MasterEnrichmentType.streaming)
						.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::is_transient, true)
					.done().get();		

			final AnalyticThreadJobBean.AnalyticThreadJobOutputBean analytic_output3 =  
					BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobOutputBean.class)
						.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::transient_type, MasterEnrichmentType.batch)
						.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::is_transient, true)
					.done().get();		
			
			final AnalyticThreadJobBean analytic_job1 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::output, analytic_output1)
					.with(AnalyticThreadJobBean::name, "analytic_output1")
					.done().get();
			final AnalyticThreadJobBean analytic_job2 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::output, analytic_output2)
					.with(AnalyticThreadJobBean::name, "analytic_output2")
					.done().get();
			final AnalyticThreadJobBean analytic_job3 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::output, analytic_output3)
					.with(AnalyticThreadJobBean::name, "analytic_output3")
					.done().get();
						
			final SharedLibraryBean mod_library = BeanTemplateUtils.build(SharedLibraryBean.class)
					.with(SharedLibraryBean::path_name, "/test/module")
					.done().get();
			final SharedLibraryBean tech_library = BeanTemplateUtils.build(SharedLibraryBean.class)
					.with(SharedLibraryBean::path_name, "/test/tech")
					.done().get();
			
			context_pair._1().setModuleConfig(mod_library);
			context_pair._1().setTechnologyConfig(tech_library);
			context_pair._1().setBucket(test_bucket);			
			context_pair._2().setUserTopology(new IEnrichmentStreamingTopology() {

				@Override
				public Tuple2<Object, Map<String, String>> getTopologyAndConfiguration(
						DataBucketBean bucket, IEnrichmentModuleContext context) {
					return null;
				}

				@Override
				public <O> JsonNode rebuildObject(
						O raw_outgoing_object,
						Function<O, LinkedHashMap<String, Object>> generic_outgoing_object_builder) {
					return null;
				}				
			});
			
			// Test the 3 different cases for user topologies
			
			// 1) Transient job => gets user topology
			
			{
				context_pair._2().setJob(analytic_job1);			
				final Object endpoint_1 = test_context.getTopologyStorageEndpoint(Object.class, Optional.empty());
				assertTrue("This endpoint should be OutputBolt: " + endpoint_1.getClass(), endpoint_1 instanceof OutputBolt);
				
				// (check it's serializable - ie that this doesn't exception)
				ObjectOutputStream out_bolt1 = new ObjectOutputStream(new ByteArrayOutputStream());
				out_bolt1.writeObject(endpoint_1);
				out_bolt1.close(); 
			}
			
			// 2) Non-transient, output topic exists (ie output is streaming)
			{
				context_pair._2().setJob(analytic_job2);			
				final Object endpoint_2 = test_context.getTopologyStorageEndpoint(Object.class, Optional.empty());
				assertTrue("This endpoint should be KafkaBolt: " + endpoint_2.getClass(), endpoint_2 instanceof TransientStreamingOutputBolt);
				
				// (check it's serializable - ie that this doesn't exception)
				ObjectOutputStream out_bolt2 = new ObjectOutputStream(new ByteArrayOutputStream());
				out_bolt2.writeObject(endpoint_2);
				out_bolt2.close(); 
			}
			// 3) Non-transient, batch output
			
			context_pair._2().setJob(analytic_job3);			
			try {
				test_context.getTopologyStorageEndpoint(Object.class, Optional.empty());
				fail("Should have errored");
			}
			catch(Exception e) {
			}
		}
		catch (Exception e) {
			System.out.println(ErrorUtils.getLongForm("{1}: {0}", e, e.getClass()));
			throw e;
		}
	}
	
	@Test
	public void test_ExternalContextCreation() throws InstantiationException, IllegalAccessException, ClassNotFoundException, InterruptedException, ExecutionException {
		_logger.info("run test_ExternalContextCreation");
		try {
			assertTrue("Config contains application name: " + ModuleUtils.getStaticConfig().root().toString(), ModuleUtils.getStaticConfig().root().toString().contains("application_name"));
			assertTrue("Config contains v1_enabled: " + ModuleUtils.getStaticConfig().root().toString(), ModuleUtils.getStaticConfig().root().toString().contains("v1_enabled"));
			
			Tuple2<MockAnalyticsContext, StreamingEnrichmentContextService> context_pair = getContextPair();
			final StreamingEnrichmentContextService test_context = context_pair._2();

			final AnalyticThreadJobBean analytic_job1 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
					.with(AnalyticThreadJobBean::name, "analytic_job1")
					.done().get();

			final AnalyticThreadBean analytic_thread = BeanTemplateUtils.build(AnalyticThreadBean.class)
															.with(AnalyticThreadBean::jobs, Arrays.asList(analytic_job1))
														.done().get();
			
			final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
													.with(DataBucketBean::_id, "test")
													.with(DataBucketBean::full_name, "/test/external-context/creation")
													.with(DataBucketBean::modified, new Date(1436194933000L))
													.with(DataBucketBean::analytic_thread, analytic_thread)
													.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
															.with("search_index_schema", BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
																	.done().get())
															.done().get())
													.done().get();						
			
			final SharedLibraryBean mod_library = BeanTemplateUtils.build(SharedLibraryBean.class)
					.with(SharedLibraryBean::path_name, "/test/module")
					.done().get();
			final SharedLibraryBean tech_library = BeanTemplateUtils.build(SharedLibraryBean.class)
					.with(SharedLibraryBean::path_name, "/test/tech")
					.done().get();

			context_pair._1().setModuleConfig(mod_library);
			context_pair._1().setTechnologyConfig(tech_library);
			context_pair._1().setBucket(test_bucket);
			context_pair._2().setJob(analytic_job1);
			
			// Empty service set:
			final String signature = test_context.getEnrichmentContextSignature(Optional.of(test_bucket), Optional.empty());
						
			final String expected_sig = "com.ikanow.aleph2.analytics.storm.services.StreamingEnrichmentContextService:analytic_job1:com.ikanow.aleph2.analytics.storm.services.MockAnalyticsContext:{\"3fdb4bfa-2024-11e5-b5f7-727283247c7e\":\"{\\\"_id\\\":\\\"test\\\",\\\"modified\\\":1436194933000,\\\"full_name\\\":\\\"/test/external-context/creation\\\",\\\"analytic_thread\\\":{\\\"jobs\\\":[{\\\"name\\\":\\\"analytic_job1\\\"}]},\\\"data_schema\\\":{\\\"search_index_schema\\\":{}}}\",\"3fdb4bfa-2024-11e5-b5f7-727283247c7f\":\"{\\\"path_name\\\":\\\"/test/tech\\\"}\",\"3fdb4bfa-2024-11e5-b5f7-727283247cff\":\"{\\\"path_name\\\":\\\"/test/module\\\"}\",\"CoreDistributedServices\":{},\"MongoDbManagementDbService\":{\"mongodb_connection\":\"localhost:9999\"},\"globals\":{\"local_cached_jar_dir\":\"file://temp/\"},\"service\":{\"CoreDistributedServices\":{\"interface\":\"com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices\",\"service\":\"com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices\"},\"CoreManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"},\"ManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"},\"SearchIndexService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService\",\"service\":\"com.ikanow.aleph2.search_service.elasticsearch.services.MockElasticsearchIndexService\"},\"SecurityService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService\",\"service\":\"com.ikanow.aleph2.data_model.interfaces.shared_services.MockSecurityService\"},\"StorageService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService\",\"service\":\"com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService\"}}}";			
			assertEquals(expected_sig, signature);

			// Check can't call multiple times
			
			// Additionals service set:

			try {
				test_context.getEnrichmentContextSignature(Optional.of(test_bucket),
												Optional.of(
														ImmutableSet.<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>builder()
															.add(Tuples._2T(IStorageService.class, Optional.empty()))
															.add(Tuples._2T(IManagementDbService.class, Optional.of("test")))
															.build()																
														)
												);
				fail("Should have errored");
			}
			catch (Exception e) {}
			// Create another injector:
			Tuple2<MockAnalyticsContext, StreamingEnrichmentContextService> context_pair2 = getContextPair();
			final StreamingEnrichmentContextService test_context2 = context_pair2._2();
			context_pair2._1().setModuleConfig(mod_library);
			context_pair2._1().setTechnologyConfig(tech_library);
			context_pair2._1().setBucket(test_bucket);
			context_pair2._2().setJob(analytic_job1);

			final String signature2 = test_context2.getEnrichmentContextSignature(Optional.of(test_bucket),
					Optional.of(
							ImmutableSet.<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>builder()
								.add(Tuples._2T(IStorageService.class, Optional.empty()))
								.add(Tuples._2T(IManagementDbService.class, Optional.of("test")))
								.build()																
							)
					);
			
			
			final String expected_sig2 = "com.ikanow.aleph2.analytics.storm.services.StreamingEnrichmentContextService:analytic_job1:com.ikanow.aleph2.analytics.storm.services.MockAnalyticsContext:{\"3fdb4bfa-2024-11e5-b5f7-727283247c7e\":\"{\\\"_id\\\":\\\"test\\\",\\\"modified\\\":1436194933000,\\\"full_name\\\":\\\"/test/external-context/creation\\\",\\\"analytic_thread\\\":{\\\"jobs\\\":[{\\\"name\\\":\\\"analytic_job1\\\"}]},\\\"data_schema\\\":{\\\"search_index_schema\\\":{}}}\",\"3fdb4bfa-2024-11e5-b5f7-727283247c7f\":\"{\\\"path_name\\\":\\\"/test/tech\\\"}\",\"3fdb4bfa-2024-11e5-b5f7-727283247cff\":\"{\\\"path_name\\\":\\\"/test/module\\\"}\",\"CoreDistributedServices\":{},\"MongoDbManagementDbService\":{\"mongodb_connection\":\"localhost:9999\"},\"globals\":{\"local_cached_jar_dir\":\"file://temp/\"},\"service\":{\"CoreDistributedServices\":{\"interface\":\"com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices\",\"service\":\"com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices\"},\"CoreManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"},\"ManagementDbService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"},\"SearchIndexService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService\",\"service\":\"com.ikanow.aleph2.search_service.elasticsearch.services.MockElasticsearchIndexService\"},\"SecurityService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService\",\"service\":\"com.ikanow.aleph2.data_model.interfaces.shared_services.MockSecurityService\"},\"StorageService\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService\",\"service\":\"com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService\"},\"test\":{\"interface\":\"com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService\",\"service\":\"com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService\"}}}"; 
			assertEquals(expected_sig2, signature2);
			
			final IEnrichmentModuleContext test_external1a = ContextUtils.getEnrichmentContext(signature);		
			
			assertTrue("external context non null", test_external1a != null);
			
			assertTrue("external context of correct type", test_external1a instanceof StreamingEnrichmentContextService);
			
			assertTrue("StreamingEnrichmentContextService dependencies", test_external1a.getServiceContext().getCoreManagementDbService() != null);
			
			// Ceck I can see my extended services: 
			
			final IEnrichmentModuleContext test_external2a = ContextUtils.getEnrichmentContext(signature2);		
			
			assertTrue("external context non null", test_external2a != null);
			
			assertTrue("external context of correct type", test_external2a instanceof StreamingEnrichmentContextService);
			
			final StreamingEnrichmentContextService test_external2b = (StreamingEnrichmentContextService)test_external2a;
			
			assertEquals("/test/module", test_external2b.getModuleConfig().path_name());			
			
			assertTrue("I can see my additonal services", null != test_external2b.getServiceContext().getService(IStorageService.class, Optional.empty()));
			assertTrue("I can see my additonal services", null != test_external2b.getServiceContext().getService(IManagementDbService.class, Optional.of("test")));
						
			//Check some "won't work in module" calls:
			test_external2b.setJob(analytic_job1);
			try {
				test_external2b.getEnrichmentContextSignature(null, null);
				fail("Should have errored");
			}
			catch (Exception e) {
				assertEquals(ErrorUtils.TECHNOLOGY_NOT_MODULE, e.getMessage());
			}
			try {
				test_external2b.getUnderlyingArtefacts();
				fail("Should have errored");
			}
			catch (Exception e) {
				assertEquals(ErrorUtils.TECHNOLOGY_NOT_MODULE, e.getMessage());
			}
			try {
				test_external2b.getTopologyEntryPoints(ISpout.class, Optional.empty());
				fail("Should have errored");
			}
			catch (Exception e) {
				assertEquals(ErrorUtils.TECHNOLOGY_NOT_MODULE, e.getMessage());
			}
			try {
				test_external2b.getTopologyStorageEndpoint(IBolt.class, Optional.empty());
				fail("Should have errored");
			}
			catch (Exception e) {
				assertEquals(ErrorUtils.TECHNOLOGY_NOT_MODULE, e.getMessage());
			}
			try {
				test_external2b.getTopologyErrorEndpoint(null, null);
				fail("Should have errored");
			}
			catch (Exception e) {
				assertEquals(ErrorUtils.TECHNOLOGY_NOT_MODULE, e.getMessage());
			}
			
		}
		catch (Exception e) {
			try {//(if it's a guice error then this will error out, but you can still use the long form error below for diagnosis)
				e.printStackTrace();
			}
			catch (Throwable t) {}
			
			System.out.println(ErrorUtils.getLongForm("{1}: {0}", e, e.getClass()));
			fail("Threw exception");
		}
	}

	@Test
	public void test_getUnderlyingArtefacts() {
		_logger.info("run test_getUnderlyingArtefacts");
		
		Tuple2<MockAnalyticsContext, StreamingEnrichmentContextService> context_pair = getContextPair();
		final StreamingEnrichmentContextService test_context = context_pair._2();
		
		// (interlude: check errors if called before getSignature
		try {
			test_context.getUnderlyingArtefacts();
			fail("Should have errored");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.SERVICE_RESTRICTIONS, e.getMessage());		
		}
		
		final AnalyticThreadJobBean analytic_job1 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
				.with(AnalyticThreadJobBean::name, "analytic_job1")
				.done().get();
		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
												.with(DataBucketBean::_id, "test")
												.with(DataBucketBean::full_name, "/test/get_underlying/artefacts")
												.with(DataBucketBean::modified, new Date())
												.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
														.with("search_index_schema", BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
																.done().get())
														.done().get())
												.done().get();
		
		final SharedLibraryBean mod_library = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::path_name, "/test/module")
				.done().get();
		final SharedLibraryBean tech_library = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::path_name, "/test/tech")
				.done().get();
		
		context_pair._1().setModuleConfig(mod_library);
		context_pair._1().setTechnologyConfig(tech_library);
		context_pair._2().setJob(analytic_job1);
		
		// Empty service set:
		test_context.getEnrichmentContextSignature(Optional.of(test_bucket), Optional.empty());		
		final Collection<Object> res1 = test_context.getUnderlyingArtefacts();
		final String exp1 = "class com.ikanow.aleph2.analytics.storm.services.StreamingEnrichmentContextService:class com.ikanow.aleph2.analytics.storm.services.MockAnalyticsContext:class com.ikanow.aleph2.data_model.utils.ModuleUtils$ServiceContext:class com.ikanow.aleph2.distributed_services.services.MockCoreDistributedServices:class com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService:class com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory:class com.ikanow.aleph2.search_service.elasticsearch.services.MockElasticsearchIndexService:class com.ikanow.aleph2.shared.crud.elasticsearch.services.MockElasticsearchCrudServiceFactory:class com.ikanow.aleph2.storage_service_hdfs.services.MockHdfsStorageService:class com.ikanow.aleph2.management_db.mongodb.services.MockMongoDbManagementDbService:class com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory";
		assertEquals(exp1, res1.stream().map(o -> o.getClass().toString()).collect(Collectors.joining(":")));
		
		// Check can retrieve the analytics context:
		
		Optional<IAnalyticsContext> analytics_context = test_context.getUnderlyingPlatformDriver(IAnalyticsContext.class, Optional.empty());
		assertTrue("Found analytics context", analytics_context.isPresent());
		assertTrue("Analytics context is of correct type: " + analytics_context.get(), analytics_context.get() instanceof MockAnalyticsContext);
	}
	
	@Test
	public void test_misc() {
		_logger.info("run test_misc");
		
		assertTrue("Injector created", _app_injector != null);
		
		Tuple2<MockAnalyticsContext, StreamingEnrichmentContextService> context_pair = getContextPair();
		final StreamingEnrichmentContextService test_context = context_pair._2();

		assertEquals(Optional.empty(), test_context.getUnderlyingPlatformDriver(String.class, Optional.empty()));
		assertEquals(Optional.empty(), test_context.getBucket());
		
		try {
			test_context.emergencyQuarantineBucket(null, null);
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "emergencyQuarantineBucket"), e.getMessage());
		}
		try {
			test_context.emergencyDisableBucket(null);
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "emergencyDisableBucket"), e.getMessage());
		}
		try {
			test_context.logStatusForBucketOwner(null, null);
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "logStatusForBucketOwner"), e.getMessage());
		}
		try {
			test_context.logStatusForBucketOwner(null, null, false);
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "logStatusForBucketOwner"), e.getMessage());
		}
		// (This has now been implemented, though not ready to test yet)
//		try {
//			test_context.getBucketStatus(null);
//			fail("Should have thrown exception");
//		}
//		catch (Exception e) {
//			assertEquals(ErrorUtils.NOT_YET_IMPLEMENTED, e.getMessage());
//		}
		try {
			test_context.getNextUnusedId();
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.get(ErrorUtils.INVALID_CALL_FROM_WRAPPED_ANALYTICS_CONTEXT_ENRICH, "getNextUnusedId"), e.getMessage());
		}
		try {
			test_context.storeErroredObject(0L, null);
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.get(ErrorUtils.INVALID_CALL_FROM_WRAPPED_ANALYTICS_CONTEXT_ENRICH, "storeErroredObject"), e.getMessage());
		}
		try {
			test_context.getTopologyErrorEndpoint(null, null);
			fail("Should have errored");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "getTopologyErrorEndpoint"), e.getMessage());
		}
	}
	
	@Test
	public void test_objectEmitting() throws InterruptedException, ExecutionException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		_logger.info("run test_objectEmitting");

		Tuple2<MockAnalyticsContext, StreamingEnrichmentContextService> context_pair = getContextPair();
		final StreamingEnrichmentContextService test_context = context_pair._2();
		
		final AnalyticThreadJobBean analytic_job1 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
				.with(AnalyticThreadJobBean::name, "analytic_job1")
				.done().get();
		
		final AnalyticThreadBean analytic_thread = BeanTemplateUtils.build(AnalyticThreadBean.class)
				.with(AnalyticThreadBean::jobs, Arrays.asList(analytic_job1))
			.done().get();

		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "test_objectemitting")
				.with(DataBucketBean::analytic_thread, analytic_thread)
				.with(DataBucketBean::full_name, "/test/object/emitting")
				.with(DataBucketBean::modified, new Date())
				.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
						.with("search_index_schema", BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
								.done().get())
						.done().get())
				.done().get();

		final SharedLibraryBean mod_library = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::path_name, "/test/module")
				.done().get();
		final SharedLibraryBean tech_library = BeanTemplateUtils.build(SharedLibraryBean.class)
				.with(SharedLibraryBean::path_name, "/test/tech")
				.done().get();
		
		context_pair._1().setModuleConfig(mod_library);
		context_pair._1().setTechnologyConfig(tech_library);
		context_pair._1().setBucket(test_bucket);
		context_pair._2().setJob(analytic_job1);
		
		// Empty service set:
		final String signature = test_context.getEnrichmentContextSignature(Optional.of(test_bucket), Optional.empty());

		final ICrudService<DataBucketBean> raw_mock_db = _core_management_db.getDataBucketStore(); // (note this is actually the underlying db because we're in a test here)
		raw_mock_db.deleteDatastore().get();
		assertEquals(0L, (long)raw_mock_db.countObjects().get());
		raw_mock_db.storeObject(test_bucket).get();		
		assertEquals(1L, (long)raw_mock_db.countObjects().get());
		
		final IEnrichmentModuleContext test_external1a = ContextUtils.getEnrichmentContext(signature);		

		//(internal)
//		ISearchIndexService check_index = test_context.getService(ISearchIndexService.class, Optional.empty()).get();
		//(external)
		final ISearchIndexService check_index = test_external1a.getServiceContext().getService(ISearchIndexService.class, Optional.empty()).get();		
		final ICrudService<JsonNode> crud_check_index = 
				check_index.getDataService()
					.flatMap(s -> s.getWritableDataService(JsonNode.class, test_bucket, Optional.empty(), Optional.empty()))
					.flatMap(IDataWriteService::getCrudService)
					.get();
		crud_check_index.deleteDatastore();
		Thread.sleep(2000L); // (wait for datastore deletion to flush)
		assertEquals(0, crud_check_index.countObjects().get().intValue());
		
		
		final JsonNode jn1 = _mapper.createObjectNode().put("test", "test1");
		final JsonNode jn2 = _mapper.createObjectNode().put("test", "test2");
		
		//(try some errors)
		try {
			test_external1a.emitMutableObject(0, test_external1a.convertToMutable(jn1), Optional.of(BeanTemplateUtils.build(AnnotationBean.class).done().get()));
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.NOT_YET_IMPLEMENTED, e.getMessage());
		}
		try {
			test_external1a.emitImmutableObject(0, jn2, Optional.empty(), Optional.of(BeanTemplateUtils.build(AnnotationBean.class).done().get()));
			fail("Should have thrown exception");
		}
		catch (Exception e) {
			assertEquals(ErrorUtils.NOT_YET_IMPLEMENTED, e.getMessage());
		}
		
		test_external1a.emitMutableObject(0, test_external1a.convertToMutable(jn1), Optional.empty());
		test_external1a.emitImmutableObject(0, jn2, Optional.empty(), Optional.empty());		
		test_external1a.emitImmutableObject(0, jn2, 
				Optional.of(_mapper.createObjectNode().put("extra", "test3_extra").put("test", "test3")), 
				Optional.empty());
		
		for (int i = 0; i < 60; ++i) {
			Thread.sleep(1000L);
			if (crud_check_index.countObjects().get().intValue() >= 2) {
				_logger.info("(Found objects after " + i + " seconds)");
				break;
			}
		}
		
		//DEBUG
		//System.out.println(crud_check_index.getUnderlyingPlatformDriver(ElasticsearchContext.class, Optional.empty()).get().indexContext().getReadableIndexList(Optional.empty()));
		//System.out.println(crud_check_index.getUnderlyingPlatformDriver(ElasticsearchContext.class, Optional.empty()).get().typeContext().getReadableTypeList());
		
		assertEquals(3, crud_check_index.countObjects().get().intValue());
		assertEquals("{\"test\":\"test3\",\"extra\":\"test3_extra\"}", ((ObjectNode)
				crud_check_index.getObjectBySpec(CrudUtils.anyOf().when("test", "test3")).get().get()).remove(Arrays.asList("_id")).toString());
		
	}

	public static class TestBean {}	
	
	@Test
	public void test_objectStateRetrieval() throws InterruptedException, ExecutionException {
		_logger.info("run test_objectStateRetrieval");
		
		Tuple2<MockAnalyticsContext, StreamingEnrichmentContextService> context_pair = getContextPair();
		final StreamingEnrichmentContextService test_context = context_pair._2();
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class).with("full_name", "TEST_HARVEST_CONTEXT").done().get();

		final SharedLibraryBean lib_bean = BeanTemplateUtils.build(SharedLibraryBean.class).with("path_name", "TEST_HARVEST_CONTEXT").done().get();
		context_pair._1().setModuleConfig(lib_bean);
		//(note deliberately don't set tech config library here to double check it accesses the right one...)
		context_pair._2().setBucket(bucket);
		
		ICrudService<AssetStateDirectoryBean> dir_a = _core_management_db.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.analytic_thread));
		ICrudService<AssetStateDirectoryBean> dir_e = _core_management_db.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.enrichment));
		ICrudService<AssetStateDirectoryBean> dir_h = _core_management_db.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.harvest));
		ICrudService<AssetStateDirectoryBean> dir_s = _core_management_db.getStateDirectory(Optional.empty(), Optional.of(AssetStateDirectoryBean.StateDirectoryType.library));

		dir_a.deleteDatastore().get();
		dir_e.deleteDatastore().get();
		dir_h.deleteDatastore().get();
		dir_s.deleteDatastore().get();
		assertEquals(0, dir_a.countObjects().get().intValue());
		assertEquals(0, dir_e.countObjects().get().intValue());
		assertEquals(0, dir_h.countObjects().get().intValue());
		assertEquals(0, dir_s.countObjects().get().intValue());
		
		@SuppressWarnings("unused")
		ICrudService<TestBean> s1 = test_context.getGlobalEnrichmentModuleObjectStore(TestBean.class, Optional.of("test"));
		assertEquals(0, dir_a.countObjects().get().intValue());
		assertEquals(0, dir_e.countObjects().get().intValue());
		assertEquals(0, dir_h.countObjects().get().intValue());
		assertEquals(1, dir_s.countObjects().get().intValue());

		@SuppressWarnings("unused")
		ICrudService<TestBean> e1 = test_context.getBucketObjectStore(TestBean.class, Optional.of(bucket), Optional.of("test"), Optional.of(AssetStateDirectoryBean.StateDirectoryType.enrichment));
		assertEquals(0, dir_a.countObjects().get().intValue());
		assertEquals(1, dir_e.countObjects().get().intValue());
		assertEquals(0, dir_h.countObjects().get().intValue());
		assertEquals(1, dir_s.countObjects().get().intValue());
		
		context_pair._1().setBucket(bucket);
		
		@SuppressWarnings("unused")
		ICrudService<TestBean> a1 = test_context.getBucketObjectStore(TestBean.class, Optional.empty(), Optional.of("test"), Optional.of(AssetStateDirectoryBean.StateDirectoryType.analytic_thread));
		assertEquals(1, dir_a.countObjects().get().intValue());
		assertEquals(1, dir_e.countObjects().get().intValue());
		assertEquals(0, dir_h.countObjects().get().intValue());
		assertEquals(1, dir_s.countObjects().get().intValue());
		
		@SuppressWarnings("unused")
		ICrudService<TestBean> h1 = test_context.getBucketObjectStore(TestBean.class, Optional.empty(), Optional.of("test"), Optional.of(AssetStateDirectoryBean.StateDirectoryType.harvest));
		assertEquals(1, dir_a.countObjects().get().intValue());
		assertEquals(1, dir_e.countObjects().get().intValue());
		assertEquals(1, dir_h.countObjects().get().intValue());
		assertEquals(1, dir_s.countObjects().get().intValue());		

		ICrudService<AssetStateDirectoryBean> dir_e_2 = test_context.getBucketObjectStore(AssetStateDirectoryBean.class, Optional.empty(), Optional.empty(), Optional.empty());
		assertEquals(1, dir_e_2.countObjects().get().intValue());
		
		@SuppressWarnings("unused")
		ICrudService<TestBean> h2 = test_context.getBucketObjectStore(TestBean.class, Optional.empty(), Optional.of("test_2"), Optional.empty());
		assertEquals(1, dir_a.countObjects().get().intValue());
		assertEquals(2, dir_e.countObjects().get().intValue());
		assertEquals(1, dir_h.countObjects().get().intValue());
		assertEquals(1, dir_s.countObjects().get().intValue());		
		assertEquals(2, dir_e_2.countObjects().get().intValue());
		
		@SuppressWarnings("unused")
		ICrudService<TestBean> s2 = test_context.getBucketObjectStore(TestBean.class, Optional.empty(), Optional.of("test_2"), Optional.of(AssetStateDirectoryBean.StateDirectoryType.library));
		assertEquals(1, dir_a.countObjects().get().intValue());
		assertEquals(2, dir_e.countObjects().get().intValue());
		assertEquals(1, dir_h.countObjects().get().intValue());
		assertEquals(2, dir_s.countObjects().get().intValue());
		
	}
}
