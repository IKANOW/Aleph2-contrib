package com.ikanow.aleph2.logging.service;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.ManagementSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.LoggingSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.logging.data_model.LoggingServiceConfigBean;
import com.ikanow.aleph2.logging.module.LoggingServiceModule;
import com.ikanow.aleph2.logging.utils.LoggingUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestLoggingService {
	private static final Logger _logger = LogManager.getLogger();
	private static ISearchIndexService search_index_service;
	private static IStorageService storage_service;
	private static LoggingService logging_service;
	protected ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	protected Injector _app_injector;
	
	// All the services
	@Inject IServiceContext _service_context;
	@Inject LoggingServiceConfigBean _config;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}
	
	public void getServices() {		
		_logger.info("run injectModules");		
		final File config_file = new File("./resources/context_local_test.properties");
		final Config config = ConfigFactory.parseFile(config_file);
		
		try {
			_app_injector = ModuleUtils.createTestInjector(Arrays.asList(new LoggingServiceModule()), Optional.of(config));
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
		search_index_service = _service_context.getSearchIndexService().get();
		storage_service = _service_context.getStorageService();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		getServices();		
		logging_service = new LoggingService(_config, search_index_service, storage_service);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testLogBucket() throws InterruptedException, ExecutionException {
		final String subsystem_name = "logging_test1";
		final long num_messages_to_log = 50;
		final DataBucketBean test_bucket = getTestBucket("test1", Level.ALL); 
		//log a few messages
		for ( int i = 0; i < num_messages_to_log; i++ ) {
			logging_service.log(Level.ERROR, test_bucket,ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
			logging_service.systemLog(Level.ERROR, Optional.of(test_bucket), ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
			logging_service.systemLog(Level.ERROR, Optional.empty(), ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
		}
		
		//check its in ES, wait 10s max for the index to refresh
		final DataBucketBean logging_test_bucket = LoggingUtils.convertBucketToLoggingBucket(test_bucket);
		final IDataWriteService<BasicMessageBean> logging_crud = search_index_service.getDataService().get().getWritableDataService(BasicMessageBean.class, logging_test_bucket, Optional.empty(), Optional.empty()).get();
		waitForResults(logging_crud, 10);
		assertEquals(num_messages_to_log*2, logging_crud.countObjects().get().longValue());
		
		final DataBucketBean logging_external_test_bucket = LoggingUtils.convertBucketToLoggingBucket(BeanTemplateUtils.clone(test_bucket).with(DataBucketBean::full_name, "/external/"+ subsystem_name+"/").done());
		final IDataWriteService<BasicMessageBean> logging_crud_external = search_index_service.getDataService().get().getWritableDataService(BasicMessageBean.class, logging_external_test_bucket, Optional.empty(), Optional.empty()).get();
		waitForResults(logging_crud_external, 10);
		assertEquals(num_messages_to_log, logging_crud_external.countObjects().get().longValue());

		//cleanup
		logging_crud.deleteDatastore().get();
	}
	
	@Test
	public void testLogFilter() throws InterruptedException, ExecutionException {
		final String subsystem_name = "logging_test2";
		final long num_messages_to_log_each_type = 5;
		final List<Level> levels = Arrays.asList(Level.DEBUG, Level.INFO, Level.ERROR);
		final DataBucketBean test_bucket = getTestBucket("test2", Level.ERROR); 
		//log a few messages
		for ( int i = 0; i < num_messages_to_log_each_type; i++ ) {
			for ( Level level : levels) {
				logging_service.log(level, test_bucket,ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
				logging_service.systemLog(level, Optional.of(test_bucket),ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
				logging_service.systemLog(level, Optional.empty(), ErrorUtils.buildMessage(true, subsystem_name, "test_message " + i, "no error")).get();
			}
		}
		
		//check its in ES, wait 10s max for the index to refresh
		final DataBucketBean logging_test_bucket = LoggingUtils.convertBucketToLoggingBucket(test_bucket);
		final IDataWriteService<BasicMessageBean> logging_crud = search_index_service.getDataService().get().getWritableDataService(BasicMessageBean.class, logging_test_bucket, Optional.empty(), Optional.empty()).get();
		waitForResults(logging_crud, 10);
		assertEquals(10, logging_crud.countObjects().get().longValue()); //should only have logged ERROR messages

		final DataBucketBean logging_external_test_bucket = LoggingUtils.convertBucketToLoggingBucket(BeanTemplateUtils.clone(test_bucket).with(DataBucketBean::full_name, "/external/"+ subsystem_name+"/").done());
		final IDataWriteService<BasicMessageBean> logging_crud_external = search_index_service.getDataService().get().getWritableDataService(BasicMessageBean.class, logging_external_test_bucket, Optional.empty(), Optional.empty()).get();
		waitForResults(logging_crud_external, 10);
		assertEquals(15, logging_crud_external.countObjects().get().longValue());
		
		//cleanup
		logging_crud.deleteDatastore().get();
	}
	
//	@Test
//	public void testLogExternal() throws InterruptedException, ExecutionException {
//		final String external_service_name = "external_1";
//		final long num_messages_to_log = 50;
//		final DataBucketBean test_bucket = getExternalBucket(external_service_name); 
//		//log a few messages
//		for ( int i = 0; i < num_messages_to_log; i++ ) {
//			logging_service.log(external_service_name, "test_message " + i).get();
//		}
//		
//		//check its in ES, wait 10s max for the index to refresh
//		final DataBucketBean logging_test_bucket = LoggingService.convertBucketToLoggingBucket(test_bucket);
//		final IDataWriteService<BasicMessageBean> logging_crud = search_index_service.getDataService().get().getWritableDataService(BasicMessageBean.class, logging_test_bucket, Optional.empty(), Optional.empty()).get();
//		waitForResults(logging_crud, 10);
//		assertEquals(num_messages_to_log, logging_crud.countObjects().get().longValue());
//
//		//cleanup
//		logging_crud.deleteDatastore().get();
//	}
//	
//	@Test
//	public void testLogSystem() throws InterruptedException, ExecutionException {
//		final long num_messages_to_log = 50;
//		final DataBucketBean test_bucket = getSystemBucket("TODO"); 
//		//log a few messages
//		for ( int i = 0; i < num_messages_to_log; i++ ) {
//			logging_service.log("test_message " + i).get();
//		}
//		
//		//check its in ES, wait 10s max for the index to refresh
//		final DataBucketBean logging_test_bucket = LoggingService.convertBucketToLoggingBucket(test_bucket);
//		final IDataWriteService<BasicMessageBean> logging_crud = search_index_service.getDataService().get().getWritableDataService(BasicMessageBean.class, logging_test_bucket, Optional.empty(), Optional.empty()).get();
//		waitForResults(logging_crud, 10);
//		assertEquals(num_messages_to_log, logging_crud.countObjects().get().longValue());
//
//		//cleanup
//		logging_crud.deleteDatastore().get();
//	}
	
	/**
	 * Waits for the crud service count objects to return some amount of objects w/in the given
	 * timeframe, returns as soon as we find any results.  Useful for waiting for ES to flush/update the index. 
	 * 
	 * @param crud_service
	 * @param max_wait_time_s
	 */
	private static void waitForResults(final IDataWriteService<?> crud_service, final long max_wait_time_s) {
		for (int ii = 0; ii < max_wait_time_s; ++ii) {
			try { Thread.sleep(1000L); } catch (Exception e) {}
			if (crud_service.countObjects().join().intValue() > 0) break;
		}
	}

	/**
	 * Creates a sample bucket with search index enabled and the given full_name located at /test/logtest/<name>/
	 * 
	 * @param name
	 * @param min_log_level 
	 * @return
	 */
	private DataBucketBean getTestBucket(final String name, Level min_log_level) {
		return BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/logtest/" + name + "/")
				.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class)
						.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(SearchIndexSchemaBean.class)
								.with(SearchIndexSchemaBean::enabled, true)
								.done().get())
						.done().get())	
				.with(DataBucketBean::management_schema, BeanTemplateUtils.build(ManagementSchemaBean.class)
						.with(ManagementSchemaBean::logging_schema, BeanTemplateUtils.build(LoggingSchemaBean.class)
								.with(LoggingSchemaBean::log_level, min_log_level)
								.done().get())
						.done().get())
				.done().get();
	}
	
//	private DataBucketBean getSystemBucket(final String technology_name) {
//		return BeanTemplateUtils.build(DataBucketBean.class)
//				.with(DataBucketBean::full_name, "/system/" + technology_name + "/")
//				.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class)
//						.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(SearchIndexSchemaBean.class)
//								.with(SearchIndexSchemaBean::enabled, true)
//								.done().get())
//						.done().get())
//				.done().get();
//	}
//	
//	private DataBucketBean getExternalBucket(final String technology_name) {
//		return BeanTemplateUtils.build(DataBucketBean.class)
//				.with(DataBucketBean::full_name, "/external/" + technology_name + "/")
//				.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class)
//						.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(SearchIndexSchemaBean.class)
//								.with(SearchIndexSchemaBean::enabled, true)
//								.done().get())
//						.done().get())
//				.done().get();
//	}

}
