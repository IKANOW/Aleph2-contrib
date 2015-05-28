package com.ikanow.aleph2.management_db.mongodb.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.management_db.mongodb.module.MockMongoDbManagementDbModule;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class TestIkanowV1SyncService {

	@Inject 
	protected IServiceContext _service_context = null;
	
	@Inject 
	protected IkanowV1SyncService sync_service; 
	
	@Before
	public void setupDependencies() throws Exception {
		if (null != _service_context) {
			return;
		}
		
		final String temp_dir = System.getProperty("java.io.tmpdir");
		
		// OK we're going to use guice, it was too painful doing this by hand...				
		Config config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("test_v1_sync_service.properties")))
							.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
							.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
		
		Injector app_injector = ModuleUtils.createInjector(Arrays.asList(new MockMongoDbManagementDbModule()), Optional.of(config));	
		app_injector.injectMembers(this);		
	}
	
	@Test
	public void testSetup() {
		final String temp_dir = System.getProperty("java.io.tmpdir");
		
		assertTrue("setup completed - service context", _service_context != null);
		assertTrue("setup completed - services", _service_context.getCoreManagementDbService() != null);
		assertTrue("setup completed - services", sync_service != null);
		assertEquals(temp_dir, _service_context.getGlobalProperties().local_root_dir());
		
		if (File.separator.equals("\\")) { // windows mode!
			assertTrue("WINDOWS MODE: hadoop home needs to be set (use -Dhadoop.home.dir={HADOOP_HOME} in JAVA_OPTS)", null != System.getProperty("hadoop.home.dir"));
			assertTrue("WINDOWS MODE: hadoop home needs to exist: " + System.getProperty("hadoop.home.dir"), null != System.getProperty("hadoop.home.dir"));
		}
	}
	
	@Test
	public void testSynchronization() throws InterruptedException, ExecutionException {
		
		for (int i = 0; i < 4; ++i) {
			try { Thread.sleep(1000); } catch (Exception e) {}
		}
		assertEquals(1, IkanowV1SyncService._num_leader_changes);
		
		@SuppressWarnings("unchecked")
		final ICrudService<JsonNode> v1_config_db = _service_context.getCoreManagementDbService().getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.source"));				
		
		assertTrue("Query optimized", v1_config_db.deregisterOptimizedQuery(Arrays.asList("extractType")));
	}
	
}
