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
package com.ikanow.aleph2.security.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;

import org.apache.shiro.session.Session;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.security.db.SessionDb;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class IkanowV2SecurityServiceTest  extends MockDbBasedTest{

	protected Config config = null;

	@Inject
	protected IServiceContext _temp_service_context = null;	
	protected static IServiceContext _service_context = null;

	protected IManagementDbService _management_db;
	protected ISecurityService securityService = null;

	protected String adminId = "4e3706c48d26852237078005";
	protected String regularUserId = "54f86d8de4b03d27d1ea0d7b";  //cb_user
	protected String testUserId = "4e3706c48d26852237079004"; 	

	
	@Before
	public void setupDependencies() throws Exception {
		try {

		if (null == _service_context) {
			final String temp_dir = System.getProperty("java.io.tmpdir");
	
			// OK we're going to use guice, it was too painful doing this by hand...
			config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("/test_security_service_v2.properties")))
	        //config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("/test_security_service_v2_remote.properties")))
					.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
					.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
					.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
					.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
	
			Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));	
			app_injector.injectMembers(this);
			_service_context = _temp_service_context;
		}
		this._management_db = _service_context.getCoreManagementDbService();
		this.securityService =  _service_context.getSecurityService();		
		initMockDb(_service_context);
		
		} catch(Throwable e) {
			
			e.printStackTrace();
		}
	}

	@Test
	public void testSessionDb(){
		SessionDb sessionDb = new SessionDb(_service_context);		
		Session session1 = mock(Session.class);
		when(session1.getId()).thenReturn("123");
		when(session1.getHost()).thenReturn("localhost");
		Date now = new Date();
		when(session1.getLastAccessTime()).thenReturn(now);
		when(session1.getStartTimestamp()).thenReturn(now);
		when(session1.getTimeout()).thenReturn(1000L*60L);
		when(session1.getAttributeKeys()).thenReturn(Arrays.asList("currentUser"));
		when(session1.getAttribute(any())).thenReturn("doesnotexist@ikanow.com");		
		sessionDb.store(session1);
		Session session2 = (Session)sessionDb.loadById("123");
		assertNotNull(session2);
		assertEquals(session1.getId(), session2.getId());		
		assertEquals(session1.getHost(), session2.getHost());		
		assertEquals(session1.getLastAccessTime(), session2.getLastAccessTime());		
		assertEquals(session1.getStartTimestamp(), session2.getStartTimestamp());		
		assertEquals(session1.getAttribute("currentUser"), session2.getAttribute("currentUser"));		
		sessionDb.delete("123");
		Session session3 = (Session)sessionDb.loadById("123");
		assertNull(session3);
		
	}
	
}
