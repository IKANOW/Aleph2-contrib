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

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AuthenticationException;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.util.ProfilingUtility;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class IkanowV1SecurityServiceTest extends MockDbBasedTest {
	private static final Logger logger = LogManager.getLogger(IkanowV1SecurityServiceTest.class);

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
			config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("/test_security_service_v1.properties")))
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
	public void testAuthenticated() {
        //token.setRememberMe(true);
		ISubject subject = loginAsTestUser();
        try {
		} catch (AuthenticationException e) {
			logger.info("Caught (expected) Authentication exception:"+e.getMessage());
			
		}
		assertEquals(true, subject.isAuthenticated());		
	}

	protected ISubject loginAsTestUser() throws AuthenticationException{
		String password = "xxxxxx";
		ISubject subject = securityService.login(testUserId,password);			
		return subject;
	}

	protected ISubject loginAsAdmin() throws AuthenticationException{
		String password = "xxxxxxxx";
		ISubject subject = securityService.login(adminId,password);			
		return subject;
	}

	protected ISubject loginAsRegularUser() throws AuthenticationException{
		String password = "xxxxxxxx";
		ISubject subject = securityService.login(regularUserId,password);			
		return subject;
	}

	@Test
	public void testRole(){
		ISubject subject = loginAsAdmin();
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.hasRole(subject,"admin"));
	}

	@Test
	public void testPermission(){
		ISubject subject = loginAsRegularUser();
		// test personal community permission
		String permission = "54f86d8de4b03d27d1ea0d7b";
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.isPermitted(subject,permission));
	}

	@Test
	public void testRunAs(){
		ISubject subject = loginAsTestUser();
		// system community
		String runAsPrincipal = "54f86d8de4b03d27d1ea0d7b"; // casey
		String runAsRole = "54f86d8de4b03d27d1ea0d7b";
		String runAsPersonalPermission = "v1_54fa4ab9e4b0b269e3a0c837";
		
		securityService.runAs(subject,Arrays.asList(runAsPrincipal));
		
		assertEquals(true,securityService.hasRole(subject,runAsRole));
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.isPermitted(subject,runAsPersonalPermission));
		Collection<String> p = securityService.releaseRunAs(subject);
		logger.debug("Released Principals:"+p);
		securityService.runAs(subject,Arrays.asList(runAsPrincipal));
		
		assertEquals(true,securityService.hasRole(subject,runAsRole));
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.isPermitted(subject,runAsPersonalPermission));
		p = securityService.releaseRunAs(subject);
		logger.debug("Released Principals:"+p);
	}

	
	@Test
	public void testSessionTimeout(){
		((IkanowV1SecurityService)securityService).setSessionTimeout(1000);
		ISubject subject = loginAsAdmin();
		// system community
		String permission = "4c927585d591d31d7b37097a";
		String role = "admin";
		assertEquals(true,securityService.hasRole(subject,"admin"));
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		subject = loginAsAdmin();
		//assertEquals(true,subject.isAuthenticated());
		
		assertEquals(true,securityService.hasRole(subject,"admin"));
	}

	@Test
	public void testCaching(){
		ISubject subject = loginAsRegularUser();
		// test personal community permission
		String permission = "54f86d8de4b03d27d1ea0d7b";
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("TU-permisssion0");
		assertEquals(true,securityService.isPermitted(subject,permission));
		ProfilingUtility.timeStopAndLog("TU-permisssion0");
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("TU-permisssion"+(i+1));
			assertEquals(true,securityService.isPermitted(subject,permission));			
			ProfilingUtility.timeStopAndLog("TU-permisssion"+(i+1));
		}
		subject = loginAsAdmin();
		// test personal community permission
		permission = "54f86d8de4b03d27d1ea0d7b";
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("AU-permisssion0");
		assertEquals(true,securityService.isPermitted(subject,permission));
		ProfilingUtility.timeStopAndLog("AU-permisssion");
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("AU-permisssion"+i+1);
			assertEquals(true,securityService.isPermitted(subject,permission));			
			ProfilingUtility.timeStopAndLog("AU-permisssion"+i+1);
		}
		
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("TU2-permisssion_L"+(i+1));
		subject = loginAsRegularUser();
		ProfilingUtility.timeStopAndLog("TU2-permisssion_L"+(i+1));
		// test personal community permission
		permission = "54f86d8de4b03d27d1ea0d7b";
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("TU2-permisssion"+(i+1));
		assertEquals(true,securityService.isPermitted(subject,permission));
			ProfilingUtility.timeStopAndLog("TU2-permisssion"+(i+1));
		}
	}

	@Test
	public void testInvalidateAuthenticationCache(){
		ISubject subject = loginAsTestUser();
		
		securityService.runAs(subject,Arrays.asList(regularUserId));

		// test personal community permission
		String permission = "54f86d8de4b03d27d1ea0d7b";
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("TU-permisssion0");
		assertEquals(true,securityService.isPermitted(subject,permission));
		ProfilingUtility.timeStopAndLog("TU-permisssion0");
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("TU-permisssion"+(i+1));
			assertEquals(true,securityService.isPermitted(subject,permission));			
			ProfilingUtility.timeStopAndLog("TU-permisssion"+(i+1));
			if(i==5){
				((IkanowV1SecurityService)securityService).invalidateAuthenticationCache(Arrays.asList(regularUserId));	
				//loginAsRegularUser();
			}
		}
	}

	@Test
	public void testInvalidateCache(){
		ISubject subject = loginAsTestUser();
		
		securityService.runAs(subject,Arrays.asList(regularUserId));

		// test personal community permission
		String permission = "54f86d8de4b03d27d1ea0d7b";
        //test a typed permission (not instance-level)
		ProfilingUtility.timeStart("TU-permisssion0");
		assertEquals(true,securityService.isPermitted(subject,permission));
		ProfilingUtility.timeStopAndLog("TU-permisssion0");
		for (int i = 0; i < 10; i++) {
			ProfilingUtility.timeStart("TU-permisssion"+(i+1));
			assertEquals(true,securityService.isPermitted(subject,permission));			
			ProfilingUtility.timeStopAndLog("TU-permisssion"+(i+1));
			if(i==5){
				((IkanowV1SecurityService)securityService).invalidateCache();	
				//loginAsRegularUser();
			}
		}
	}

	@Test
	public void testRunAsDemoted(){
		ISubject subject = loginAsAdmin();
		// system community
		String runAsPrincipal = regularUserId; 
		
		assertEquals(true,securityService.hasRole(subject,"admin"));

		securityService.runAs(subject,Arrays.asList(runAsPrincipal));
		
		assertEquals(false,securityService.hasRole(subject,"admin"));
		Collection<String> p = securityService.releaseRunAs(subject);
		assertEquals(true,securityService.hasRole(subject,"admin"));
		logger.debug("Released Principals:"+p);
	}

}
