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

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class IkanowV1RealmTest {
	private static final Logger logger = LogManager.getLogger(IkanowV1RealmTest.class);

	protected Config config = null;

	@Inject
	protected IServiceContext _service_context = null;

	protected IManagementDbService _management_db;
	protected ISecurityService securityService = null;

	@Before
	public void setupDependencies() throws Exception {
		try {
			
		if (_service_context != null) {
			return;
		}

		final String temp_dir = System.getProperty("java.io.tmpdir");

		// OK we're going to use guice, it was too painful doing this by hand...
		config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("/test_security_service_v1.properties")))
				.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
				.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
				.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
				.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));

		Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
		this._management_db = _service_context.getCoreManagementDbService();
		this.securityService =  _service_context.getSecurityService();
		} catch(Throwable e) {
			
			e.printStackTrace();
		}
	}

	
	@Test		
	@Ignore
	public void testAuthenticated() {
        //token.setRememberMe(true);
		ISubject subject = login();
        try {
		} catch (AuthenticationException e) {
			logger.info("Caught (expected) Authentication exception:"+e.getMessage());
			
		}
		assertEquals(System.getProperty("IKANOW_SECURITY_PWD")!=null, subject.isAuthenticated());		
	}

	protected ISubject login() throws AuthenticationException{
		ISubject subject = securityService.getSubject();
		assertNotNull(subject);
		String userName = System.getProperty("IKANOW_SECURITY_LOGIN","noone@ikanow.com");
		String password = System.getProperty("IKANOW_SECURITY_PWD", "not allowed!");
		securityService.login(userName,password);			
		return subject;
	}

	@Test
	@Ignore
	public void testRolePermission(){
		ISubject subject = login();
		// system community
		String permission = "4c927585d591d31d7b37097a";
		String role = System.getProperty("IKANOW_SECURITY_LOGIN","noone@ikanow.com")+"_communities";
//		assertEquals(System.getProperty("IKANOW_SECURITY_LOGIN")!=null,securityService.hasRole(subject,role));
        //test a typed permission (not instance-level)
		assertEquals(System.getProperty("IKANOW_SECURITY_LOGIN")!=null,securityService.isPermitted(subject,permission));
	}

	@Test
	@Ignore
	public void testRunAs(){
		ISubject subject = login();
		// system community
		String permission = "4c927585d591d31d7b37097a";
		String runAsPrincipal = "54f86d8de4b03d27d1ea0d7b";
		String caseysRole = "54f86d8de4b03d27d1ea0d7b_data_group";
		String caseysPersonalPermission = "v1_55a544bee4b056ae0f9bd92b";
		
//		String role = System.getProperty("IKANOW_SECURITY_LOGIN","noone@ikanow.com")+"_communities";
		((Subject)subject.getSubject()).runAs(new SimplePrincipalCollection(runAsPrincipal,this.getClass().getSimpleName()));
		assertEquals(true,securityService.hasRole(subject,caseysRole));
        //test a typed permission (not instance-level)
		assertEquals(true,securityService.isPermitted(subject,caseysPersonalPermission));
		PrincipalCollection p = ((Subject)subject.getSubject()).releaseRunAs();	
		logger.debug("Released Principals:"+p);
		assertEquals(false,securityService.isPermitted(subject,caseysPersonalPermission));
	}

	@Test
	@Ignore
	public void testAuthorizedService(){
		ISubject subject = login();
	}
}
