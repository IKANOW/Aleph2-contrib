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
package com.ikanow.alelph2.security_service;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ikanow.aleph2.security_service.IKANOWSecurityService;
import com.ikanow.aleph2.security_service.IKANOWUserIdentity;
import com.ikanow.infinit.e.data_model.api.ResponsePojo.ResponseObject;
import com.ikanow.infinit.e.data_model.driver.InfiniteDriver;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestIKANOWSecurityService {

	private InfiniteDriver driver = new InfiniteDriver("https://dev.ikanow.com/api/"); //TODO obviously don't hardcode api
	private static String ikanow_encoded_password;
	private static String ikanow_username;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		//Run these tests w/ environmental variables to set these
		//this will keep them hidden out of our checked in code
		Config config = ConfigFactory.load();
		ikanow_encoded_password = config.getString("ikanow.test_encoded_password");
		ikanow_username = config.getString("ikanow.test_username");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	private Map<String, Object> getAuthToken(String cookie_text) {
		String cookie = cookie_text.substring(15, 39);
		Map<String, Object> token = new HashMap<String, Object>();
		token.put("infinite_api_key", cookie);
		return token;
	}

	@Test
	public void testIKANOWLogin() throws Exception {
		//login
		ResponseObject ro = new ResponseObject();
		driver.login_encrypted(ikanow_username, ikanow_encoded_password, ro);
		
		
		//send cookie through security service to make sure it authenticates us
		Map<String, Object> token = getAuthToken(driver.getCookie());
		
		IKANOWSecurityService ikanow_security = new IKANOWSecurityService();
		IKANOWUserIdentity identity = (IKANOWUserIdentity) ikanow_security.getIdentity(token);
		assertNotNull(identity);
		
		//test it's the user we logged in as
		assertEquals("cburch@ikanow.com", identity.ikanow_user.getEmail());
	}

}
