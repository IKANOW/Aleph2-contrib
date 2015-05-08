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
package com.ikanow.aleph2.security_service;

import java.util.Base64;
import java.util.Map;
import java.util.logging.Logger;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.objects.shared.Identity;
import com.ikanow.infinit.e.data_model.api.ResponsePojo.ResponseObject;
import com.ikanow.infinit.e.data_model.driver.InfiniteDriver;
import com.ikanow.infinit.e.data_model.store.social.person.PersonPojo;

public class IKANOWSecurityService implements ISecurityService {

	private final static Logger logger = Logger.getLogger(IKANOWSecurityService.class.getName());
	private InfiniteDriver driver = new InfiniteDriver("https://dev.ikanow.com/api/"); //TODO obviously don't hardcode api
	
	public IKANOWSecurityService()
	{
		//TODO read in the rules from mongodb or something
	}

	public boolean hasPermission(Identity identity, Class<?> resourceClass,
			String resourceIdentifier, String operation) {
		//TODO check against our cache/store for this specific rule
		logger.info("SecurityService checking permission: " + identity.getClass().toString() 
				+ " " + resourceClass.toString() + " " + resourceIdentifier + " " + operation);
		
		//TODO run any checks through ikanow security
		//IKANOW actually implements it's own internal security so we always return true for every resource
		
		
		return true;
	}
	
	public boolean hasPermission(Identity identity, String resourceName,
			String resourceIdentifier, String operation) {
		//ikanow security does not support arbitrary string rule names
		return false;
	}

	/**
	 * Checks if the user is logged in given the header, currently
	 * I'm using basic auth and checking for the username "user" and password "password".
	 * This is equal to setting the Authorization header of a request to: "Basic dXNlcjpwYXNzd29yZA==" 
	 * @throws Exception 
	 * 
	 */
	public Identity getIdentity(Map<String, Object> token) throws Exception {
		if ( token.containsKey("Authorization") )
		{
			//some example code on how to handle a basic auth scheme
			return getBasicAuthIdentity((String)token.get("Authorization"));
		}
		else if ( token.containsKey("infinite_api_key") )
		{
			//TODO handle login and api key also			
			//try to handle an ikanow cookie scheme
			return getIkanowCookieIdentity((String)token.get("infinite_api_key"));
		}
		throw new Exception("Unknown Auth token, IKANOW accepts a token named 'infinite_api_key'" 
				+ " with the assigned key received during a login");
	}

	
	/**
	 * Attempts to retrieve the currently logged in user given an ikanow cookie
	 * 
	 * @param ikanow_cookie
	 * @return
	 * @throws Exception
	 */
	private Identity getIkanowCookieIdentity(@NonNull String ikanow_cookie) throws Exception {
		driver.useExistingCookie(ikanow_cookie);
		ResponseObject ro = new ResponseObject();
		PersonPojo person = driver.getPerson(null, ro);
		if ( person == null )
			throw new Exception(ro.getMessage());
		return new IKANOWUserIdentity(person);
	}

	/**
	 * Test function to return a fake user identify based on a basic auth token.
	 * 
	 * @param authorization_header
	 * @return
	 * @throws Exception
	 */
	private Identity getBasicAuthIdentity(@NonNull String authorization_header) throws Exception {		
		if ( authorization_header == null )
			throw new Exception("Need token named 'Authorization' to get identity.");
		authorization_header = authorization_header.substring(6);
		logger.info(authorization_header);
		
		String decode = new String( Base64.getDecoder().decode(authorization_header.getBytes("UTF-8")), "UTF-8");		
		logger.info(decode);
		
		String[] splits = decode.split(":");
		String username = splits[0];
		String password = splits[1];
		
		if ( username.equals("user") && password.equals("password"))
			return new IKANOWUserIdentity();
		
		throw new Exception("User/pass did not match");
	}

	public void grantPermission(Identity identity, Class<?> resourceClass,
			String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub
		
	}

	public void grantPermission(Identity identity, String resourceName,
			String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub
		
	}

	public void revokePermission(Identity identity, Class<?> resourceClass,
			String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub
		
	}
	
	public void revokePermission(Identity identity, String resourceName,
			String resourceIdentifier, String operation) {
		// TODO Auto-generated method stub
		
	}

	public void clearPermission(Class<?> resourceClass,
			String resourceIdentifier) {
		// TODO Auto-generated method stub
		
	}

	public void clearPermission(String resourceName, String resourceIdentifier) {
		// TODO Auto-generated method stub
		
	}
}
