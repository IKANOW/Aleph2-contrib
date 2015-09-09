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



import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.codec.Base64;

public class IkanowV1CredentialsMatcher extends AccountStatusCredentialsMatcher {
	private static final Logger logger = LogManager.getLogger(IkanowV1CredentialsMatcher.class);

	
    @Override
    public boolean doCredentialsMatch(AuthenticationToken token, AuthenticationInfo info) {
        String plainPassword = new String(((UsernamePasswordToken)token).getPassword());
    	String accountCredentials = (String)getCredentials(info);
    	 try {
    	    
        	AuthenticationBean ab = ((IkanowV1AuthenticationInfo)info).getAuthenticationBean();        	 
        	if(checkStatus(ab)){
        		return checkPassword(plainPassword, accountCredentials);
        	}
		} catch (Exception e) {
			logger.error("Caught error encoding:"+e.getMessage(),e);
		
    	}
        return false;
    }
	
	/**
	 *  Encrypt the password
	 * @throws NoSuchAlgorithmException 
	 * @throws UnsupportedEncodingException 
	 */
	public static String encrypt(String password) throws NoSuchAlgorithmException, UnsupportedEncodingException 
	{	
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		md.update(password.getBytes("UTF-8"));		
		return Base64.encodeToString(md.digest());		
	}

	/**
	 *  Check the password
	 * @throws UnsupportedEncodingException 
	 * @throws NoSuchAlgorithmException 
	 */
	protected static boolean checkPassword(String plainPassword, String encryptedPassword) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		return encryptedPassword.equals(encrypt(plainPassword));
		//return encryptor.checkpw(plainPassword, encryptedPassword);
	}	
	

}