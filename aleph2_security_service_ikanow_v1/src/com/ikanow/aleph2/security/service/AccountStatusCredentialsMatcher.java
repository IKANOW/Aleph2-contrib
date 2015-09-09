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



import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.credential.SimpleCredentialsMatcher;

/**
 * This class checks if the account status is Active. No other Authentication is checked. Used instead of NoCredentialMatcher but limits authentication to active accounts only.
 * @author jfreydank
 *
 */
public class AccountStatusCredentialsMatcher extends SimpleCredentialsMatcher {
	private static final Logger logger = LogManager.getLogger(AccountStatusCredentialsMatcher.class);

	
    @Override
    public boolean doCredentialsMatch(AuthenticationToken token, AuthenticationInfo info) {
    	 try {
    	    
        	AuthenticationBean ab = ((IkanowV1AuthenticationInfo)info).getAuthenticationBean();        	 
        	return checkStatus(ab);
		} catch (Exception e) {
			logger.error("Caught error encoding:"+e.getMessage(),e);
		
    	}
        return false;
    }
	
    protected static boolean checkStatus(AuthenticationBean ab) {
		// TODO taken from V1 logic, why does null count as active?
		return ( (ab.getAccountStatus() == null) || ( AuthenticationBean.ACCOUNT_STATUS_ACTIVE.equals(ab.getAccountStatus()) ) );
	}

}