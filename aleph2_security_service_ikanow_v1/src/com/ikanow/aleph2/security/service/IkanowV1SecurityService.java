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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.session.Session;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.security.module.IkanowV1SecurityModule;

public class IkanowV1SecurityService implements ISecurityService, IExtraDependencyLoader{
	
	
	protected ISubject currentSubject = null;
	private static final Logger logger = LogManager.getLogger(IkanowV1SecurityService.class);
	@Inject
	protected IServiceContext serviceContext;

	
	@Inject
	public IkanowV1SecurityService(IServiceContext serviceContext, SecurityManager securityManager) {
		this.serviceContext = serviceContext;
		SecurityUtils.setSecurityManager(securityManager);
	}


	protected void initUnauthorized(){
		try {
			logger.debug("Init was called, it should not be called except in rare cases, use login instead.");

	        // get the currently executing user:
	        Subject currentUser = getShiroSubject();
	        this.currentSubject = new SubjectWrapper(currentUser);
	        // Do some stuff with a Session (no need for a web or EJB container!!!)
	        
		} catch (Throwable e) {
			logger.error("initUnauthorized Caught exception",e);
		}

	}
	
	// Clean way to get the subject
	private Subject getShiroSubject()
	{
	    Subject currentUser = ThreadContext.getSubject();// SecurityUtils.getSubject();

	    if (currentUser == null)
	    {
	        currentUser = SecurityUtils.getSubject();
	    }

	    return currentUser;
	}
	
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Collections.emptyList();
	}

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class, Optional<String> driver_options) {
		return Optional.empty();
	}

	// ***************** new functions ***********************//

	public ISubject getSubject() {
		if(currentSubject == null){
			initUnauthorized();
		}
		return currentSubject;
	}

	@Override
	public ISubject login(String principalName, Object credentials) {
		
		String password = (String)credentials;
        UsernamePasswordToken token = new UsernamePasswordToken(principalName,password);
        //token.setRememberMe(true);

        ensureUserIsLoggedOut();
        Subject shiroSubject = getShiroSubject();
        shiroSubject.login((AuthenticationToken)token);
        currentSubject = new SubjectWrapper(shiroSubject);
		return currentSubject;
	}

	private void ensureUserIsLoggedOut()
	{
	    try
	    {
	    	Subject shiroSubject = getShiroSubject();
	        if (shiroSubject == null)
	            return;

	        // Log the user out and kill their session if possible.
	        shiroSubject.logout();
	        Session session = shiroSubject.getSession(false);
	        if (session == null)
	            return;

	        session.stop();
	    }
	    catch (Exception e)
	    {
	        // Ignore all errors, as we're trying to silently 
	        // log the user out.
	    }
	}
	@Override
	public boolean hasRole(ISubject subject, String roleIdentifier) {
		boolean ret = ((Subject)getSubject().getSubject()).hasRole(roleIdentifier);
		return ret;
	}

	
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new IkanowV1SecurityModule());
	}


	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public boolean isPermitted(ISubject subject, String permission) {
		boolean ret = ((Subject)getSubject().getSubject()).isPermitted(permission);
		return ret;
	}


	@Override
	public void runAs(ISubject subject,Collection<String> principals) {
		// TODO Auto-generated method stub
		
		((Subject)subject.getSubject()).runAs(new SimplePrincipalCollection(principals,IkanowV1Realm.class.getSimpleName()));
	}


	@Override
	public Collection<String> releaseRunAs(ISubject subject) {
		// TODO Auto-generated method stub
		return null;
	}
			
}
