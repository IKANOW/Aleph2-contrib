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
package com.ikanow.aleph2.security.module;

import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.session.mgt.SessionManager;

import com.google.inject.binder.AnnotatedBindingBuilder;

/** This class uses the Shiro.ini for configuration of the Filter chains but then uses the the WebEnvironment and 
 * a separate entry for EnvironmentLoaderListener in the web.xml to configure the web module. 
 * This is the non-conform way of configuring Shiro-Web with guice, does not require ServletContext (servlet-api.jar) for Module. 
 * Use the class IkanowV1WebSecurityModule which extends ShiroWebModule for a complete Guice based configuration (including FilterChains. 
 * Web.ini content:
 * 
  <listener>
        <listener-class>org.apache.shiro.web.env.EnvironmentLoaderListener</listener-class>
    </listener>
    ...
	<context-param>
	     <param-name>shiroEnvironmentClass</param-name>
	     <param-value>com.ikanow.aleph2.web_login.WebEnvironment</param-value>
	</context-param>
	<context-param>
	     <param-name>aleph2.module_class</param-name>
        <param-value>com.ikanow.aleph2.security.module.IkanowV1WebSecurityModuleIniBased</param-value>
	</context-param>

 * @author jfreydank
 *
 */
public class IkanowV1WebSecurityModuleIniBased extends IkanowV1SecurityModule {

	@Override
	/**
	 * Partial functionality from ShiroModule. We do not want the whole guice functionality because the WebSecurityManager is created using the IniFactory.
	 */
	public void configure() {

        configureShiro();
        bind(realmCollectionKey())
                .to(realmSetKey());
        expose(realmCollectionKey());

    }

	@Override
    protected void bindSecurityManager(AnnotatedBindingBuilder<? super SecurityManager> bind) {
		// empty on purpose because we are using the WebSecurityManager created by the IniWebEnvironment
    }

    /**
     * Binds the session manager.  Override this method in order to provide your own session manager binding.
     * <p/>
     * By default, a {@link org.apache.shiro.session.mgt.DefaultSessionManager} is bound as an eager singleton.
     *
     * @param bind
     */
	@Override
    protected void bindSessionManager(AnnotatedBindingBuilder<SessionManager> bind) {
		// empty on purpose because we are using the WebSecurityManager created by the IniWebEnvironment
    }


}
