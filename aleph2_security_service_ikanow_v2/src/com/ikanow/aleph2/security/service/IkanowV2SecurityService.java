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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.shiro.mgt.SecurityManager;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISubject;
import com.ikanow.aleph2.security.module.IkanowV2SecurityModule;
import com.ikanow.aleph2.security.shiro.IkanowV2Realm;

public class IkanowV2SecurityService extends SecurityService implements ISecurityService, IExtraDependencyLoader{
	
	
	protected ISubject currentSubject = null;

	@Inject
	protected IServiceContext serviceContext;

	
	@Inject
	public IkanowV2SecurityService(IServiceContext serviceContext, SecurityManager securityManager) {
		super(serviceContext,securityManager);

	}


	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class, Optional<String> driver_options) {
		return Optional.empty();
	}

	

	static IkanowV2SecurityModule _temp;
	
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)(_temp = new IkanowV2SecurityModule()));
	}

	public void killMe() throws Exception {
		if (null != _temp) {
			_temp.destroy();
			
		}
	}
	

	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected String getRealmName(){
		return IkanowV2Realm.class.getName();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.security.service.SecurityService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Stream.concat(
				super.getUnderlyingArtefacts().stream(),
				Stream.of(this))
				.collect(Collectors.toList());
	}


	
	
}
