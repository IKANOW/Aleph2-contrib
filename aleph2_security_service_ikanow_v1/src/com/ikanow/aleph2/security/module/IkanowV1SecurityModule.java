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

import org.apache.shiro.authc.credential.CredentialsMatcher;

import com.google.inject.multibindings.Multibinder;
import com.ikanow.aleph2.security.interfaces.IRoleProvider;
import com.ikanow.aleph2.security.service.AccountStatusCredentialsMatcher;
import com.ikanow.aleph2.security.service.IModificationChecker;
import com.ikanow.aleph2.security.service.IkanowV1AdminRoleProvider;
import com.ikanow.aleph2.security.service.IkanowV1DataGroupRoleProvider;
import com.ikanow.aleph2.security.service.IkanowV1DataModificationChecker;
import com.ikanow.aleph2.security.service.IkanowV1Realm;
import com.ikanow.aleph2.security.service.IkanowV1UserGroupRoleProvider;

public class IkanowV1SecurityModule extends CoreSecurityModule{
	
	
	public IkanowV1SecurityModule(){
	}
	
	@Override
	protected void bindMisc() {
		bind(IModificationChecker.class).to(IkanowV1DataModificationChecker.class).asEagerSingleton();
		expose(IModificationChecker.class);
	}

	@Override
	protected void bindRealms() {
		bindRealm().to(IkanowV1Realm.class).asEagerSingleton();		
	}
	

	@Override
    protected void bindRoleProviders(){
		Multibinder<IRoleProvider> uriBinder = Multibinder.newSetBinder(binder(), IRoleProvider.class);
	    uriBinder.addBinding().to(IkanowV1AdminRoleProvider.class);
	    uriBinder.addBinding().to(IkanowV1UserGroupRoleProvider.class);
	    uriBinder.addBinding().to(IkanowV1DataGroupRoleProvider.class);
    }
	
	@Override
	protected void bindCredentialsMatcher() {
 		bind(CredentialsMatcher.class).to(AccountStatusCredentialsMatcher.class);
	}
}
