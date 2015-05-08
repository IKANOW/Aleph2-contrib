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

import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.objects.shared.Identity;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.infinit.e.data_model.store.social.person.PersonPojo;

/**
 * Identifier for an IKANOW user, typically the currently logged in user.
 * 
 * @author Burch
 *
 */
public class IKANOWUserIdentity implements Identity {

	public PersonPojo ikanow_user;
	
	public IKANOWUserIdentity() {
		this(null);
	}
	
	public IKANOWUserIdentity(PersonPojo person) {
		ikanow_user = person;
	}
	
	public boolean hasPermission(Object resource, String operation) {
		//check the security service if there is a rule for this identity/resource/operation
		ISecurityService security_service = ContextUtils.getAccessContext().getSecurityService();
		return security_service.hasPermission(this, resource.getClass(), null, operation);		
	}
	
	public boolean hasPermission(Class<?> resourceClass, String identifier, String operation) {
		ISecurityService security_service = ContextUtils.getAccessContext().getSecurityService();
		return security_service.hasPermission(this, resourceClass, identifier, operation);
	}
	
	public boolean hasPermission(String resourceName, String identifier, String operation) {
		ISecurityService security_service = ContextUtils.getAccessContext().getSecurityService();
		return security_service.hasPermission(this, resourceName, identifier, operation);
	}
}
