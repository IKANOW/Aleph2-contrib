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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import scala.Tuple2;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.security.IRoleProvider;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;

public class IkanowV1AdminRoleProvider implements IRoleProvider {
	protected final IServiceContext _context;
	protected IManagementDbService _core_management_db = null;
	protected IManagementDbService _underlying_management_db = null;
	private static final Logger logger = LogManager.getLogger(IkanowV1AdminRoleProvider.class);
	private ICrudService<AuthenticationBean> authenticationDb = null;

	@SuppressWarnings("unchecked")
	protected void initDb(){
		if(_core_management_db == null){
			_core_management_db = _context.getCoreManagementDbService();
		}
		if(_underlying_management_db == null) {
		_underlying_management_db = _context.getService(IManagementDbService.class, Optional.empty()).get();
		}

		String authDboptions = "security.authentication/"+AuthenticationBean.class.getName();
        authenticationDb = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of(authDboptions)).get();

		logger.debug("AuthenticationDB:"+authenticationDb);
	}

	@Inject
	public IkanowV1AdminRoleProvider(final IServiceContext service_context){
		_context = service_context;

	}
	

	protected ICrudService<AuthenticationBean> getAuthenticationStore(){
		if(authenticationDb == null){
			initDb();
		}
	      return authenticationDb;		
	}

	@Override
	public Tuple2<Set<String>, Set<String>> getRolesAndPermissions(String principalName) {
		
        Set<String> roleNames = new HashSet<String>();
        Set<String> permissions = new HashSet<String>();
			
        try {
    		SingleQueryComponent<AuthenticationBean> queryUsername = CrudUtils.anyOf(AuthenticationBean.class).when("profileId",new ObjectId(principalName));		
    		Optional<AuthenticationBean> result = getAuthenticationStore().getObjectBySpec(queryUsername).get();
	        if(result.isPresent()){
	        	AuthenticationBean b = result.get();
	        	logger.debug("Loaded user info from db:"+b);
	        	String accountType =b.getAccountType();
        		if(accountType!=null && ("Admin".equalsIgnoreCase(accountType) || "admin-enabled".equalsIgnoreCase(accountType))){
					roleNames.add("admin");							        			
        	    	permissions.add("*");
        			logger.debug("Permission Admin(*) loaded for "+principalName);
        		}

	        }
		} catch (Exception e) {
			logger.error("Caught Exception",e);
		}
		logger.debug("Roles loaded for "+principalName+":");
		logger.debug(roleNames);
		return Tuple2.apply(roleNames, permissions);
	}

	
}
