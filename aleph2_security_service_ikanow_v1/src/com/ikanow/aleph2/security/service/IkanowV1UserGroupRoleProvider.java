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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.security.interfaces.IRoleProvider;

public class IkanowV1UserGroupRoleProvider implements IRoleProvider {
	private ICrudService<JsonNode> personDb = null;
	protected final IServiceContext _context;
	protected IManagementDbService _core_management_db = null;
	protected IManagementDbService _underlying_management_db = null;
	private static final Logger logger = LogManager.getLogger(IkanowV1UserGroupRoleProvider.class);

	@SuppressWarnings("unchecked")
	protected void initDb(){
		if(_core_management_db == null){
			_core_management_db = _context.getCoreManagementDbService();
		}
		if(_underlying_management_db == null) {
		_underlying_management_db = _context.getService(IManagementDbService.class, Optional.empty()).get();
		}
		String personOptions = "social.person";
		personDb = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of(personOptions)).get();
	}

	@Inject
	public IkanowV1UserGroupRoleProvider(final IServiceContext service_context){
		_context = service_context;

	}
	
	protected ICrudService<JsonNode> getPersonStore(){
		if(personDb == null){
			initDb();
		}
	      return personDb;		
	}

	@Override
	public Tuple2<Set<String>, Set<String>> getRolesAndPermissions(String principalName) {
		
        Set<String> roleNames = new HashSet<String>();
        Set<String> permissions = new HashSet<String>();
		Optional<JsonNode> result;
		try {
			
			ObjectId objecId = new ObjectId(principalName); 
			result = getPersonStore().getObjectBySpec(CrudUtils.anyOf().when("_id", objecId)).get();
	        if(result.isPresent()){
	        	// community based roles
	        	JsonNode person = result.get();
	        	JsonNode communities = person.get("communities");
	        	if (communities!= null && communities.isArray()) {
					if(communities.size()>0){
						roleNames.add(principalName+"_user_group");						
					}
	        	    for (final JsonNode community : communities) {
	        	    	JsonNode type = community.get("type");
	        	    	if(type!=null && "user".equalsIgnoreCase(type.asText())){
		        	    	String communityId = community.get("_id").asText();
		        	    	String communityName = community.get("name").asText();
		        	    	permissions.add(communityId);
		        			logger.debug("Permission (ShareIds) loaded for "+principalName+",("+communityName+"):" + communityId);
	        	    	}
	        	    }	        	    
	        	} // communities

	        }
		} catch (Exception e) {
			logger.error("Caught Exception",e);
		}
		logger.debug("Roles loaded for "+principalName+":");
		logger.debug(roleNames);
		return Tuple2.apply(roleNames, permissions);
	}

	
}
