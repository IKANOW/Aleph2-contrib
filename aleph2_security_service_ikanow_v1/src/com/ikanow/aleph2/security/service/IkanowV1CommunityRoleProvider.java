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
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.security.interfaces.IRoleProvider;

public class IkanowV1CommunityRoleProvider implements IRoleProvider {
	private ICrudService<JsonNode> personDb = null;
	protected final IServiceContext _context;
	protected IManagementDbService _core_management_db = null;
	protected IManagementDbService _underlying_management_db = null;
	private static final Logger logger = LogManager.getLogger(IkanowV1CommunityRoleProvider.class);

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
		logger.debug("PersonDB:"+personDb);
	}

	@Inject
	public IkanowV1CommunityRoleProvider(final IServiceContext service_context){
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
	        	JsonNode communities = result.get().get("communities");
	        	if (communities.isArray()) {
					if(communities.size()>0){
						roleNames.add(principalName+"_communities");						
					}
	        	    for (final JsonNode community : communities) {
	        	    	String communityId = community.get("_id").asText();
	        	    	String communityPermission = PermissionExtractor.createPermission(ISecurityService.ROOT_PERMISSION_COMMUNITY, Optional.of(ISecurityService.ACTION_WILDCARD), communityId);
	        	    	permissions.add(communityPermission);
	        	    }
	        	}
	        }
		} catch (Exception e) {
			logger.error("Caught Exception",e);
		}
		return Tuple2.apply(roleNames, permissions);
	}

	
}
