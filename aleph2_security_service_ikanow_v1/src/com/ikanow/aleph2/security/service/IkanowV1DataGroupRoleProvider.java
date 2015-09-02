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
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.security.IRoleProvider;
import com.ikanow.aleph2.data_model.utils.CrudUtils;

public class IkanowV1DataGroupRoleProvider implements IRoleProvider {
	private ICrudService<JsonNode> personDb = null;
	private ICrudService<JsonNode> sourceDb = null;
	private ICrudService<JsonNode> shareDb = null;
	private IManagementCrudService<DataBucketBean> bucketDb = null;
	protected final IServiceContext _context;
	protected IManagementDbService _core_management_db = null;
	protected IManagementDbService _underlying_management_db = null;
	private static final Logger logger = LogManager.getLogger(IkanowV1DataGroupRoleProvider.class);

	@SuppressWarnings("unchecked")
	protected void initDb(){
		if(_core_management_db == null){
			_core_management_db = _context.getCoreManagementDbService();
		}
		if(_underlying_management_db == null) {
		_underlying_management_db = _context.getService(IManagementDbService.class, Optional.empty()).get();
		}
		if(personDb ==null){
		String personOptions = "social.person";
		personDb = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of(personOptions)).get();
		}
		if(sourceDb==null){
			String ingestOptions = "ingest.source";		
			sourceDb = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of(ingestOptions)).get();
			logger.debug("SourceDB:"+sourceDb);
		}
		if(bucketDb==null){
			bucketDb = _core_management_db.getDataBucketStore();
		}
		if(shareDb==null){
			String shareOptions = "social.share";
			shareDb = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of(shareOptions)).get();
		}
	}

	@Inject
	public IkanowV1DataGroupRoleProvider(final IServiceContext service_context){
		_context = service_context;

	}
	
	protected ICrudService<JsonNode> getPersonDb(){
		if(personDb == null) {
			initDb();
		}
	      return personDb;		
	}
	protected ICrudService<JsonNode> getShareDb(){
		if(shareDb == null){
			initDb();
		}
	      return shareDb;		
	}

	protected ICrudService<JsonNode> getSourceDb(){
		if(sourceDb == null){
			initDb();
		}
	      return sourceDb;		
	}

	protected ICrudService<DataBucketBean> getBucketDb(){
		if(bucketDb == null){
			initDb();
		}
	      return bucketDb;		
	}

	@Override
	public Tuple2<Set<String>, Set<String>> getRolesAndPermissions(String principalName) {
		
        Set<String> roleNames = new HashSet<String>();
        Set<String> permissions = new HashSet<String>();
		Optional<JsonNode> result;
		try {
			
			ObjectId objecId = new ObjectId(principalName); 
			result = getPersonDb().getObjectBySpec(CrudUtils.anyOf().when("_id", objecId)).get();
	        if(result.isPresent()){
	        	JsonNode communities = result.get().get("communities");
	        	if (communities.isArray()) {
					if(communities.size()>0){
						roleNames.add(principalName+"_data_group");						
					}
	        	    for (final JsonNode community : communities) {
	        	    	JsonNode type = community.get("type");
	        	    	if(type==null || "data".equalsIgnoreCase(type.asText())){
		        	    	String communityId = community.get("_id").asText();
		        	    	permissions.add(communityId);
		        	    	Tuple2<Set<String>,Set<String>> sourceAndBucketIds = loadSourcesAndBucketIdsByCommunityId(communityId);
		        	    	// add all sources to permissions
		        	    	permissions.addAll(sourceAndBucketIds._1());
		        	    	// add all bucketids to permissions
		        	    	permissions.addAll(sourceAndBucketIds._2());
		        	    	Set<String> bucketIds = sourceAndBucketIds._2();
		        	    	if(!bucketIds.isEmpty()){
		        	    		Set<String> bucketPaths  = loadBucketPathsbyIds(bucketIds);
		        	    		Set<String> bucketPathPermissions = convertPathtoPermissions(bucketPaths);
		        	    		permissions.addAll(bucketPathPermissions);
		        	    	}
		        	    	Set<String> shareIds = loadShareIdsByCommunityId(communityId);
		        	    	permissions.addAll(shareIds);
	        	    	}
	        	    }
	        	}
	        }
		} catch (Exception e) {
			logger.error("Caught Exception",e);
		}
		return Tuple2.apply(roleNames, permissions);
	}

	/** 
	 * Converts the Path into a wildcard format used by Shiro.
	 * @param bucketPaths
	 * @return
	 */
	private Set<String> convertPathtoPermissions(Set<String> bucketPaths) {
		Set<String> bucketPathPermissions = new HashSet<String>();
		for (String bucketPath : bucketPaths) {
			String bucketPermission = bucketPath.replaceAll("/", ":");
			if(bucketPermission.startsWith(":")){
				bucketPermission = bucketPermission.substring(1);
			}
			bucketPathPermissions.add(bucketPermission);
		}
		return bucketPathPermissions;
	}

	/**
	 * Returns the sourceIds and the bucket IDs associated with the community.
	 * @param communityId
	 * @return
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	protected Tuple2<Set<String>, Set<String>> loadSourcesAndBucketIdsByCommunityId(String communityId) throws InterruptedException, ExecutionException {
		Set<String> sourceIds = new HashSet<String>();
		Set<String> bucketIds = new HashSet<String>();		
		ObjectId objecId = new ObjectId(communityId); 
		Cursor<JsonNode> cursor = getSourceDb().getObjectsBySpec(CrudUtils.anyOf().when("communityIds", objecId)).get();

	        		for (Iterator<JsonNode> it = cursor.iterator(); it.hasNext();) {
	        			JsonNode source = it.next();
	        			String sourceId = source.get("_id").asText();	
	        			sourceIds.add(sourceId);
	        			JsonNode extracType = source.get("extractType");
	        			if(extracType!=null && "V2DataBucket".equalsIgnoreCase(extracType.asText())){
	        				JsonNode bucketId = source.get("key");
	        				if(bucketId !=null){
	        					// TODO HACK , according to Alex, buckets have a semicolon as last id character to facilitate some string conversion 
	        					bucketIds.add(bucketId.asText()+";");
	        				}
	        			}
	        			// bucket id
	        		}
					
		return new Tuple2<Set<String>, Set<String>>(sourceIds,bucketIds);
	}

	protected Set<String> loadBucketPathsbyIds(Set<String> bucketIds) throws InterruptedException, ExecutionException {
		Set<String> bucketPaths = new HashSet<String>();
		Cursor<DataBucketBean> cursor = getBucketDb().getObjectsBySpec(CrudUtils.anyOf(DataBucketBean.class).withAny("_id", bucketIds)).get();

	        		for (Iterator<DataBucketBean> it = cursor.iterator(); it.hasNext();) {
	        			DataBucketBean bucket = it.next();
	        			bucketPaths.add(bucket.full_name());
	        		}
	        			// bucket id					
		return bucketPaths;
	}

	/**
	 * Returns the sourceIds and the bucket IDs associated with the community.
	 * @param communityId
	 * @return
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	protected Set<String> loadShareIdsByCommunityId(String communityId) throws InterruptedException, ExecutionException {
		Set<String> shareIds = new HashSet<String>();
		ObjectId objecId = new ObjectId(communityId); 
		Cursor<JsonNode> cursor = getShareDb().getObjectsBySpec(CrudUtils.anyOf().when("communities._id", objecId)).get();

	        		for (Iterator<JsonNode> it = cursor.iterator(); it.hasNext();) {
	        			JsonNode share = it.next();
	        			String shareId = share.get("_id").asText();	
	        			shareIds.add(shareId);	        			
	        		}
					
		return shareIds;
	}
	
}
