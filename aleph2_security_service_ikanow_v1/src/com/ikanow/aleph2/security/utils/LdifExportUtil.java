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
package com.ikanow.aleph2.security.utils;

import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.security.service.AuthenticationBean;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
public class LdifExportUtil {
	private static final Logger logger = LogManager.getLogger(LdifExportUtil.class);

	private ICrudService<JsonNode> personDb = null;
	private ICrudService<JsonNode> sourceDb = null;
	private ICrudService<JsonNode> shareDb = null;
	private IManagementCrudService<DataBucketBean> bucketDb = null;
	protected IManagementDbService _core_management_db = null;
	protected IManagementDbService _underlying_management_db = null;
	private ICrudService<JsonNode> communityDb = null;
	private ICrudService<AuthenticationBean> authenticationDb = null;

	public static String SYSTEM_COMMUNITY_ID ="4c927585d591d31d7b37097a";
	protected Config config = null;

	@Inject
	protected IServiceContext _temp_service_context = null;	
	protected static IServiceContext _service_context = null;

	protected IManagementDbService _management_db;

	

	public LdifExportUtil(){
		setup();
	}
	

	@SuppressWarnings("unchecked")
	protected void initDb() {
		if (_core_management_db == null) {
			_core_management_db = _service_context.getCoreManagementDbService();
		}
		if (_underlying_management_db == null) {
			_underlying_management_db = _service_context.getService(IManagementDbService.class, Optional.empty()).get();
		}
		if (personDb == null) {
			String personOptions = "social.person";
			personDb = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of(personOptions)).get();
		}
		if (sourceDb == null) {
			String ingestOptions = "ingest.source";
			sourceDb = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of(ingestOptions)).get();
		}
		if (bucketDb == null) {
			bucketDb = _core_management_db.getDataBucketStore();
		}
		if (shareDb == null) {
			String shareOptions = "social.share";
			shareDb = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of(shareOptions)).get();
		}
		if (communityDb == null) {
			String communityOptions = "social.community";
			communityDb = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of(communityOptions)).get();
		}
		
		if (personDb == null) {
		String personOptions = "social.person";
		personDb = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of(personOptions)).get();
		}
		
		if (authenticationDb == null) {
		String authDboptions = "security.authentication/"+AuthenticationBean.class.getName();
        authenticationDb = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of(authDboptions)).get();
        //logger.debug(authenticationDb.getUnderlyingPlatformDriver(DBCollection.class, Optional.empty()).get().getClass().getSimpleName());
        //crud.getUnderlyingPlatform
		}
	}

		public void exportToLdif(String outPath){
		// copy Template
		Path pOutPath = Paths.get(outPath);
		
		try {
			Files.copy(this.getClass().getResourceAsStream("aleph2_template.ldif"), pOutPath, StandardCopyOption.REPLACE_EXISTING);
			String userEntries = createUserEntries();
			
			Files.write(pOutPath,  userEntries.getBytes(), StandardOpenOption.APPEND);
		} catch (Exception e) {
			logger.error(e);
		}
	}
	
		protected String createUserEntries() throws Exception{
			// TODO Auto-generated method stub
			StringBuffer sb =  new StringBuffer();
			List<AuthenticationBean> auths = loadAuthentication();
			for (AuthenticationBean authenticationBean : auths) {
				
				String profileId = authenticationBean.getProfileId();
				ObjectId objecId = new ObjectId(profileId); 
				Optional<JsonNode> result = getPersonDb().getObjectBySpec(CrudUtils.anyOf().when("_id", objecId)).get();
				String firstName = "";
				String lastName = "";
				String email = "";
				String displayName = "";
		        if(result.isPresent()){
		        	JsonNode person = result.get();
		        	firstName = extractField(person,"firstName");
		        	lastName = extractField(person,"lastName");
		        	email = extractField(person,"email");
		        	displayName = extractField(person,"displayName");
		        }
				//
				sb.append("\n");
				String dn = "dn: cn="+profileId+",ou=users,ou=aleph2,dc=ikanow,dc=com";
				sb.append(dn);
				sb.append("\n");
				sb.append("objectclass: inetOrgPerson");
				sb.append("\n");
				sb.append("cn: "+firstName+" "+lastName);
				sb.append("\n");
				sb.append("cn: "+profileId);
				sb.append("\n");
				sb.append("givenName: "+firstName);
				sb.append("\n");
				sb.append("sn: "+lastName);
				sb.append("\n");
				sb.append("mail: "+email);
				sb.append("\n");
				sb.append("displayName: "+displayName);
				sb.append("\n");
				sb.append("employeeNumber: "+profileId);
				sb.append("\n");				
				sb.append("uid: "+authenticationBean.getUsername());
				sb.append("\n");
				sb.append("userPassword: {sha256}"+authenticationBean.getPassword());
				sb.append("\n");
				sb.append("employeeType: "+authenticationBean.getAccountType());
				sb.append("\n");
				sb.append("ou: aleph2");
				sb.append("\n");
			}
			return sb.toString();
		}

	    protected List<AuthenticationBean> loadAuthentication() throws Exception {

	    	List<AuthenticationBean> auths = new ArrayList<AuthenticationBean>();
				SingleQueryComponent<AuthenticationBean> query = CrudUtils.anyOf(AuthenticationBean.class).withPresent("_id") ;
			//SingleQueryComponent<AuthenticationBean> query = CrudUtils.allOf(AuthenticationBean.class).orderBy(Tuples._2T("_id", -1)).limit(100);
				//SingleQueryComponent<AuthenticationBean> query = CrudUtils.allOf(AuthenticationBean.class).when("username", "testuser@ikanow.com");
	    		Cursor<AuthenticationBean> fCursor = getAuthenticationStore().getObjectsBySpec(query).get();
	    		//long count = fCursor.count();
				Iterator<AuthenticationBean> it = fCursor.iterator();
				
				while(it.hasNext()) {
					auths.add(it.next());
				}
	        return auths;
	    }

		public void setup() {
		try {

		if (null == _service_context) {
			final String temp_dir = System.getProperty("java.io.tmpdir");
	
			// OK we're going to use guice, it was too painful doing this by hand...
			config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("ldif_export.properties")))
					.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
					.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
					.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
					.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));
	
			Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));	
			app_injector.injectMembers(this);
			_service_context = _temp_service_context;
		}
		this._management_db = _service_context.getCoreManagementDbService();
		
		} catch(Throwable e) {
			
			e.printStackTrace();
		}
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
		protected ICrudService<JsonNode> getCommunityDb(){
			if(communityDb == null) {
				initDb();
			}
		      return communityDb;		
		}

		protected ICrudService<JsonNode> getPersonStore(){
			if(personDb == null){
				initDb();
			}
		      return personDb;		
		}
		protected ICrudService<AuthenticationBean> getAuthenticationStore(){
			if(authenticationDb == null){
				initDb();
			}
		      return authenticationDb;		
		}

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

	
	public static void main(String[] args) {
		LdifExportUtil util = new LdifExportUtil();
		util.exportToLdif("aleph2.ldif");
		System.exit(0);
	}
	
/*	public Tuple2<Set<String>, Set<String>> getRolesAndPermissions2(String principalName) {
		
        Set<String> roleNames = new HashSet<String>();
        Set<String> permissions = new HashSet<String>();
//		Cursor<JsonNode> result;
		Optional<JsonNode> result;
		try {
			
			ObjectId objecId = new ObjectId(principalName); 
			result = getPersonDb().getObjectBySpec(CrudUtils.anyOf().when("_id", objecId)).get();
			roleNames.add(principalName);						
	        if(result.isPresent()){
	        	JsonNode person = result.get();
	        	JsonNode communities = person.get("communities"); 
	        	if (communities!=null && communities.isArray()) {
					for (Iterator<JsonNode> it = communities.iterator(); it.hasNext();) {
	        	    JsonNode community = it.next();
	        	    	JsonNode type = community.get("type");
	        	    	if(type==null || "data".equalsIgnoreCase(type.asText())){
		        	    	String communityId = community.get("_id").asText();
		        	    	if(!SYSTEM_COMMUNITY_ID.equals(communityId)){
		        	    	String communityName = community.get("name").asText();
		        	    	permissions.add(communityId);
		        	    	Tuple2<Set<String>,Set<String>> sourceAndBucketIds = loadSourcesAndBucketIdsByCommunityId(communityId);
		        	    	// add all sources to permissions
		        	    	permissions.addAll(sourceAndBucketIds._1());
		        	    	// add all bucketids to permissions
		        			logger.debug(sourceAndBucketIds._2());
		        	    	permissions.addAll(sourceAndBucketIds._2());
		        	    	Set<String> bucketIds = sourceAndBucketIds._2();
		        			logger.debug("Permission (SourceIds) loaded for "+principalName+",("+communityName+"):");
		        			logger.debug(sourceAndBucketIds._1());
		        			logger.debug("Permission (BucketIds) loaded for "+principalName+",("+communityName+"):");
		        	    	if(!bucketIds.isEmpty()){
		        	    		Set<String> bucketPaths  = loadBucketPathsbyIds(bucketIds);
		        	    		Set<String> bucketPathPermissions = convertPathtoPermissions(bucketPaths);
		        	    		permissions.addAll(bucketPathPermissions);
		        	    	}
		        	    	Set<String> shareIds = loadShareIdsByCommunityId(communityId);
		        	    	permissions.addAll(shareIds);
		        			logger.debug("Permission (ShareIds) loaded for "+principalName+",("+communityName+"):");
		        			logger.debug(shareIds);
		        	    	}
	        	    	}
					} // it
	        	}
	        }
		} catch (Exception e) {
			logger.error("Caught Exception",e);
		}
		logger.debug("Roles loaded for "+principalName+":");
		logger.debug(roleNames);
		return Tuple2.apply(roleNames, permissions);
	}
*/
	
	/**
	 * Returns the sourceIds and the bucket IDs associated with the community.
	 * @param communityId
	 * @return
	 * @throws ExecutionException 
	 * @throws InterruptedException 
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
	 */

	protected static String extractField(JsonNode node, String field) {
		String value = "";
		if (node != null) {
			JsonNode fieldNode = node.get(field);
			if (fieldNode != null) {
				value = fieldNode.asText();
			}
		}
		return value;
	}
}
