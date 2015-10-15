package com.ikanow.aleph2.security.service;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;

public class IkanowV1DataModificationChecker implements IModificationChecker{

	protected IServiceContext _context;
	private ICrudService<JsonNode> communityDb = null;
	protected IManagementDbService _underlying_management_db = null;
	private static final Logger logger = LogManager.getLogger(IkanowV1CommunityRoleProvider.class);
	private Date lastModified = null;



	@Inject
	public IkanowV1DataModificationChecker(IServiceContext serviceContext){
		this._context = serviceContext;
	}
	
	public boolean isModified(){
		Optional<JsonNode> result;
		try {
						
			// index modified
			if(lastModified==null){
				// wait for index
				getCommunityDb().optimizeQuery(Arrays.asList("modified")).thenApply(this::queryLastModifiedDate);//.thenRun(queryLastModifiedRunnable);
				return true;
			}
		} catch (Exception e) {
				logger.error("Caught Exception",e);
		}
		return false;
	}
	
	private Date queryLastModifiedDate(Boolean wasIndexed) {
		if(wasIndexed){
			
			SingleQueryComponent<JsonNode> query = CrudUtils.allOf().orderBy(new Tuple2<String,Integer>("modified",-1)).limit(1);			
			CompletableFuture<Cursor<JsonNode>> fCursor = getCommunityDb().getObjectsBySpec(query, Arrays.asList("modified"), false);
			Iterator<JsonNode> it;
			try {
				it = fCursor.get().iterator();
				if(it.hasNext()){
					JsonNode n = it.next();
					logger.debug(n);
				}
				}
					 catch (Exception e) {
				logger.error("queryLastModifiedDate caught Exception",e);
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	protected void initDb() {
		if (communityDb == null) {
			String communityOptions = "social.community";
			communityDb = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of(communityOptions)).get();
		}
	}

	protected ICrudService<JsonNode> getCommunityDb(){
		if (_underlying_management_db == null) {
			_underlying_management_db = _context.getService(IManagementDbService.class, Optional.empty()).get();
		}

		if(communityDb == null) {
			initDb();
		}
	      return communityDb;		
	}


}
