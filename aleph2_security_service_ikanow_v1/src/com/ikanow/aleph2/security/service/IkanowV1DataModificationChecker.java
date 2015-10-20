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

import java.text.SimpleDateFormat;
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
	private static final Logger logger = LogManager.getLogger(IkanowV1DataModificationChecker.class);
	private Date lastModified = null;



	@Inject
	public IkanowV1DataModificationChecker(IServiceContext serviceContext){
		this._context = serviceContext;
	}
	
	public boolean isModified(){
		try {
						
			// index modified
			if(lastModified==null){
				// dont wait for index
				getCommunityDb().optimizeQuery(Arrays.asList("modified")).thenApply(this::queryLastModifiedDate);
				return true;
			}
		} catch (Exception e) {
				logger.error("Caught Exception",e);
		}
		return false;
	}
	
	protected Date queryLastModifiedDate(Boolean wasIndexed) {
		if (wasIndexed) {

			SingleQueryComponent<JsonNode> query = CrudUtils.allOf().orderBy(new Tuple2<String, Integer>("modified", -1)).limit(1);
			CompletableFuture<Cursor<JsonNode>> fCursor = getCommunityDb().getObjectsBySpec(query, Arrays.asList("modified"), true);
			try {
				Iterator<JsonNode> it = fCursor.get().iterator();
				if (it.hasNext()) {
					JsonNode n = it.next();
					logger.debug(n);					
					JsonNode modifiedNode = n.get("modified");
					if (modifiedNode != null) {
						String modifiedText = modifiedNode.asText();
						// logger.debug(modifiedText);
						// example Thu Aug 13 15:44:08 CDT 2015
						SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
						lastModified = sdf.parse(modifiedText);
						// logger.debug(lastModified);
					}
				}
			} catch (Throwable e) {
				logger.error("queryLastModifiedDate caught Exception", e);
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
