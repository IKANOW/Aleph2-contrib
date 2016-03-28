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
package com.ikanow.aleph2.security.db;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.session.Session;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;

public abstract class AbstractDb {

	protected static final Logger logger = LogManager.getLogger(SessionDb.class);
	
	protected final IServiceContext _context;
	protected IManagementDbService _core_management_db = null;
	protected IManagementDbService _underlying_management_db = null;
	protected ICrudService<JsonNode> db = null;

	public AbstractDb(final IServiceContext service_context){
		_context = service_context;		
	}

	@SuppressWarnings("unchecked")
	protected void initDb(){
		if(_core_management_db == null){
			_core_management_db = _context.getCoreManagementDbService();
		}
		if(_underlying_management_db == null) {
		_underlying_management_db = _context.getServiceProvider(IManagementDbService.class, Optional.empty()).get().get();
		}
		
		db = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of(getDbOptions())).get();
		logger.debug("Opened DB:"+getDbOptions()+":"+db);
	}

	protected abstract String getDbOptions();
	protected abstract JsonNode serialize(Object value);
	protected abstract Object deserialize(JsonNode node);

	protected ICrudService<JsonNode> getStore(){
		if(db == null){
			initDb();
		}
	      return db;		
	}



	public Session load(Object id) {
		Session s = null;
		try {
			Optional<JsonNode> ojs = getStore().getObjectById(id).get();
			if (ojs.isPresent()) {
				s = (Session) deserialize(ojs.get());
			}
		} catch (Exception e) {
			logger.error("Caught Exception loading from db:", e);
		}
		return s;
	}

	/**
	 * This method converts to a JsonNode, stores the JsonNode in the database and returns the node. 
	 * @param session
	 * @return
	 */
	public JsonNode store(Object session) {
		JsonNode js = serialize(session);
		getStore().storeObject(js,true).join();
		return js;
		
	}


	public boolean delete(Object id) {
		return getStore().deleteObjectById(id).join().booleanValue();	
	}

}
