package com.ikanow.aleph2.security.db;

import java.io.Serializable;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.session.Session;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;

public class SessionDb {
	private static final Logger logger = LogManager.getLogger(SessionDb.class);

	protected final IServiceContext _context;
	protected IManagementDbService _core_management_db = null;
	protected IManagementDbService _underlying_management_db = null;
	protected ICrudService<JsonNode> db = null;

	public SessionDb(final IServiceContext service_context){
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


	protected String getDbOptions(){
		return "aleph2_security.session";
	}
	protected ICrudService<JsonNode> getStore(){
		if(db == null){
			initDb();
		}
	      return db;		
	}

	public void store(Session session) {
		// TODO Auto-generated method stub
		
	}

	public void delete(Serializable id) {
		// TODO Auto-generated method stub
		
	}

	public Session load(Serializable sessionId) {
		// TODO Auto-generated method stub
		return null;
	}

}
