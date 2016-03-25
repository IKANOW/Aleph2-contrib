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

import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.shiro.session.Session;
import org.apache.shiro.session.mgt.SimpleSession;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;

public class SessionDb extends AbstractDb{

	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
			
	public SessionDb(final IServiceContext service_context){
		super(service_context);
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

	public JsonNode store(Object session) {
		JsonNode js = serialize(session);
		db.storeObject(js);
		return js;
		
	}

	protected JsonNode serialize(Object session) {
		JsonNode js= null;
		if(session instanceof Session){
			Session s  = (Session)session;
			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			 ObjectNode root = mapper.createObjectNode();			 
			 ObjectNode sessionOb = root.putObject("session");
			 sessionOb.put("_id", s.getId().toString());
			 sessionOb.put("last_access_time", sdf.format(s.getLastAccessTime()));
			 sessionOb.put("start_time_stamp", sdf.format(s.getStartTimestamp()));
			 sessionOb.put("timeout", s.getTimeout());
			 sessionOb.put("host", s.getHost());
			 ObjectNode attributesOb = sessionOb.putObject("attributes");
			 for (Iterator<Object> it = s.getAttributeKeys().iterator(); it.hasNext();) {
				Object key = it.next();
				Object value = s.getAttribute(key);
				if(value!=null){
					// base64 encode objects in session
					attributesOb.put(""+key, SerializableUtils.serialize(value));	
				}
			}
		}
		return js;
	}

	public void delete(Object id) {
		db.deleteObjectById(id);		
	}

	protected Object deserialize(JsonNode sessionOb) {
		SimpleSession s  = null;
		try {
			if(sessionOb!= null){				
				s =  new SimpleSession();
				 s.setId(sessionOb.get("_id").asText());
				 s.setLastAccessTime(sdf.parse(sessionOb.get("last_access_time").asText()));
				 s.setStartTimestamp(sdf.parse(sessionOb.get("start_time_stamp").asText()));
				 s.setTimeout(sessionOb.get("timeout").asLong());
				 s.setHost(sessionOb.get("host").asText());
				 JsonNode attributesOb = sessionOb.get("attributes");
				 for (Iterator<Entry<String, JsonNode>> it = attributesOb.fields(); it.hasNext();) {
					 Entry<String, JsonNode> e = it.next();
					 s.setAttribute(e.getKey(), SerializableUtils.deserialize(e.getValue().asText()));
				}
			}		
		} catch (Exception e) {
			logger.error("Caught Exception deserializing :"+sessionOb,e);
		}
		return s;
	}
	
	public Session load(Object id) {
		Session s = null;
		try {
			Optional<JsonNode> ojs = db.getObjectById(id).get();
			if (ojs.isPresent()) {
				s = (Session) deserialize(ojs.get());
			}
		} catch (Exception e) {
			logger.error("Caught Exception loading from db:", e);
		}
		return s;
	}

}
