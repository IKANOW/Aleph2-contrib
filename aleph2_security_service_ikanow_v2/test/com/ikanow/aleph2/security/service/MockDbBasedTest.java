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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class MockDbBasedTest {
	protected static final Logger logger = LogManager.getLogger(MockDbBasedTest.class);

	private String postFix="2";
	
	protected void initMockDb(IServiceContext _context){
		IManagementDbService underlying_management_db = _context.getService(IManagementDbService.class, Optional.empty()).get();
		// only fill in mock db
		if(underlying_management_db.getClass().getName().contains("Mock")){
			// safety so we dont overwrite remote db
			postFix=""; 
			initAuthentication(underlying_management_db);
		}
	}
	

	protected void initAuthentication(IManagementDbService underlying_management_db){
		try {
			String authDboptions = "security.authentication"+postFix;
			DBCollection authenticationDb = underlying_management_db.getUnderlyingPlatformDriver(DBCollection.class, Optional.of(authDboptions)).get();
			//clear db			
			authenticationDb.drop();
			 //Open the file for reading
			parseLinesAndSave("/data/security.authentication.json",authenticationDb);
		} catch (Exception e) {
			logger.error("Caught Exception",e);
		} 
	}

	protected static void parseLinesAndSave(String jsonResourceNme, DBCollection dbCollection){
		try {
			//clear db			
			dbCollection.drop();
			 //Open the file for reading
		       BufferedReader br = new BufferedReader(new InputStreamReader(MockDbBasedTest.class.getResourceAsStream(jsonResourceNme)));
		       String line=  null;
		       while ((line = br.readLine()) != null) { // while loop begins here
					Object jsonObject = JSON.parse(line);
					dbCollection.save((DBObject)jsonObject);
		       } // end while 
		} catch (Exception e) {
			logger.error("Caught Exception",e);
		} 
		
	}
	
}
