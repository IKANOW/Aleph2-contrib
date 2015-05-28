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
 ******************************************************************************/
package com.ikanow.aleph2.management_db.mongodb.services;

import java.util.Arrays;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.module.MockMongoDbManagementDbModule;
import com.ikanow.aleph2.shared.crud.mongodb.services.IMongoDbCrudServiceFactory;

public class MockMongoDbManagementDbService extends MongoDbManagementDbService {

	/** Guice generated constructor
	 * @param crud_factory
	 */
	@Inject
	public MockMongoDbManagementDbService(
			IMongoDbCrudServiceFactory crud_factory, 
			final MongoDbManagementDbConfigBean properties,
			final IkanowV1SyncService sync_service)
	{
		super(crud_factory, properties, sync_service);

		//DEBUG
		//System.out.println("Hello world from: " + this.getClass() + ": underlying=" + crud_factory);
	}
	/** This service needs to load some additional classes via Guice. Here's the module that defines the bindings
	 * @return
	 */
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new MockMongoDbManagementDbModule());
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader#youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules()
	 */
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// (done see above)		
	}
}
