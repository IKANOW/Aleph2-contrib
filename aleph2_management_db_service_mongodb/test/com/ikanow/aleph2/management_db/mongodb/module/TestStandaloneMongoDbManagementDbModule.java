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
package com.ikanow.aleph2.management_db.mongodb.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Optional;

import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.management_db.mongodb.services.MongoDbManagementDbService;
import com.ikanow.aleph2.shared.crud.mongodb.services.IMongoDbCrudServiceFactory;
import com.ikanow.aleph2.shared.crud.mongodb.services.MockMongoDbCrudServiceFactory;
import com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudServiceFactory;

public class TestStandaloneMongoDbManagementDbModule {

	@Inject IManagementDbService _underlying_management_db_service;
	
	@Inject IMongoDbCrudServiceFactory _crud_service_factory;
	
	
	@Test
	public void testStandaloneGuiceSetup_Mock() {
		StandaloneMongoDbManagementDbModule module = new StandaloneMongoDbManagementDbModule(Optional.of(true));
		
		Injector injector = module.getInjector();
		
		assertFalse("Injector should not be null", injector == null);
		
		injector.injectMembers(this);
		
		assertFalse("_underlying_management_db_service should not be null", _underlying_management_db_service == null);		
		assertEquals(MongoDbManagementDbService.class, _underlying_management_db_service.getClass());
		
		assertFalse("_crud_service_factory should not be null", _crud_service_factory == null);		
		assertEquals(MockMongoDbCrudServiceFactory.class, _crud_service_factory.getClass());				
	}
	
	@Test
	public void testStandaloneGuiceSetup_Real() {
		StandaloneMongoDbManagementDbModule module = new StandaloneMongoDbManagementDbModule(Optional.of(false));
		
		Injector injector = module.getInjector();
		
		assertFalse("Injector should not be null", injector == null);
		
		injector.injectMembers(this);
		
		assertFalse("_underlying_management_db_service should not be null", _underlying_management_db_service == null);		
		assertEquals(MongoDbManagementDbService.class, _underlying_management_db_service.getClass());
		
		assertFalse("_crud_service_factory should not be null", _crud_service_factory == null);		
		assertEquals(MongoDbCrudServiceFactory.class, _crud_service_factory.getClass());				
	}
	
}
