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
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.management_db.mongodb.module.MongoDbManagementDbModule;
import com.ikanow.aleph2.shared.crud.mongodb.services.IMongoDbCrudServiceFactory;
import com.mongodb.DB;
import com.mongodb.DBCollection;

/** Implementation of the management DB service using MongoDB (or mock MongoDB) 
 * @author acp
 *
 */
public class MongoDbManagementDbService implements IManagementDbService, IExtraDependencyLoader  {

	protected IMongoDbCrudServiceFactory _crud_factory;
	
	/** Guice generated constructor
	 * @param crud_factory
	 */
	@Inject
	public MongoDbManagementDbService(IMongoDbCrudServiceFactory crud_factory) {
		_crud_factory = crud_factory;

		//DEBUG
		//System.out.println("Hello world from: " + this.getClass() + ": underlying=" + crud_factory);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getSharedLibraryStore()
	 */
	public IManagementCrudService<SharedLibraryBean> getSharedLibraryStore() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerLibraryState(java.lang.Class, com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getPerLibraryState(Class<T> clazz,
			SharedLibraryBean library, Optional<String> sub_collection) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getDataBucketStore()
	 */
	public IManagementCrudService<DataBucketBean> getDataBucketStore() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getDataBucketStatusStore()
	 */
	public IManagementCrudService<DataBucketStatusBean> getDataBucketStatusStore() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerBucketState(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getPerBucketState(Class<T> clazz,
			DataBucketBean bucket, Optional<String> sub_collection) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getAnalyticThreadStore()
	 */
	public IManagementCrudService<AnalyticThreadBean> getAnalyticThreadStore() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerAnalyticThreadState(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getPerAnalyticThreadState(Class<T> clazz,
			AnalyticThreadBean analytic_thread, Optional<String> sub_collection) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	public <T> T getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		
		if (DBCollection.class == driver_class) {
			if (!driver_options.isPresent()) {
				throw new RuntimeException("If requesting DB collection, need to specify db_name.coll_name");
			}
			final String[] db_coll = driver_options.get().split("[.]", 2);
			if (2 != db_coll.length) {
				throw new RuntimeException("If requesting DB collection, need to specify db_name.coll_name: " + driver_options.get());
			}
			return (T) _crud_factory.getMongoDbCollection(db_coll[0], db_coll[1]);
		}
		else if (DB.class == driver_class) {
			if (!driver_options.isPresent()) {
				throw new RuntimeException("If requesting DB, need to specify db_name");
			}
			return (T) _crud_factory.getMongoDb(driver_options.get());
		}
		else if (ICrudService.class == driver_class) {
			if (!driver_options.isPresent()) {
				throw new RuntimeException("If requesting a CRUD service, need to specify db_name.coll_name[/fully.qualified.bean.class]");
			}
			final String[] dbcoll_clazz = driver_options.get().split("[/]", 2);
			final String[] db_coll = dbcoll_clazz[0].split("[.]", 2);
			if (2 != db_coll.length) {
				throw new RuntimeException("If requesting a CRUD service, need to specify db_name.coll_name[/fully.qualified.bean.class]: " + driver_options.get());
			}
			try {
				if (2 != dbcoll_clazz.length) { // returns the JSON version
					return (T) _crud_factory.getMongoDbCrudService(
							JsonNode.class, Object.class, _crud_factory.getMongoDbCollection(db_coll[0], db_coll[1]), 
							Optional.empty(), Optional.empty(), Optional.empty());
				}
				else {
					return (T) _crud_factory.getMongoDbCrudService(
							Class.forName(dbcoll_clazz[1]), Object.class, _crud_factory.getMongoDbCollection(db_coll[0], db_coll[1]), 
							Optional.empty(), Optional.empty(), Optional.empty());
				}
			}
			catch (Exception e) {
				throw new RuntimeException("Requesting CRUD service, specified invalid class name: " + driver_options.get());
			}
		}
		return null;
	}

	/** This service needs to load some additional classes via Guice. Here's the module that defines the bindings
	 * @return
	 */
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new MongoDbManagementDbModule());
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader#youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules()
	 */
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// (done see above)		
	}
}
