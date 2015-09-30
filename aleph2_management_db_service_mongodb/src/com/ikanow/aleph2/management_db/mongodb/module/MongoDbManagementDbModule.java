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

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.services.IkanowV1SyncService_Buckets;
import com.ikanow.aleph2.management_db.mongodb.services.IkanowV1SyncService_LibraryJars;
import com.ikanow.aleph2.management_db.mongodb.services.IkanowV1SyncService_PurgeBuckets;
import com.ikanow.aleph2.management_db.mongodb.services.IkanowV1SyncService_TestBuckets;
import com.ikanow.aleph2.shared.crud.mongodb.data_model.MongoDbConfigurationBean;
import com.ikanow.aleph2.shared.crud.mongodb.services.IMongoDbCrudServiceFactory;
import com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudServiceFactory;
import com.typesafe.config.Config;

/** A module to instantiate private services in the MongoDB "underlying" management DB service
 *  THIS CLASS HAS NO COVERAGE SO NEED TO HANDLE TEST ON MODIFICATION
 * @author acp
 */
public class MongoDbManagementDbModule extends AbstractModule {

	protected IMongoDbCrudServiceFactory _crud_service_factory;
	
	/** User constructor
	 */
	public MongoDbManagementDbModule() {}
	
	/** Guice generated constructor
	 * @param crud_service_factory
	 */
	@Inject
	public MongoDbManagementDbModule(
			IMongoDbCrudServiceFactory crud_service_factory
			)
	{
		_crud_service_factory = crud_service_factory;
		
		//DEBUG
		//System.out.println("Hello world from: " + this.getClass() + ": underlying=" + crud_service_factory);
	}
	
	/* (non-Javadoc)
	 * @see com.google.inject.AbstractModule#configure()
	 */
	public void configure() {		
		final Config config = ModuleUtils.getStaticConfig();				
		MongoDbManagementDbConfigBean bean;
		try {
			bean = BeanTemplateUtils.from(PropertiesUtils.getSubConfig(config, MongoDbManagementDbConfigBean.PROPERTIES_ROOT).orElse(null), MongoDbManagementDbConfigBean.class);
		} 
		catch (Exception e) {
			throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_CONFIG_ERROR,
					MongoDbConfigurationBean.class.toString(),
					config.getConfig(MongoDbManagementDbConfigBean.PROPERTIES_ROOT)
					), e);
		}
		this.bind(MongoDbConfigurationBean.class).toInstance(bean); // (for crud service)
		this.bind(MongoDbManagementDbConfigBean.class).toInstance(bean); // (for mgmt db)
		this.bind(IkanowV1SyncService_Buckets.class).in(Scopes.SINGLETON);
		this.bind(IkanowV1SyncService_LibraryJars.class).in(Scopes.SINGLETON);
		this.bind(IkanowV1SyncService_TestBuckets.class).in(Scopes.SINGLETON);
		this.bind(IkanowV1SyncService_PurgeBuckets.class).in(Scopes.SINGLETON);
		this.bind(IMongoDbCrudServiceFactory.class).to(MongoDbCrudServiceFactory.class).in(Scopes.SINGLETON);
		this.bind(BucketTestService.class).in(Scopes.SINGLETON);
	}

	
	/** This is here simply so we can override it in tests rather than actually make the core_mgmt_db test call
	 * @author Burch
	 */
	public static class BucketTestService {
		public ManagementFuture<Boolean> test_bucket(final IManagementDbService core_management_db, final DataBucketBean to_test, final ProcessingTestSpecBean test_spec) {
			return core_management_db.testBucket(to_test, test_spec);
		}
	}
	
}
