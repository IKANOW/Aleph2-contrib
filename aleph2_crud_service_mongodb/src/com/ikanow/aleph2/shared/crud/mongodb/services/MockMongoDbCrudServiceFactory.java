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
package com.ikanow.aleph2.shared.crud.mongodb.services;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.fakemongo.Fongo;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.typesafe.config.Config;

/** A factory for accessing the MongoDB instance and its generic wrapper - interface, mock version
 * @author acp
 */
public class MockMongoDbCrudServiceFactory implements IMongoDbCrudServiceFactory {
	private static final Logger _logger = LogManager.getLogger();

	// Compromize: create different fongos for each thread - that way within a thread/test it will work
	// but different threads (/tests) will see different dbs
	// Then have a config object that lets you hardwire it
	private static Fongo _fongo_single = null;
	private static ThreadLocal<Fongo> _fongo = new ThreadLocal<Fongo>() {
		@Override protected Fongo initialValue() {
			return _fongo_single == null ? new Fongo("aleph2") : _fongo_single;
		}
	};
	
	public final static String THREAD_CONFIG = "MockMongoDbCrudServiceFactory.one_per_thread";
	
	public MockMongoDbCrudServiceFactory() {		
		Config static_config = ModuleUtils.getStaticConfig();
		
		synchronized (MockMongoDbCrudServiceFactory.class) {
			if (!static_config.hasPath(THREAD_CONFIG) || !static_config.getBoolean(THREAD_CONFIG)) {
				if (null == _fongo_single) {
					_logger.info("Creating single 'fongo' DB instance called 'aleph2'");
					_fongo_single = new Fongo("aleph2");
				}
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.IMongoDbCrudServiceFactory#getMongoDb(java.lang.String)
	 */
	public
	DB getMongoDb(String db_name) { 
		synchronized (MockMongoDbCrudServiceFactory.class) {
			return _fongo.get().getDB(db_name);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.IMongoDbCrudServiceFactory#getMongoDbCollection(java.lang.String)
	 */
	public DBCollection getMongoDbCollection(final String db_name_and_collection) {
		final String[] db_coll = db_name_and_collection.split("\\s*[.]\\s*");
		return getMongoDbCollection(db_coll[0], db_coll[1]);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.IMongoDbCrudServiceFactory#getMongoDbCollection(java.lang.String, java.lang.String)
	 */
	public
	DBCollection getMongoDbCollection(String db_name, String collection_name) {
		synchronized (MockMongoDbCrudServiceFactory.class) {
			return _fongo.get().getDB(db_name).getCollection(collection_name);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.IMongoDbCrudServiceFactory#getMongoDbCrudService(java.lang.Class, java.lang.Class, com.mongodb.DBCollection, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	public <O, K> MongoDbCrudService<O, K> getMongoDbCrudService(final Class<O> bean_clazz, final Class<K> key_clazz, 
			final DBCollection coll, 
			final Optional<String> auth_fieldname, final Optional<AuthorizationBean> auth, final Optional<ProjectBean> project)
	{
		return new MongoDbCrudService<O, K>(bean_clazz, key_clazz, coll, auth_fieldname, auth, project);
	}
}
