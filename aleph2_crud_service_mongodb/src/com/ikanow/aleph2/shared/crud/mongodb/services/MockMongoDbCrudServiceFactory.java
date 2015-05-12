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

import org.checkerframework.checker.nullness.qual.NonNull;

import com.github.fakemongo.Fongo;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.mongodb.DB;
import com.mongodb.DBCollection;

/** A factory for accessing the MongoDB instance and its generic wrapper - interface, mock version
 * @author acp
 */
public class MockMongoDbCrudServiceFactory implements IMongoDbCrudServiceFactory {

	private static Fongo _fongo;
	
	public MockMongoDbCrudServiceFactory() {
		_fongo = new Fongo("aleph2");
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.IMongoDbCrudServiceFactory#getMongoDb(java.lang.String)
	 */
	@NonNull
	public
	DB getMongoDb(String db_name) { 
		synchronized (MockMongoDbCrudServiceFactory.class) {
			return _fongo.getDB(db_name);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.IMongoDbCrudServiceFactory#getMongoDbCollection(java.lang.String, java.lang.String)
	 */
	@NonNull
	public
	DBCollection getMongoDbCollection(String db_name, String collection_name) {
		synchronized (MockMongoDbCrudServiceFactory.class) {
			return _fongo.getDB(db_name).getCollection(collection_name);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.IMongoDbCrudServiceFactory#getMongoDbCrudService(java.lang.Class, java.lang.Class, com.mongodb.DBCollection, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	@NonNull
	public <O, K> MongoDbCrudService<O, K> getMongoDbCrudService(final @NonNull Class<O> bean_clazz, final @NonNull Class<K> key_clazz, 
			final @NonNull DBCollection coll, 
			final Optional<String> auth_fieldname, final Optional<AuthorizationBean> auth, final Optional<ProjectBean> project)
	{
		return new MockMongoDbCrudService<O, K>(bean_clazz, key_clazz, coll, auth_fieldname, auth, project);
	}
}
