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

import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.mongodb.DB;
import com.mongodb.DBCollection;

/** A factory for accessing the MongoDB instance and its generic wrapper - interface, can be real or mock
 * @author acp
 */
public interface IMongoDbCrudServiceFactory {

	/** Get the MongoDB DB instance for the given name
	 * @param db_name - the DB name
	 * @return the DB driver
	 */
	@NonNull
	DB getMongoDb(final @NonNull String db_name);

	/**Get the MongoDB DB collection instance for the given names
	 * @param db_name - the DB name
	 * @param collection_name - the collection name
	 * @return the DBCollection driver
	 */
	@NonNull
	DBCollection getMongoDbCollection(final @NonNull String db_name, final @NonNull String collection_name);

	/**Get the MongoDB DB collection instance for the given names
	 * @param db_name_and_collection - <the DB name>.<the collection name>
	 * @return the DBCollection driver
	 */
	@NonNull
	DBCollection getMongoDbCollection(final @NonNull String db_name_and_collection);

	/** A factory to obtain a CrudService
	 * @param bean_clazz - the class to which this CRUD service is being mapped
	 * @param key_clazz - if you know the type of the _id then add this here, else use Object.class (or ObjectId to use MongoDB defaults)
	 * @param coll - must provide the MongoDB collection
	 * @param auth_fieldname - optionally, the fieldname to which auth/project beans are applied
	 * @param auth - optionally, an authorization overlay added to each query
	 * @param project - optionally, a project overlay added to each query
	 */
	@NonNull
	<O, K> MongoDbCrudService<O, K> getMongoDbCrudService(final @NonNull Class<O> bean_clazz, final @NonNull Class<K> key_clazz, 
						final @NonNull DBCollection coll, 
						final Optional<String> auth_fieldname, final Optional<AuthorizationBean> auth, final Optional<ProjectBean> project);
}
