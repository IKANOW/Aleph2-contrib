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

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.shared.crud.mongodb.data_model.MongoDbConfigurationBean;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

/** A factory for accessing the MongoDB instance and its generic wrapper - interface, real version
 * @author acp
 */
public class MongoDbCrudServiceFactory implements IMongoDbCrudServiceFactory {

	final MongoClient _mongo_client;
	
	@Inject
	public MongoDbCrudServiceFactory(MongoDbConfigurationBean config) throws UnknownHostException {
		String mongo_config = Optional.ofNullable(config.mongodb_connection()).orElse("localhost:27017");
		// Create a list of server addresses and connect		
		String[] servers = mongo_config.split("\\s*,\\s*");
		List<ServerAddress> server_list = Arrays.asList(servers).stream()
											.map(a -> {
												try {
													String[] server_and_port = a.split("\\s*:\\s*");
													if (1 == server_and_port.length) {
														return new ServerAddress(a);
													}
													else {
														return new ServerAddress(server_and_port[0], Integer.decode(server_and_port[1]));
													}
												}
												catch (Exception e) {
													return (ServerAddress)null;
												}
											})
											.filter(sa -> null != sa)
											.collect(Collectors.toList());
		
		_mongo_client = new MongoClient(server_list);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.IMongoDbCrudServiceFactory#getMongoDb(java.lang.String)
	 */
	public
	DB getMongoDb(String db_name) { 
		return _mongo_client.getDB(db_name);
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
		return _mongo_client.getDB(db_name).getCollection(collection_name);
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
