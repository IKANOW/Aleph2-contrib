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
package com.ikanow.aleph2.management_db.mongodb.utils;

import java.util.stream.IntStream;

import scala.Tuple3;

import com.ikanow.aleph2.data_model.utils.Tuples;
import com.mongodb.DB;
import com.mongodb.Mongo;

/** Utilities for handling naming bucket/library/analytics stores
 * @author Alex
 */
public class MongoDbCollectionUtils {

	private static final int MAX_COLLS_PER_DB = 201;
	
	/** Finds the right database - iterates through DBs until it either finds the collection or not, if not then it finds the first DB with < 200 collections in it
	 * @param client
	 * @param collection_name
	 * @return
	 */
	public static DB findDatabase(final Mongo client, String db_name_prefix, String collection_name) {

		// Does the collection exist? Keep looking until we find an empty DB
		final Tuple3<Boolean, Boolean, DB> intermediate = IntStream.iterate(1, i -> i + 1).boxed()
			.map(ii -> db_name_prefix + "_" + ii)
			.map(dbn -> client.getDB(dbn))
			.map(db -> {
				final boolean is_empty = db.getCollectionNames().isEmpty();
				final boolean contains_names = !is_empty && db.collectionExists(collection_name);
				return Tuples._3T(is_empty, contains_names, db);
			})
			.filter(t3 -> t3._1() || t3._2())
			.findFirst().get();
		//(guaranteed we'll either find an empty DB or one containing our collection)
		
		// If the collection doesn't exist, find the first available DB 
		return intermediate._2()
				? intermediate._3()
				: IntStream.iterate(1, i -> i + 1).boxed()
					.map(ii -> db_name_prefix + "_" + ii)
					.map(dbn -> client.getDB(dbn))
					.filter(db -> db.getCollectionNames().size() < MAX_COLLS_PER_DB)
					.findFirst()
					.get();
	}
}
