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
package com.ikanow.aleph2.shared_services.crud.mongodb;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.bson.types.ObjectId;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.mongojack.JacksonDBCollection;
import org.mongojack.internal.MongoJackModule;

import scala.Tuple2;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.shared_services.crud.mongodb.utils.MongoDbUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCollectionProxyFactory;
import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import com.mongodb.InsertOptions;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

public class MongoDbCrudService<O, K> implements ICrudService<O> {

	public final static String _ID = "_id";
	
	/** Constructor
	 * @param bean_clazz - the class to which this CRUD service is being mapped
	 * @param key_clazz - if you know the type of the _id then add this here, else use Object.class (or ObjectId to use MongoDB defaults)
	 * @param coll - must provide the MongoDB collection
	 * @param auth_fieldname - optionally, the fieldname to which auth/project beans are applied
	 * @param auth - optionally, an authorization overlay added to each query
	 * @param project - optionally, a project overlay added to each query
	 */
	MongoDbCrudService(@NonNull Class<O> bean_clazz, @NonNull Class<K> key_clazz, @NonNull DBCollection coll, Optional<String> auth_fieldname, Optional<AuthorizationBean> auth, Optional<ProjectBean> project) {
		_state = new State(bean_clazz, key_clazz, coll, auth_fieldname, auth, project);
	}
	
	/** Immutable state for the service 
	 */
	protected class State {
		State(Class<O> bean_clazz_, Class<K> key_clazz_, DBCollection coll_, Optional<String> auth_fieldname_, Optional<AuthorizationBean> auth_, Optional<ProjectBean> project_) {
			// Setup some Jackson overrides:
			ObjectMapper my_mapper = new ObjectMapper();
			MongoJackModule.configure(my_mapper);
			my_mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
			
			bean_clazz = bean_clazz_;
			key_clazz = key_clazz_;
			
			if (key_clazz == String.class) {
				try {
					string_id_field = bean_clazz.getDeclaredField(_ID);
					string_id_field.setAccessible(true);
				}
				catch (Exception e) {
					throw new RuntimeException("Internal logic error: get String _id field for " + bean_clazz, e);
				}
			}
			else {
				string_id_field = null;				
			}
			
			orig_coll = Patterns.matchAndReturn(coll_)
							.when(FongoDBCollection.class, c -> c) // (only override native DBCollections)
							.otherwise(c -> DBCollectionProxyFactory.get(c));

			coll = JacksonDBCollection.wrap(orig_coll, bean_clazz, key_clazz, my_mapper);
			auth_fieldname = auth_fieldname_;
			auth = auth_;
			project = project_;
		}		
		final Class<O> bean_clazz;
		final Class<K> key_clazz;
		final JacksonDBCollection<O, K> coll; 
		final DBCollection orig_coll;
		final Optional<String> auth_fieldname;
		final Optional<AuthorizationBean> auth;
		final Optional<ProjectBean> project;
		
		final Field string_id_field;
	}
	protected final State _state;
	
	/** creates a new object and inserts an _id field if needed
	 * @param bean the object to convert
	 * @return the converted BSON (possibly with _id inserted)
	 */
	protected DBObject convertToBson(@NonNull O bean) {
		final DBObject dbo = _state.coll.convertToDbObject(bean);
		if (null != _state.string_id_field) {
			if (!dbo.containsField(_ID)) dbo.put(_ID, new ObjectId().toString());
		}
		return dbo;
	}
	
	/** Generates a future that will error as soon as it's touched
	 * @param e - the underlying exception
	 * @return a future that errors when touched
	 */
	protected static <T> Future<T> returnError(Exception e) {
		return new CompletableFuture<T>() {
			@Override public T get() throws ExecutionException {
				throw new ExecutionException(e);
			}
		};		
	}
	
	//////////////////////////////////////////////////////

	// Authorization and project filtering:
	
	/** Returns a copy of the CRUD service that is filtered based on the client (user) and project rights
	 * @param authorization_fieldname the fieldname in the bean that determines where the per-bean authorization is held
	 * @param client_auth Optional specification of the user's access rights
	 * @param project_auth Optional specification of the projects's access rights
	 * @return The filtered CRUD repo
	 */
	@Override
	@NonNull
	public ICrudService<O> getFilteredRepo(
								@NonNull String authorization_fieldname,
								Optional<AuthorizationBean> client_auth,
								Optional<ProjectBean> project_auth)
	{
		return new MongoDbCrudService<O, K>(_state.bean_clazz, _state.key_clazz, _state.orig_coll, Optional.of(authorization_fieldname), client_auth, project_auth);
	}

	//////////////////////////////////////////////////////
	
	// *C*REATE
	
	/** Stores the specified object in the database, optionally failing if it is already present
	 *  If the "_id" field of the object is not set then it is assigned
	 * @param new_object
	 * @param replace_if_present if true then any object with the specified _id is overwritten
	 * @return A future containing the _id (filled in if not present in the object) - accessing the future will also report on errors via ExecutionException 
	 */
	@Override
	@NonNull
	public Future<Supplier<Object>> storeObject(O new_object, boolean replace_if_present) {
		try {
			final DBObject dbo = convertToBson(new_object);
			@SuppressWarnings("unused")
			final com.mongodb.WriteResult result = (replace_if_present) ? _state.orig_coll.save(dbo) : _state.orig_coll.insert(dbo);			
			return CompletableFuture.completedFuture(() -> dbo.get(_ID));
		}
		catch (Exception e) {
			return MongoDbCrudService.<Supplier<Object>>returnError(e);
		}
	}

	/** Stores the specified object in the database, failing if it is already present
	 *  If the "_id" field of the object is not set then it is assigned
	 * @param new_object
	 * @return An optional containing the object, or Optional.empty() if not present 
	 */
	@Override
	@NonNull
	public Future<Supplier<Object>> storeObject(O new_object) {
		return storeObject(new_object, false);
	}

	/**
	 * @param objects - a list of objects to insert
	 * @param continue_on_error if true then duplicate objects are ignored (not inserted) but the store continues
	 * @return a future containing the list of objects and the number actually inserted
	 */
	@NonNull 
	public Future<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(@NonNull List<O> new_objects, boolean continue_on_error) {
		try {
			final List<DBObject> l = new_objects.stream().map(o -> convertToBson(o)).collect(Collectors.toList());

			final com.mongodb.WriteResult orig_result = _state.orig_coll.insert(l, 
					(InsertOptions)Patterns.matchAndReturn(_state.orig_coll)
								.when(FongoDBCollection.class, c -> continue_on_error, 
										c -> new InsertOptions().continueOnError(continue_on_error).writeConcern(new WriteConcern()))
								.otherwise(c -> new InsertOptions().continueOnError(continue_on_error)));
			
			return CompletableFuture.completedFuture(
					Tuples._2T(() -> l.stream().map(o -> (Object)_state.coll.convertFromDbId(o.get(_ID))).collect(Collectors.toList()),
							() -> (Long)(long)orig_result.getN()));
		}		
		catch (Exception e) {			
			return MongoDbCrudService.<Tuple2<Supplier<List<Object>>, Supplier<Long>>>returnError(e);
		}		
	}
	
	/**
	 * @param objects - a list of objects to insert, failing out as soon as a duplicate is inserted 
	 * @return a future containing the list of objects and the number actually inserted
	 */
	@NonNull 
	public Future<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(@NonNull List<O> new_objects) {
		return storeObjects(new_objects, false);
	}
	
	//////////////////////////////////////////////////////
	
	// *R*ETRIEVE
	
	/** Registers that you wish to optimize specific queries
	 * @param ordered_field_list a list of the fields in the query
	 * @return a future describing if the optimization was successfully completed - accessing the future will also report on errors via ExecutionException
	 */
	@Override
	@NonNull
	public Future<Boolean> optimizeQuery(@NonNull List<String> ordered_field_list) {
		
		return CompletableFuture.supplyAsync(() -> {
			final BasicDBObject index_keys = new BasicDBObject(
					ordered_field_list.stream().collect(
							Collectors.toMap(f -> f, f -> 1, (v1, v2) -> 1, LinkedHashMap::new))
					);
			
			_state.orig_coll.createIndex(index_keys, new BasicDBObject("background", true));
			
			return true;
		});
	}

	/** Inform the system that a specific query optimization is no longer required
	 * @param ordered_field_list a list of the fields in the query
	 * @return whether this optimization was registered in the first place
	 */
	@Override
	@NonNull
	public boolean deregisterOptimizedQuery(@NonNull List<String> ordered_field_list) {
		try {
			final BasicDBObject index_keys = new BasicDBObject(
					ordered_field_list.stream().collect(
							Collectors.toMap(f -> f, f -> 1, (v1, v2) -> 1, LinkedHashMap::new))
					);
			
			if (_state.orig_coll instanceof FongoDBCollection) { // (doesn't do the exception, so check by hand)
				final String index_keys_str = index_keys.toString();
				
				final List<DBObject> matching_indexes = _state.orig_coll.getIndexInfo().stream()
															.filter(dbo -> index_keys_str.equals(dbo.get("key").toString()))
															.collect(Collectors.toList());
				if (matching_indexes.isEmpty()) {
					throw new MongoException("index " + index_keys_str + " didn't exist");
				}				
			}			
			return true;
		}
		catch (MongoException ex) {
			return false;
		}
	}
	
	/** Returns the object (in optional form to handle its not existing) given a simple object template that contains a unique search field (but other params are allowed)
	 * @param unique_spec A specification (must describe at most one object) generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @return A future containing an optional containing the object, or Optional.empty() - accessing the future will also report on errors via ExecutionException 
	 */
	@Override
	@NonNull
	public Future<Optional<O>> getObjectBySpec(@NonNull QueryComponent<O> unique_spec) {
		return getObjectBySpec(unique_spec, Collections.<String>emptyList(), false);
	}

	/** Returns the object (in optional form to handle its not existing) given a simple object template that contains a unique search field (but other params are allowed)
	 * @param unique_spec A specification (must describe at most one object) generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @param field_list list of fields to return, supports "." nesting
	 * @param include - if true, the field list is to be included; if false, to be excluded
	 * @return A future containing an optional containing the object, or Optional.empty() - accessing the future will also report on errors via ExecutionException
	 */
	@Override
	@NonNull 
	public Future<Optional<O>> getObjectBySpec(@NonNull QueryComponent<O> unique_spec, @NonNull List<String> field_list, boolean include) {
		try {			
			if (field_list.isEmpty()) {
				return CompletableFuture.completedFuture(Optional.ofNullable(_state.coll.findOne(MongoDbUtils.convertToMongoQuery(unique_spec)._1())));				
			}
			else {
				final BasicDBObject fields = new BasicDBObject(field_list.stream().collect(Collectors.toMap(f -> f, f -> 1)));
				return CompletableFuture.completedFuture(Optional.ofNullable(_state.coll.findOne(MongoDbUtils.convertToMongoQuery(unique_spec)._1(), fields)));
			}
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Optional<O>>returnError(e);
		}		
	}
	
	/** Returns the object given the id
	 * @param id the id of the object
	 * @return A future containing an optional containing the object, or Optional.empty() - accessing the future will also report on errors via ExecutionException
	 */
	@Override
	@NonNull
	public Future<Optional<O>> getObjectById(@NonNull Object id) {
		return getObjectById(id, Collections.<String>emptyList(), false);
	}

	/** Returns the object given the id
	 * @param id the id of the object
	 * @param field_list List of fields to return, supports "." nesting
	 * @param include - if true, the field list is to be included; if false, to be excluded
	 * @return A future containing an optional containing the object, or Optional.empty() - accessing the future will also report on errors via ExecutionException 
	 */
	@SuppressWarnings("unchecked")
	@Override
	@NonNull
	public Future<Optional<O>> getObjectById(@NonNull Object id, @NonNull List<String> field_list, boolean include) {
		try {			
			if (field_list.isEmpty()) {
				return CompletableFuture.completedFuture(Optional.ofNullable(_state.coll.findOneById((K)id)));				
			}
			else {
				final BasicDBObject fields = new BasicDBObject(field_list.stream().collect(Collectors.toMap(f -> f, f -> 1)));
				return CompletableFuture.completedFuture(Optional.ofNullable(_state.coll.findOneById((K)id, fields)));
			}
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Optional<O>>returnError(e);
		}		
	}

	/** Returns the list of objects specified by the spec (all fields returned)
	 * @param spec A specification generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @return A future containing a (possibly empty) list of Os - accessing the future will also report on errors via ExecutionException 
	 */
	@Override
	@NonNull
	public Future<Iterable<O>> getObjectsBySpec(@NonNull QueryComponent<O> spec) {
		return getObjectsBySpec(spec, Collections.<String>emptyList(), false);
	}

	/** Returns the list of objects specified by the spec
	 * @param spec A specification generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @param field_list List of fields to return, supports "." nesting
	 * @param include - if true, the field list is to be included; if false, to be excluded
	 * @return A future containing a (possibly empty) list of Os - accessing the future will also report on errors via ExecutionException 
	 */
	@Override
	@NonNull
	public Future<Iterable<O>> getObjectsBySpec(@NonNull QueryComponent<O> spec, @NonNull List<String> field_list, boolean include) {
		// TODO Auto-generated method stub
		return null;
	}

	/** Counts the number of objects specified by the spec (all fields returned)
	 * @param spec A specification generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @return A future containing the number of matching objects - accessing the future will also report on errors via ExecutionException 
	 */
	@Override
	@NonNull
	public Future<Long> countObjectsBySpec(@NonNull QueryComponent<O> spec) {
		try {			
			final Tuple2<DBObject, DBObject> query_and_meta = MongoDbUtils.convertToMongoQuery(spec);
			final Long limit = (Long)query_and_meta._2().get("$limit");
			if (null == limit) {
				return CompletableFuture.completedFuture(_state.orig_coll.count(query_and_meta._1()));
			}
			else {
				final BasicDBObject count_command = new BasicDBObject("count", _state.orig_coll.getName());
				count_command.put("query", query_and_meta._1());
				count_command.put("limit", limit);
				return CompletableFuture.completedFuture((Long)_state.orig_coll.getDB().command(count_command).get("n"));
			}
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Long>returnError(e);
		}		
	}

	//////////////////////////////////////////////////////
	
	// *U*PDATE
	
	@Override
	@NonNull
	public Future<Boolean> updateObjectById(String id, Optional<O> set,
			Optional<O> add, Optional<O> remove) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@NonNull
	public Future<Boolean> updateObjectBySpec(
			@NonNull QueryComponent<O> unique_spec, Optional<O> set,
			Optional<O> add, Optional<O> remove) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@NonNull
	public Future<Long> updateObjectsBySpec(@NonNull QueryComponent<O> spec,
			Optional<O> set, Optional<O> add, Optional<O> remove) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@NonNull
	public Future<Boolean> updateAndReturnObjectById(String id,
			Optional<O> set, Optional<O> add, Optional<O> remove,
			Optional<Boolean> before_updated) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@NonNull
	public Future<Boolean> updateAndReturnObjectBySpec(
			@NonNull QueryComponent<O> unique_spec, Optional<O> set,
			Optional<O> add, Optional<O> remove,
			Optional<Boolean> before_updated) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@NonNull
	public Future<Boolean> updateAndReturnObjectBySpec(
			@NonNull QueryComponent<O> unique_spec, Optional<O> set,
			Optional<O> add, Optional<O> remove,
			Optional<Boolean> before_updated, @NonNull List<String> field_list, boolean include) {
		// TODO Auto-generated method stub
		return null;
	}	
	
	//////////////////////////////////////////////////////
	
	// *D*ELETE
	
	@Override
	@NonNull
	public Future<Boolean> deleteObjectById(@NonNull String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@NonNull
	public Future<Boolean> deleteObjectBySpec(
			@NonNull QueryComponent<O> unique_spec) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@NonNull
	public Future<Long> deleteObjectsBySpec(@NonNull QueryComponent<O> spec) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@NonNull
	public Optional<IBasicSearchService<O>> getSearchService() {
		// TODO Auto-generated method stub
		return null;
	}

	//////////////////////////////////////////////////////
	
	// Misc
	
	@Override
	@NonNull
	public <T> T getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		// TODO Auto-generated method stub
		return null;
	}

}
