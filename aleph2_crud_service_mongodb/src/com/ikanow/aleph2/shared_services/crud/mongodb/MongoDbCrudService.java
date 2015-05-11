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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.bson.types.ObjectId;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.mongojack.DBCursor;
import org.mongojack.JacksonDBCollection;
import org.mongojack.WriteResult;
import org.mongojack.internal.MongoJackModule;

import scala.Tuple2;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
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

/** An implementation of the CrudService for MongoDB
 * @author acp
 *
 * @param <O> - the bean class
 * @param <K> - the key class (typically String or ObjectId)
 */
public class MongoDbCrudService<O, K> implements ICrudService<O> {

	/** A wrapper for a Jackson DBCursor that is auto-closeable
	 * @author acp
	 *
	 * @param <O>
	 */
	public static class MongoDbCursor<O> extends Cursor<O> {
		protected MongoDbCursor(final @NonNull DBCursor<O> cursor) {
			_cursor = cursor;
		}
		protected final DBCursor<O> _cursor;
		
		@Override
		public void close() throws Exception {
			_cursor.close();
			
		}

		@Override
		@NonNull
		public Iterator<O> iterator() {
			return _cursor.iterator();
		}

		@Override
		@NonNull
		public long count() {
			return _cursor.count();
		}		
	}
	
	//TODO (ALEPH-22): add T==JsonNode as a special case (insert/query/cursor)
	// query isn't great because of query component - perhaps you can just have a query component that is JsonNode specific?
	
	public final static String _ID = "_id";
	
	/** Constructor
	 * @param bean_clazz - the class to which this CRUD service is being mapped
	 * @param key_clazz - if you know the type of the _id then add this here, else use Object.class (or ObjectId to use MongoDB defaults)
	 * @param coll - must provide the MongoDB collection
	 * @param auth_fieldname - optionally, the fieldname to which auth/project beans are applied
	 * @param auth - optionally, an authorization overlay added to each query
	 * @param project - optionally, a project overlay added to each query
	 */
	MongoDbCrudService(final @NonNull Class<O> bean_clazz, final @NonNull Class<K> key_clazz, 
						final @NonNull DBCollection coll, 
						final Optional<String> auth_fieldname, final Optional<AuthorizationBean> auth, final Optional<ProjectBean> project) {
		_state = new State(bean_clazz, key_clazz, coll, auth_fieldname, auth, project);
	}
	
	/** Immutable state for the service 
	 */
	protected class State {
		State(final Class<O> bean_clazz_, final Class<K> key_clazz_, final DBCollection coll_, 
				final Optional<String> auth_fieldname_, final Optional<AuthorizationBean> auth_, final Optional<ProjectBean> project_) {
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
			
			orig_coll = Patterns.match(coll_).<DBCollection>andReturn()
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
	@NonNull
	protected DBObject convertToBson(final @NonNull O bean) {
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
	@NonNull
	protected static <T> Future<T> returnError(final @NonNull Exception e) {
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
								final @NonNull String authorization_fieldname,
								final Optional<AuthorizationBean> client_auth,
								final Optional<ProjectBean> project_auth)
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
	public Future<Supplier<Object>> storeObject(final @NonNull O new_object, final boolean replace_if_present) {
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
	public Future<Supplier<Object>> storeObject(final @NonNull O new_object) {
		return storeObject(new_object, false);
	}

	/**
	 * @param objects - a list of objects to insert
	 * @param continue_on_error if true then duplicate objects are ignored (not inserted) but the store continues
	 * @return a future containing the list of objects and the number actually inserted
	 */
	@NonNull 
	public Future<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(final @NonNull List<O> new_objects, final boolean continue_on_error) {
		try {
			final List<DBObject> l = new_objects.stream().map(o -> convertToBson(o)).collect(Collectors.toList());

			final com.mongodb.WriteResult orig_result = _state.orig_coll.insert(l, 
						Patterns.match(_state.orig_coll)
								.<InsertOptions>andReturn()
								.when(FongoDBCollection.class, () -> continue_on_error, 
										() -> new InsertOptions().continueOnError(continue_on_error).writeConcern(new WriteConcern()))
								.otherwise(() -> new InsertOptions().continueOnError(continue_on_error)));
			
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
	public Future<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(final @NonNull List<O> new_objects) {
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
	public Future<Boolean> optimizeQuery(final @NonNull List<String> ordered_field_list) {
		
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
	public final boolean deregisterOptimizedQuery(final @NonNull List<String> ordered_field_list) {
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
	public Future<Optional<O>> getObjectBySpec(final @NonNull QueryComponent<O> unique_spec) {
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
	public Future<Optional<O>> getObjectBySpec(final @NonNull QueryComponent<O> unique_spec, final @NonNull List<String> field_list, final boolean include) {
		try {			
			if (field_list.isEmpty()) {
				return CompletableFuture.completedFuture(Optional.ofNullable(_state.coll.findOne(MongoDbUtils.convertToMongoQuery(unique_spec)._1())));				
			}
			else {
				final BasicDBObject fields = new BasicDBObject(field_list.stream().collect(Collectors.toMap(f -> f, f -> include ? 1 : 0)));
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
	public Future<Optional<O>> getObjectById(final @NonNull Object id) {
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
	public Future<Optional<O>> getObjectById(final @NonNull Object id, final @NonNull List<String> field_list, final boolean include) {
		try {			
			if (field_list.isEmpty()) {
				return CompletableFuture.completedFuture(Optional.ofNullable(_state.coll.findOneById((K)id)));				
			}
			else {
				final BasicDBObject fields = new BasicDBObject(field_list.stream().collect(Collectors.toMap(f -> f, f -> include ? 1 : 0)));
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
	public Future<Cursor<O>> getObjectsBySpec(final @NonNull QueryComponent<O> spec) {
		return getObjectsBySpec(spec, Collections.<String>emptyList(), false);
	}

	/** Returns the list of objects/order/limit specified by the spec. Note that the resulting object should be run within a try-with-resources or read fully.
	 * @param spec A specification generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @param field_list List of fields to return, supports "." nesting
	 * @param include - if true, the field list is to be included; if false, to be excluded
	 * @return A future containing a (possibly empty) list of Os - accessing the future will also report on errors via ExecutionException 
	 */
	@Override
	@NonNull
	public Future<Cursor<O>> getObjectsBySpec(final @NonNull QueryComponent<O> spec, final @NonNull List<String> field_list, final boolean include) {
		try {	
			final Tuple2<DBObject, DBObject> query_and_meta = MongoDbUtils.convertToMongoQuery(spec);
			final DBCursor<O> cursor = 
					Optional.of(Patterns.match(query_and_meta)
						.<DBCursor<O>>andReturn()
						.when(qm -> field_list.isEmpty(), qm -> _state.coll.find(qm._1()))
						.otherwise(qm -> {
							final BasicDBObject fs = new BasicDBObject(field_list.stream().collect(Collectors.toMap(f -> f, f -> include ? 1 : 0)));
							return _state.coll.find(qm._1(), fs);
						}))
						// (now we're processing on a cursor "c")
						.map(c -> {
							final DBObject sort = (DBObject)query_and_meta._2().get("$sort");
							return (null != sort) ? c.sort(sort) : c; 
						})
						.map(c -> {
							final Long limit = (Long)query_and_meta._2().get("$limit");
							return (null != limit) ? c.limit(limit.intValue()) : c; 
						})
						.get();
			
			return CompletableFuture.completedFuture(new MongoDbCursor<O>(cursor));
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Cursor<O>>returnError(e);
		}		
	}

	/** Counts the number of objects specified by the spec (all fields returned)
	 * @param spec A specification generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @return A future containing the number of matching objects - accessing the future will also report on errors via ExecutionException 
	 */
	@Override
	@NonNull
	public Future<Long> countObjectsBySpec(final @NonNull QueryComponent<O> spec) {
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
				return CompletableFuture.completedFuture(((Number)_state.orig_coll.getDB().command(count_command).get("n")).longValue());
			}
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Long>returnError(e);
		}		
	}

	/** Counts the number of objects in the data store
	 * @param spec A specification generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @return A future containing the number of matching objects - accessing the future will also report on errors via ExecutionException 
	 */
	@NonNull 
	public Future<Long> countObjects() {
		return countObjectsBySpec(CrudUtils.allOf(_state.bean_clazz));
	}
	
	
	//////////////////////////////////////////////////////
	
	// *U*PDATE
	
	/** Updates the specified object
	 * @param id the id of the object to update
	 * @param set overwrites any fields
	 * @param add increments numbers or adds to sets/lists
	 * @param remove decrements numbers of removes from sets/lists
	 * @return a future describing if the update was successful - accessing the future will also report on errors via ExecutionException
	 */
	@Override
	@NonNull
	public Future<Boolean> updateObjectById(Object id, final Optional<O> set, final Optional<QueryComponent<O>> add, final Optional<QueryComponent<O>> remove) {
		return updateObjectBySpec(CrudUtils.allOf(_state.bean_clazz).when("_id", id), Optional.of(false), set, add, remove);
	}

	/** Updates the specified object
	 * @param unique_spec A specification (must describe at most one object) generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @param upsert if specified and true then inserts the object if it doesn't exist
	 * @param set overwrites any fields
	 * @param add increments numbers or adds to sets/lists
	 * @param remove decrements numbers of removes from sets/lists
	 * @return a future describing if the update was successful - accessing the future will also report on errors via ExecutionException
	 */
	@Override
	@NonNull
	public Future<Boolean> updateObjectBySpec(final @NonNull QueryComponent<O> unique_spec, final Optional<Boolean> upsert, final Optional<O> set, final Optional<QueryComponent<O>> add, final Optional<QueryComponent<O>> remove)
	{
		try {			
			final Tuple2<DBObject, DBObject> query_and_meta = MongoDbUtils.convertToMongoQuery(unique_spec);
			final DBObject update_object = MongoDbUtils.createUpdateObject(set, add, remove);
			
			final WriteResult<O, K> wr = _state.coll.update(query_and_meta._1(), update_object, false, upsert.orElse(false));
			
			return CompletableFuture.completedFuture(wr.getN() > 0);
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Boolean>returnError(e);
		}		
	}

	/** Updates the specified object
	 * @param spec A specification generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @param upsert if specified and true then inserts the object if it doesn't exist
	 * @param set overwrites any fields
	 * @param add increments numbers or adds to sets/lists
	 * @param remove decrements numbers of removes from sets/lists
	 * @return a future describing the number of objects updated - accessing the future will also report on errors via ExecutionException
	 */
	@Override
	@NonNull
	public Future<Long> updateObjectsBySpec(final @NonNull QueryComponent<O> spec, final Optional<Boolean> upsert, 
												final Optional<O> set, final Optional<QueryComponent<O>> add, final Optional<QueryComponent<O>> remove)
	{
		try {			
			final Tuple2<DBObject, DBObject> query_and_meta = MongoDbUtils.convertToMongoQuery(spec);
			final DBObject update_object = MongoDbUtils.createUpdateObject(set, add, remove);
			
			final WriteResult<O, K> wr = _state.coll.update(query_and_meta._1(), update_object, true, false);
			
			return CompletableFuture.completedFuture((Long)(long)wr.getN());
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Long>returnError(e);
		}		
	}

	/** Updates the specified object, returning the updated version. all 3 update types to be Optional.empty() to delete the object.
	 * @param unique_spec A specification (must describe at most one object) generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...). orderBy can  be used to sort.   
	 * @param upsert if specified and true then inserts the object if it doesn't exist
	 * @param set overwrites any fields
	 * @param add increments numbers or adds to sets/lists
	 * @param remove decrements numbers of removes from sets/lists
	 * @param before_updated if specified and "true" then returns the object _before_ it is modified
	 * @param field_list List of fields to return, supports "." nesting
	 * @param include - if true, the field list is to be included; if false, to be excluded
	 * @return a future containing the object, if found (or upserted) - accessing the future will also report on errors via ExecutionException
	 */
	@Override
	@NonNull
	public Future<Optional<O>> updateAndReturnObjectBySpec(
								@NonNull QueryComponent<O> unique_spec, final Optional<Boolean> upsert, 
								final Optional<O> set, final Optional<QueryComponent<O>> add, final Optional<QueryComponent<O>> remove,
								final Optional<Boolean> before_updated, final @NonNull List<String> field_list, final boolean include)
	{
		try {			
			final Tuple2<DBObject, DBObject> query_and_meta = MongoDbUtils.convertToMongoQuery(unique_spec);
			final DBObject update_object = MongoDbUtils.createUpdateObject(set, add, remove);
			
			final BasicDBObject fields = new BasicDBObject(field_list.stream().collect(Collectors.toMap(f -> f, f -> include ? 1 : 0)));
			
			final boolean do_remove = !set.isPresent() && !add.isPresent() && !remove.isPresent();
			
			final O ret_val = _state.coll.findAndModify(query_and_meta._1(), fields, (DBObject)query_and_meta._2().get("$sort"), do_remove, update_object, !before_updated.orElse(false), upsert.orElse(false));
			
			return CompletableFuture.completedFuture(Optional.ofNullable(ret_val));
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Optional<O>>returnError(e);
		}		
	}	
	
	//////////////////////////////////////////////////////
	
	// *D*ELETE
	
	/** Deletes the specific object
	 * @param id the id of the object to update
	 * @return a future describing if the delete was successful - accessing the future will also report on errors via ExecutionException
	 */
	@SuppressWarnings("unchecked")
	@Override
	@NonNull
	public Future<Boolean> deleteObjectById(final @NonNull Object id) {
		try {			
			final WriteResult<O, K> wr = _state.coll.removeById((K)id);
			return CompletableFuture.completedFuture(wr.getN() > 0);
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Boolean>returnError(e);
		}		
	}

	/** Deletes the specific object
	 * @param unique_spec A specification (must describe at most one object) generated by CrudUtils.allOf(...) (all fields must be match) or CrudUtils.anyOf(...) (any fields must match) together with extra fields generated by .withAny(..), .withAll(..), present(...) or notPresent(...)   
	 * @return a future describing if the delete was successful - accessing the future will also report on errors via ExecutionException
	 */
	@Override
	@NonNull
	public Future<Boolean> deleteObjectBySpec(final @NonNull QueryComponent<O> unique_spec) {
		try {			
			// Delete and return the object
			final Future<Optional<O>> ret_val =
					updateAndReturnObjectBySpec(unique_spec, Optional.of(false), Optional.empty(), Optional.empty(), Optional.empty(),
													Optional.of(true), Arrays.asList("_id"), true); 					

			// Return a future that just wraps ret_val - ie returns true if the doc is present, ie was just deleted
			return new CompletableFuture<Boolean>() {
				@Override public Boolean get() throws ExecutionException, InterruptedException {
					return ret_val.get().isPresent();
				}
			};		
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Boolean>returnError(e);
		}		
	}

	/** Deletes the specific object
	 * @param A specification that must be initialized via CrudUtils.anyOf(...) and then the desired fields added via .exists(<field or getter>)
	 * @return a future describing the number of objects updated - accessing the future will also report on errors via ExecutionException
	 */
	@Override
	@NonNull
	public Future<Long> deleteObjectsBySpec(final @NonNull QueryComponent<O> spec) {
		try {			
			final Tuple2<DBObject, DBObject> query_and_meta = MongoDbUtils.convertToMongoQuery(spec);
			final Long limit = (Long) query_and_meta._2().get("$limit");
			final DBObject sort = (DBObject) query_and_meta._2().get("$sort");
			
			if ((null == limit) && (null == sort)) { // Simple case, just delete as many docs as possible
				final WriteResult<O, K> wr = _state.coll.remove(query_and_meta._1());
				return CompletableFuture.completedFuture((Long)(long)wr.getN());				
			}
			else {
				
				final com.mongodb.DBCursor cursor = 
						Optional.of(_state.orig_coll.find(query_and_meta._1(), new BasicDBObject("_id", 1)))
							// (now we're processing on a cursor "c")
							.map(c -> {
								return (null != sort) ? c.sort(sort) : c; 
							})
							.map(c -> {
								return (null != limit) ? c.limit(limit.intValue()) : c; 
							})
							.get();
				
				final List<Object> ids = StreamSupport.stream(cursor.spliterator(), false).map(o -> o.get("_id")).collect(Collectors.toList());
				
				return deleteObjectsBySpec(CrudUtils.allOf(_state.bean_clazz).withAny("_id", ids));
			}			
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Long>returnError(e);
		}		
	}

	/** Deletes the entire datastore and all documents, including mappings/indexes/metdata etc
	 * @return a future describing if the delete was successful - accessing the future will also report on errors via ExecutionException
	 */
	@NonNull 
	public Future<Boolean> deleteDatastore() {
		try {			
			_state.orig_coll.drop();
			return CompletableFuture.completedFuture((Boolean)(true));
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Boolean>returnError(e);
		}				
	}
	
	//////////////////////////////////////////////////////
	
	// Misc
	
	/** Returns an identical version of this CRUD service but using JsonNode instead of beans (which may save serialization)
	 * @return the JsonNode-genericized version of this same CRUD service
	 */
	@NonNull
	public ICrudService<JsonNode> getRawCrudService() {
		return new MongoDbCrudService<JsonNode, K>(JsonNode.class, _state.key_clazz, _state.orig_coll, _state.auth_fieldname, _state.auth, _state.project);
	}
	
	/** Returns a simple searchable ("Lucene-like") view of the data
	 * NOT IMPLEMENTED
	 * @return a search service
	 */
	@Override
	@NonNull
	public final Optional<IBasicSearchService<O>> getSearchService() {
		// TODO (ALEPH-22): Handle search service via either $text or elasticsearch
		return Optional.empty();
	}

	/** USE WITH CARE: this returns the driver to the underlying technology
	 *  shouldn't be used unless absolutely necessary!
	 * @param driver_class the class of the driver
	 * @param a string containing options in some technology-specific format
	 * @return a driver to the underlying technology. Will exception if you pick the wrong one!
	 */
	@SuppressWarnings("unchecked")
	@Override
	@NonNull
	public <T> T getUnderlyingPlatformDriver(Class<T> driver_class, final Optional<String> driver_options) {
		if (JacksonDBCollection.class == driver_class) return (T) _state.coll;
		else if (DBCollection.class == driver_class) return (T) _state.orig_coll;
		else return null;
	}

}
