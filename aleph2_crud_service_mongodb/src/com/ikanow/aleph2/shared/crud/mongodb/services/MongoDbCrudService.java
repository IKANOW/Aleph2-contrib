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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.mongodb.MongoDbDataContext;
import org.apache.metamodel.schema.Table;
import org.bson.types.ObjectId;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.mongojack.DBCursor;
import org.mongojack.JacksonDBCollection;
import org.mongojack.WriteResult;
import org.mongojack.internal.MongoJackModule;
import org.mongojack.internal.object.BsonObjectGenerator;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.shared.crud.mongodb.utils.ErrorUtils;
import com.ikanow.aleph2.shared.crud.mongodb.utils.MongoDbUtils;
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

	//TODO (ALEPH-22): handle auth and project overlay
	
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
		
		_object_mapper = MongoJackModule.configure(BeanTemplateUtils.configureMapper(Optional.empty()));
	}
	
	/** Immutable state for the service 
	 */
	protected class State {
		State(final Class<O> bean_clazz_, final Class<K> key_clazz_, final DBCollection coll_, 
				final Optional<String> auth_fieldname_, final Optional<AuthorizationBean> auth_, final Optional<ProjectBean> project_) {
			// Setup some Jackson overrides:
			ObjectMapper my_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
			MongoJackModule.configure(my_mapper);
			
			bean_clazz = bean_clazz_;
			key_clazz = key_clazz_;
			
			if (key_clazz == String.class) {
				insert_string_id_if_missing = true;
			}
			else {
				insert_string_id_if_missing = false;
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
		final boolean insert_string_id_if_missing;
	}
	protected final State _state;
	protected final ObjectMapper _object_mapper;
	
	/** creates a new object and inserts an _id field if needed
	 * @param bean the object to convert
	 * @return the converted BSON (possibly with _id inserted)
	 */
	@NonNull
	protected DBObject convertToBson(final @NonNull O bean) {
		final DBObject dbo = Patterns.match().<DBObject>andReturn()
				.when(() -> JsonNode.class != _state.bean_clazz, () -> _state.coll.convertToDbObject(bean))
				.otherwise(() -> {
					try (BsonObjectGenerator generator = new BsonObjectGenerator()) {
						_object_mapper.writeTree(generator, (JsonNode)bean);
						return generator.getDBObject();
					} catch (Exception e) {
						return new BasicDBObject();
					} 								
				});
		if (_state.insert_string_id_if_missing) {
			if (!dbo.containsField(_ID)) dbo.put(_ID, new ObjectId().toString());
		}
		return dbo;
	}
	
	/** Generates a future that will error as soon as it's touched
	 * @param e - the underlying exception
	 * @return a future that errors when touched
	 */
	@NonNull
	protected static <T> CompletableFuture<T> returnError(final @NonNull Exception e) {
		return new CompletableFuture<T>() {
			@Override public T get() throws ExecutionException {
				throw new ExecutionException(e);
			}
		};		
	}
	
	/** Creates an empty query that handles the JsonNode case
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@NonNull
	private static <O> SingleQueryComponent<O> emptyQuery(final @NonNull Class<O> bean_clazz) {
		if (JsonNode.class == bean_clazz) {
			return (SingleQueryComponent<O>) CrudUtils.allOf();
		}
		else {
			return CrudUtils.allOf(bean_clazz);
		}
	}
	
	//////////////////////////////////////////////////////

	// Authorization and project filtering:
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getFilteredRepo(java.lang.String, java.util.Optional, java.util.Optional)
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
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object, boolean)
	 */
	@Override
	@NonNull
	public CompletableFuture<Supplier<Object>> storeObject(final @NonNull O new_object, final boolean replace_if_present) {
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

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object)
	 */
	@Override
	@NonNull
	public CompletableFuture<Supplier<Object>> storeObject(final @NonNull O new_object) {
		return storeObject(new_object, false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List, boolean)
	 */
	@NonNull 
	public CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(final @NonNull List<O> new_objects, final boolean continue_on_error) {
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
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List)
	 */
	@NonNull 
	public CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(final @NonNull List<O> new_objects) {
		return storeObjects(new_objects, false);
	}
	
	//////////////////////////////////////////////////////
	
	// *R*ETRIEVE
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#optimizeQuery(java.util.List)
	 */
	@Override
	@NonNull
	public CompletableFuture<Boolean> optimizeQuery(final @NonNull List<String> ordered_field_list) {
		
		return CompletableFuture.supplyAsync(() -> {
			final BasicDBObject index_keys = new BasicDBObject(
					ordered_field_list.stream().collect(
							Collectors.toMap(f -> f, f -> 1, (v1, v2) -> 1, LinkedHashMap::new))
					);
			
			_state.orig_coll.createIndex(index_keys, new BasicDBObject("background", true));
			
			return true;
		});
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
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
					throw new MongoException(ErrorUtils.get(ErrorUtils.MISSING_MONGODB_INDEX_KEY, index_keys_str));
				}				
			}			
			return true;
		}
		catch (MongoException ex) {
			return false;
		}
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	@NonNull
	public CompletableFuture<Optional<O>> getObjectBySpec(final @NonNull QueryComponent<O> unique_spec) {
		return getObjectBySpec(unique_spec, Collections.<String>emptyList(), false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@Override
	@NonNull 
	public CompletableFuture<Optional<O>> getObjectBySpec(final @NonNull QueryComponent<O> unique_spec, final @NonNull List<String> field_list, final boolean include) {
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
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object)
	 */
	@Override
	@NonNull
	public CompletableFuture<Optional<O>> getObjectById(final @NonNull Object id) {
		return getObjectById(id, Collections.<String>emptyList(), false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object, java.util.List, boolean)
	 */
	@SuppressWarnings("unchecked")
	@Override
	@NonNull
	public CompletableFuture<Optional<O>> getObjectById(final @NonNull Object id, final @NonNull List<String> field_list, final boolean include) {
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

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	@NonNull
	public CompletableFuture<Cursor<O>> getObjectsBySpec(final @NonNull QueryComponent<O> spec) {
		return getObjectsBySpec(spec, Collections.<String>emptyList(), false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@Override
	@NonNull
	public CompletableFuture<Cursor<O>> getObjectsBySpec(final @NonNull QueryComponent<O> spec, final @NonNull List<String> field_list, final boolean include) {
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

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	@NonNull
	public CompletableFuture<Long> countObjectsBySpec(final @NonNull QueryComponent<O> spec) {
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

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjects()
	 */
	@NonNull 
	public CompletableFuture<Long> countObjects() {
		return countObjectsBySpec(emptyQuery(_state.bean_clazz));
	}
	
	
	//////////////////////////////////////////////////////
	
	// *U*PDATE
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectById(java.lang.Object, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	@Override
	@NonNull
	public CompletableFuture<Boolean> updateObjectById(final @NonNull Object id, final @NonNull UpdateComponent<O> update) {
		return updateObjectBySpec(emptyQuery(_state.bean_clazz).when("_id", id), Optional.of(false), update);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	@Override
	@NonNull
	public CompletableFuture<Boolean> updateObjectBySpec(final @NonNull QueryComponent<O> unique_spec, final Optional<Boolean> upsert, final @NonNull UpdateComponent<O> update)
	{
		try {			
			final Tuple2<DBObject, DBObject> query_and_meta = MongoDbUtils.convertToMongoQuery(unique_spec);
			final DBObject update_object = MongoDbUtils.createUpdateObject(update);
			
			final WriteResult<O, K> wr = _state.coll.update(query_and_meta._1(), update_object, false, upsert.orElse(false));
			
			return CompletableFuture.completedFuture(wr.getN() > 0);
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Boolean>returnError(e);
		}		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	@Override
	@NonNull
	public CompletableFuture<Long> updateObjectsBySpec(final @NonNull QueryComponent<O> spec, final Optional<Boolean> upsert, final @NonNull UpdateComponent<O> update)
	{
		try {			
			final Tuple2<DBObject, DBObject> query_and_meta = MongoDbUtils.convertToMongoQuery(spec);
			final DBObject update_object = MongoDbUtils.createUpdateObject(update);
			
			final WriteResult<O, K> wr = _state.coll.update(query_and_meta._1(), update_object, true, false);
			
			return CompletableFuture.completedFuture((Long)(long)wr.getN());
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Long>returnError(e);
		}		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, java.util.Optional, java.util.Optional, java.util.Optional, java.util.Optional, java.util.List, boolean)
	 */
	@Override
	@NonNull
	public CompletableFuture<Optional<O>> updateAndReturnObjectBySpec(
								@NonNull QueryComponent<O> unique_spec, final Optional<Boolean> upsert, 
								final @NonNull UpdateComponent<O> update,
								final Optional<Boolean> before_updated, final @NonNull List<String> field_list, final boolean include)
	{
		try {			
			final Tuple2<DBObject, DBObject> query_and_meta = MongoDbUtils.convertToMongoQuery(unique_spec);
			final DBObject update_object = MongoDbUtils.createUpdateObject(update);
			
			final BasicDBObject fields = new BasicDBObject(field_list.stream().collect(Collectors.toMap(f -> f, f -> include ? 1 : 0)));
			
			// ($unset: null removes the object, only possible via the UpdateComponent.deleteObject call) 
			final boolean do_remove = update_object.containsField("$unset") && (null == update_object.get("$unset"));
			
			final O ret_val = _state.coll.findAndModify(query_and_meta._1(), fields, (DBObject)query_and_meta._2().get("$sort"), do_remove, update_object, !before_updated.orElse(false), upsert.orElse(false));
			
			return CompletableFuture.completedFuture(Optional.ofNullable(ret_val));
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Optional<O>>returnError(e);
		}		
	}	
	
	//////////////////////////////////////////////////////
	
	// *D*ELETE
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectById(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	@NonNull
	public CompletableFuture<Boolean> deleteObjectById(final @NonNull Object id) {
		try {			
			final WriteResult<O, K> wr = _state.coll.removeById((K)id);
			return CompletableFuture.completedFuture(wr.getN() > 0);
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Boolean>returnError(e);
		}		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	@NonNull
	public CompletableFuture<Boolean> deleteObjectBySpec(final @NonNull QueryComponent<O> unique_spec) {
		try {			
			// Delete and return the object
			final CompletableFuture<Optional<O>> ret_val =
					updateAndReturnObjectBySpec(unique_spec, Optional.of(false), CrudUtils.update(_state.bean_clazz).deleteObject(),
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

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	@NonNull
	public CompletableFuture<Long> deleteObjectsBySpec(final @NonNull QueryComponent<O> spec) {
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
				
				return deleteObjectsBySpec(emptyQuery(_state.bean_clazz).withAny("_id", ids));
			}			
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Long>returnError(e);
		}		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteDatastore()
	 */
	@NonNull 
	public CompletableFuture<Boolean> deleteDatastore() {
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
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getRawCrudService()
	 */
	@NonNull
	public ICrudService<JsonNode> getRawCrudService() {
		return new MongoDbCrudService<JsonNode, K>(JsonNode.class, _state.key_clazz, _state.orig_coll, _state.auth_fieldname, _state.auth, _state.project); 
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
	 */
	@Override
	@NonNull
	public final Optional<IBasicSearchService<O>> getSearchService() {
		// TODO (ALEPH-22): Handle search service via either $text or elasticsearch
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@Override
	@NonNull
	public <T> T getUnderlyingPlatformDriver(Class<T> driver_class, final Optional<String> driver_options) {
		if (JacksonDBCollection.class == driver_class) return (T) _state.coll;
		else if (DBCollection.class == driver_class) return (T) _state.orig_coll;
		else if (IMetaModel.class == driver_class) return (T) ((null == _meta_model) 
														? (_meta_model = new MongoDbMetaModel(_state.orig_coll)) : _meta_model);
		else return null;
	}

	
	/** A table-level interface to the CRUD store using the open MetaModel library
	 * MongoDB implementation
	 * @author acp
	 */
	public static class MongoDbMetaModel implements IMetaModel {
		protected MongoDbMetaModel(DBCollection coll) {
			_context = new MongoDbDataContext(coll.getDB());
			_table = _context.getTableByQualifiedLabel(coll.getFullName());
		}
		public final DataContext _context; 
		public final Table _table; 
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.IMetaModel#getContext()
		 */
		@NonNull 
		public DataContext getContext() {
			return _context;
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.IMetaModel#getTable()
		 */
		@NonNull 
		public Table getTable() {
			return _table;
		}
	}
	protected MongoDbMetaModel _meta_model = null;
}
