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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.mongojack.internal.MongoJackModule;
import org.mongojack.internal.object.BsonObjectGenerator;
import org.mongojack.internal.object.BsonObjectTraversingParser;

import scala.Tuple2;

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.shared.crud.mongodb.utils.MongoDbUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.FongoDBCollection;
import com.mongodb.InsertOptions;
import com.mongodb.WriteConcern;

public class MongoDbCrudService_Json<O, K> extends MongoDbCrudService<JsonNode, K> {

	protected final MongoDbCrudService<O, K> _parent;
	protected final Class<O> _parent_bean_clazz;
	protected final ObjectMapper _object_mapper;

	/** For convenience, maps the JsonNode back to an object so we can use it in the original code (low performance code only) 
	 * @param j
	 * @return
	 * @throws JsonProcessingException 
	 */
	@SuppressWarnings("unchecked")
	@NonNull
	protected O mapJsonToBean(final @NonNull JsonNode j) throws JsonProcessingException {
		return _object_mapper.treeToValue(j, (Class<O>)_parent_bean_clazz.getClass());
	}
	
	/** Maps the bean to a JsonNode since that's what the client is expecting (low performance code only)
	 * @param bean
	 * @return
	 */
	@NonNull 
	protected JsonNode mapBeanToJson(final @NonNull O bean) {
		return _object_mapper.valueToTree(bean);
	}
	
	/** For higher performance code - lazily wraps a list of JsonNodes with a DBObject
	 * @param bean_list
	 * @return
	 */
	@NonNull 
	protected List<DBObject> wrapJsonListAsDbObjects(final @NonNull List<JsonNode> bean_list) {
		return Lists.transform(bean_list, new Function<JsonNode, DBObject>() {
			@Override
			public DBObject apply(JsonNode j) {
				try (BsonObjectGenerator generator = new BsonObjectGenerator()) {
					_object_mapper.writeTree(generator, j);
					return generator.getDBObject();
				} catch (Exception e) {
					return new BasicDBObject();
				} 
			}
			
		});
	}
	
	/** A wrapper for a DBCursor that is auto-closeable
	 * @param <O>
	 */
	public class MongoDbToJsonNodeCursor extends Cursor<JsonNode> {
		/**
		 * @param cursor
		 */
		protected MongoDbToJsonNodeCursor(final @NonNull DBCursor cursor) {
			_cursor = cursor;
		}
		protected final DBCursor _cursor;
		
		/* (non-Javadoc)
		 * @see java.lang.AutoCloseable#close()
		 */
		@Override
		public void close() throws Exception {
			_cursor.close();
			
		}

		/* (non-Javadoc)
		 * @see java.lang.Iterable#iterator()
		 */
		@Override
		@NonNull
		public Iterator<JsonNode> iterator() {
			return new MongoDbToJsonNodeIterator(_cursor.iterator());
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor#count()
		 */
		@Override
		@NonNull
		public long count() {
			return _cursor.count();
		}		
	}
	/**A wrapper for a DBCursor iterator 
	 * @author acp
	 *
	 */
	public class MongoDbToJsonNodeIterator implements Iterator<JsonNode> {

		protected final Iterator<DBObject> _cursor_iterator;
		
		/** Creates a wrapping iterator DBObject -> JsonNode
		 * @param cursor_iterator
		 */
		protected MongoDbToJsonNodeIterator(Iterator<DBObject> cursor_iterator) {
			_cursor_iterator = cursor_iterator;
		}
		
		/* (non-Javadoc)
		 * @see java.util.Iterator#hasNext()
		 */
		@Override
		public boolean hasNext() {
			return _cursor_iterator.hasNext();
		}

		/* (non-Javadoc)
		 * @see java.util.Iterator#next()
		 */
		@Override
		public JsonNode next() {
			final DBObject dbo = _cursor_iterator.next();
			try (BsonObjectTraversingParser parser = new BsonObjectTraversingParser(_state.coll, dbo, _object_mapper)) {
				return _object_mapper.readTree(parser);
			} catch (Exception e) {
				return _object_mapper.createObjectNode();
			}
		}		
	}
	
	
	/** (Low performance code) wraps an optional single bean future in one that translates to a JsonNode
	 * @param bean_version
	 * @return
	 */
	@NonNull 
	protected CompletableFuture<Optional<JsonNode>> wrapSingleBeanFuture(CompletableFuture<Optional<O>> bean_version) {
		return new CompletableFuture<Optional<JsonNode>>() {
			@Override public Optional<JsonNode> get() throws InterruptedException, ExecutionException {
				final Optional<O> bean_optional = bean_version.get();
				if (bean_optional.isPresent()) {
					return Optional.of(mapBeanToJson(bean_optional.get()));
				}
				else {
					return Optional.empty();
				}
			}
		};
		
	}
	
	/** Construct a copy of an existing CRUD service, but using JsonNode for performance
	 *  (in practice only the "high performance" calls will be translated)
	 * @param bean_clazz - the class to which this CRUD service is being mapped
	 * @param key_clazz - if you know the type of the _id then add this here, else use Object.class (or ObjectId to use MongoDB defaults)
	 * @param coll - must provide the MongoDB collection
	 * @param auth_fieldname - optionally, the fieldname to which auth/project beans are applied
	 * @param auth - optionally, an authorization overlay added to each query
	 * @param project - optionally, a project overlay added to each query
	 * @param parent - the raw parent repo
	 */
	protected MongoDbCrudService_Json(@NonNull Class<O> bean_clazz,
			@NonNull Class<K> key_clazz, @NonNull DBCollection coll,
			Optional<String> auth_fieldname, Optional<AuthorizationBean> auth,
			Optional<ProjectBean> project,
			MongoDbCrudService<O, K> parent)
	{
		super(JsonNode.class, key_clazz, coll, auth_fieldname, auth, project);
		
		_parent_bean_clazz = bean_clazz;
		_parent = parent;
		
		_object_mapper = MongoJackModule.configure(new ObjectMapper());
		_object_mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#getFilteredRepo(java.lang.String, java.util.Optional, java.util.Optional)
	 */
	public @NonNull ICrudService<JsonNode> getFilteredRepo(
			@NonNull String authorization_fieldname,
			Optional<AuthorizationBean> client_auth,
			Optional<ProjectBean> project_auth)
	{
		return new MongoDbCrudService_Json<O, K>(_parent_bean_clazz, _state.key_clazz, _state.orig_coll, Optional.of(authorization_fieldname), client_auth, project_auth, _parent);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#storeObject(java.lang.Object, boolean)
	 */
	public @NonNull CompletableFuture<Supplier<Object>> storeObject(
			@NonNull JsonNode new_object, boolean replace_if_present) {
		try {
			return _parent.storeObject(mapJsonToBean(new_object), replace_if_present);
		}
		catch (Exception e) {
			return MongoDbCrudService.returnError(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#storeObject(java.lang.Object)
	 */
	public @NonNull CompletableFuture<Supplier<Object>> storeObject(
			@NonNull JsonNode new_object) {
		try {
			return _parent.storeObject(mapJsonToBean(new_object));
		}
		catch (Exception e) {
			return MongoDbCrudService.returnError(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#storeObjects(java.util.List, boolean)
	 */
	public @NonNull CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			@NonNull List<JsonNode> new_objects, boolean continue_on_error)			
	{
		// Here need a c/p from the original:
		try {
			final List<DBObject> l = this.wrapJsonListAsDbObjects(new_objects);

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
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#storeObjects(java.util.List)
	 */
	public @NonNull CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			@NonNull List<JsonNode> new_objects) {
		return this.storeObjects(new_objects, false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#optimizeQuery(java.util.List)
	 */
	public @NonNull CompletableFuture<Boolean> optimizeQuery(
			@NonNull List<String> ordered_field_list) {
		return _parent.optimizeQuery(ordered_field_list);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public @NonNull CompletableFuture<Optional<JsonNode>> getObjectBySpec(
			@NonNull QueryComponent<JsonNode> unique_spec) {
		return this.getObjectBySpec(unique_spec, Collections.<String>emptyList(), false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	public @NonNull CompletableFuture<Optional<JsonNode>> getObjectBySpec(
			@NonNull QueryComponent<JsonNode> unique_spec,
			@NonNull List<String> field_list, boolean include)
	{
		final CompletableFuture<Optional<O>> bean_version = _parent.getObjectBySpec(CrudUtils.from_json(unique_spec), field_list, include);
		return wrapSingleBeanFuture(bean_version);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#getObjectById(java.lang.Object)
	 */
	public @NonNull CompletableFuture<Optional<JsonNode>> getObjectById(
			@NonNull Object id) {
		return this.getObjectById(id, Collections.<String>emptyList(), false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#getObjectById(java.lang.Object, java.util.List, boolean)
	 */
	public @NonNull CompletableFuture<Optional<JsonNode>> getObjectById(
			@NonNull Object id, @NonNull List<String> field_list,
			boolean include) {
		final CompletableFuture<Optional<O>> bean_version = _parent.getObjectById(id, field_list, include);
		return wrapSingleBeanFuture(bean_version);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public @NonNull CompletableFuture<Cursor<JsonNode>> getObjectsBySpec(
			@NonNull QueryComponent<JsonNode> spec) {
		return this.getObjectsBySpec(spec, Collections.<String>emptyList(), false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	public @NonNull CompletableFuture<Cursor<JsonNode>> getObjectsBySpec(
			@NonNull QueryComponent<JsonNode> spec, @NonNull List<String> field_list,
			boolean include) {
		//c/p from bean version
		try {	
			final Tuple2<DBObject, DBObject> query_and_meta = MongoDbUtils.convertToMongoQuery(spec);
			final DBCursor cursor = 
					Optional.of(Patterns.match(query_and_meta)
						.<DBCursor>andReturn()
						.when(qm -> field_list.isEmpty(), qm -> _state.orig_coll.find(qm._1()))
						.otherwise(qm -> {
							final BasicDBObject fs = new BasicDBObject(field_list.stream().collect(Collectors.toMap(f -> f, f -> include ? 1 : 0)));
							return _state.orig_coll.find(qm._1(), fs);
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
			
			return CompletableFuture.completedFuture(new MongoDbToJsonNodeCursor(cursor));
		}
		catch (Exception e) {			
			return MongoDbCrudService.<Cursor<JsonNode>>returnError(e);
		}		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public @NonNull CompletableFuture<Long> countObjectsBySpec(
			@NonNull QueryComponent<JsonNode> spec) {
		return _parent.countObjectsBySpec(CrudUtils.from_json(spec));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#countObjects()
	 */
	public @NonNull CompletableFuture<Long> countObjects() {
		return _parent.countObjects();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#updateObjectById(java.lang.Object, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	public @NonNull CompletableFuture<Boolean> updateObjectById(
			@NonNull Object id, @NonNull UpdateComponent<JsonNode> update) {
		return _parent.updateObjectById(id, CrudUtils.from_json(update));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	public @NonNull CompletableFuture<Boolean> updateObjectBySpec(
			@NonNull QueryComponent<JsonNode> unique_spec, Optional<Boolean> upsert,
			@NonNull UpdateComponent<JsonNode> update) {
		return _parent.updateObjectBySpec(CrudUtils.from_json(unique_spec), upsert, CrudUtils.from_json(update));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	public @NonNull CompletableFuture<Long> updateObjectsBySpec(
			@NonNull QueryComponent<JsonNode> spec, Optional<Boolean> upsert,
			@NonNull UpdateComponent<JsonNode> update) {
		return _parent.updateObjectsBySpec(CrudUtils.from_json(spec), upsert, CrudUtils.from_json(update));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent, java.util.Optional, java.util.List, boolean)
	 */
	public @NonNull CompletableFuture<Optional<JsonNode>> updateAndReturnObjectBySpec(
			@NonNull QueryComponent<JsonNode> unique_spec, Optional<Boolean> upsert,
			@NonNull UpdateComponent<JsonNode> update,
			Optional<Boolean> before_updated, @NonNull List<String> field_list,
			boolean include)
	{
		final CompletableFuture<Optional<O>> bean_version = _parent.updateAndReturnObjectBySpec(CrudUtils.from_json(unique_spec), upsert, CrudUtils.from_json(update), before_updated, field_list, include);
		return wrapSingleBeanFuture(bean_version);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#deleteObjectById(java.lang.Object)
	 */
	public @NonNull CompletableFuture<Boolean> deleteObjectById(
			@NonNull Object id) {
		return _parent.deleteObjectById(id);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#deleteObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public @NonNull CompletableFuture<Boolean> deleteObjectBySpec(
			@NonNull QueryComponent<JsonNode> unique_spec) {
		return _parent.deleteObjectBySpec(CrudUtils.from_json(unique_spec));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.mongodb.services.MongoDbCrudService#deleteObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	public @NonNull CompletableFuture<Long> deleteObjectsBySpec(
			@NonNull QueryComponent<JsonNode> spec) {
		return _parent.deleteObjectsBySpec(CrudUtils.from_json(spec));
	}

	public @NonNull CompletableFuture<Boolean> deleteDatastore() {
		return _parent.deleteDatastore();
	}

	public @NonNull ICrudService<JsonNode> getRawCrudService() {
		return this;
	}

	public <T> @NonNull T getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options)
	{
		return _parent.getUnderlyingPlatformDriver(driver_class, driver_options);
	}
}
