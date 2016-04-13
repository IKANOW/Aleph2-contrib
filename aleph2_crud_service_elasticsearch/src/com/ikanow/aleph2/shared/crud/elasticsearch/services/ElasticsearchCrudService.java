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
package com.ikanow.aleph2.shared.crud.elasticsearch.services;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.elasticsearch.ElasticSearchDataContext;
import org.apache.metamodel.schema.Table;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.JsonUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext.ReadWriteContext;
import com.ikanow.aleph2.shared.crud.elasticsearch.utils.ElasticsearchContextUtils;
import com.ikanow.aleph2.shared.crud.elasticsearch.utils.ElasticsearchFutureUtils;
import com.ikanow.aleph2.shared.crud.elasticsearch.utils.ElasticsearchUtils;
import com.ikanow.aleph2.shared.crud.elasticsearch.utils.ErrorUtils;

import fj.data.Either;

//TODO (ALEPH-14) .... more thoughts on field list buckets ... options for auto generating .number fields and .raw fields (and nested - that might live in the search index bit though?)

//TODO: currently the _id doesn't get put into the _source, is this what i want? (I don't think so)

public class ElasticsearchCrudService<O> implements ICrudService<O> {

	public enum CreationPolicy { AVAILABLE_IMMEDIATELY, SINGLE_OBJECT_AVAILABLE_IMMEDIATELY, OPTIMIZED };
	
	public ElasticsearchCrudService(final Class<O> bean_clazz, 
			final ElasticsearchContext es_context, 
			final Optional<Boolean> id_ranges_ok, final CreationPolicy creation_policy, 
			final Optional<String> auth_fieldname, final Optional<AuthorizationBean> auth, final Optional<ProjectBean> project,
			final Optional<DataSchemaBean.WriteSettings> batch_write_settings)
	{
		_state = new State(bean_clazz, es_context, id_ranges_ok.orElse(false), creation_policy, auth_fieldname, auth, project);
		_object_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		_batch_write_settings = batch_write_settings;
	}
	protected class State {
		State(final Class<O> bean_clazz, final ElasticsearchContext es_context, 
				final boolean id_ranges_ok, final CreationPolicy creation_policy,
				final Optional<String> auth_fieldname, final Optional<AuthorizationBean> auth, final Optional<ProjectBean> project
				)			
		{
			this.es_context = es_context;
			client = es_context.client();
			clazz = bean_clazz;
			this.id_ranges_ok = id_ranges_ok;
			this.creation_policy = creation_policy;
			
			this.auth = auth;
			this.auth_fieldname = auth_fieldname;
			this.project = project;
		}
		final ElasticsearchContext es_context;
		final Client client;
		final Class<O> clazz;
		final boolean id_ranges_ok;
		final CreationPolicy creation_policy;
		
		final Optional<String> auth_fieldname;
		final Optional<AuthorizationBean> auth;
		final Optional<ProjectBean> project;		
	}
	protected final State _state;
	protected final ObjectMapper _object_mapper;
	protected final Optional<DataSchemaBean.WriteSettings> _batch_write_settings;
	
	/** A wrapper for an ES return value that is auto-closeable
	 * @author acp
	 *
	 * @param <O>
	 */
	public class ElasticsearchCursor extends Cursor<O> {
				
		//TODO (ALEPH-14): if there's no limit then should convert this to a scroll query...
		
		protected ElasticsearchCursor(final SearchResponse sr) {
			_hits = sr == null ? null : sr.getHits();
		}
		protected final SearchHits _hits;
		
		@Override
		public void close() throws Exception {
		}

		@Override
		public Iterator<O> iterator() {
			return null == _hits
					? Collections.emptyIterator()
					: Arrays.stream(_hits.hits())
						.<O>map(hit -> {
							final Map<String, Object> src_fields = hit.getSource();
							src_fields.computeIfAbsent(JsonUtils._ID, __ -> hit.getId());
							return _object_mapper.convertValue(src_fields, _state.clazz);
						})
						.iterator();
		}

		@Override
		public long count() {
			return Optional.ofNullable(_hits).map(SearchHits::totalHits).orElse(0L);
		}		
	}
	
	/////////////////////////////////////////////////////
	
	// UTILS
	
	/** Utility function for adding a set of objects to a single index
	 * @param rw_context - either the index/type context, or just (index,type) for retries 
	 * @param new_object - either the object to insert/save, or (id, string source) (must be the object(left) if the index/type context (ie left) is used for "rw_context")
	 * @param replace_if_present - replace the existing object (else error)
	 * @param bulk - whether being called as part of a bulk operation
	 * @return
	 */
	private IndexRequestBuilder singleObjectIndexRequest(final Either<ReadWriteContext, Tuple2<String, String>> rw_context, 
			final Either<O, Tuple2<String, String>> new_object, final boolean replace_if_present, final boolean bulk)
	{
		final Either<JsonNode, Tuple2<String, String>> json_object =
				new_object.left().map(left-> {
					return ((JsonNode.class.isAssignableFrom(_state.clazz))
							? (JsonNode) left
							: BeanTemplateUtils.toJson(left));
				});
		
		return Optional
				.of(rw_context.<IndexRequestBuilder>either(
									left -> _state.client.prepareIndex(
										left.indexContext().getWritableIndex(Optional.of(json_object.left().value())),
										left.typeContext().getWriteType())
									, 
									right ->_state.client.prepareIndex(right._1(), right._2()))
					.setOpType(replace_if_present ? OpType.INDEX : OpType.CREATE)
					.setConsistencyLevel(WriteConsistencyLevel.ONE)
					.setRefresh(!bulk && CreationPolicy.OPTIMIZED != _state.creation_policy)
					.setSource(json_object.<String>either(left -> left.toString(), right -> right._2()))
						)
				.map(i -> json_object.<IndexRequestBuilder>either(left -> left.has(JsonUtils._ID) ? i.setId(left.get(JsonUtils._ID).asText()) : i, right -> i.setId(right._1())))
				//DEBUG
				//.map(irb -> { System.out.println("REQUEST INDICES = " + Arrays.toString(irb.request().indices())); return irb; })
				.get();		
	}
	
	/** Utility function for deleting an object
	 * @param rw_context
	 * @param id
	 * @param obj_to_delete
	 * @param bulk
	 * @return
	 */
	private DeleteRequestBuilder singleObjectDeleteRequest(
			final Either<ReadWriteContext, Tuple2<String, String>> rw_context,
			final String id, final Either<O, Tuple2<String, String>> obj_to_delete, final boolean bulk)
	{
		final Either<JsonNode, Tuple2<String, String>> json_object =
				obj_to_delete.left().map(left-> {
					return ((JsonNode.class.isAssignableFrom(_state.clazz))
							? (JsonNode) left
							: BeanTemplateUtils.toJson(left));
				});
		
		return Optional
				.of(rw_context.<DeleteRequestBuilder>either(
									left -> _state.client.prepareDelete()
										.setIndex(left.indexContext().getWritableIndex(Optional.of(json_object.left().value())))
										.setType(left.typeContext().getWriteType())
									, 
									right ->_state.client.prepareDelete().setIndex(right._1()).setType(right._2())
									)
							.setConsistencyLevel(WriteConsistencyLevel.ONE)
							.setRefresh(!bulk && CreationPolicy.OPTIMIZED != _state.creation_policy)
							.setId(id)
					)
					.get();
	}
	
	private static final String PARSE_ERROR_FRAGMENT = "failed to parse [";
	private static final int PARSE_ERROR_FRAGMENT_LEN = PARSE_ERROR_FRAGMENT.length();
	private static final String PARSE_ERROR_FRAGMENT_2 = "tried to parse field [";
	private static final int PARSE_ERROR_FRAGMENT_LEN_2 = PARSE_ERROR_FRAGMENT_2.length();
	
	/** Utility function to extra the field from a parsing exception
	 * @param error_message
	 * @return
	 */
	private String getFieldFromParsingException(final String error_message) {
		int index1 = error_message.lastIndexOf(PARSE_ERROR_FRAGMENT) + PARSE_ERROR_FRAGMENT_LEN; // (Get 2 chances at this)		
		if (index1 < PARSE_ERROR_FRAGMENT_LEN) { // (=> lastIndexOf returned -1)
			index1 = error_message.lastIndexOf(PARSE_ERROR_FRAGMENT_2) + PARSE_ERROR_FRAGMENT_LEN_2; 
			if (index1 < PARSE_ERROR_FRAGMENT_LEN_2) return null; // (=> lastIndexOf returned -1)
		}
		final int index2 = index1 + error_message.substring(index1).indexOf(']');
		if (index2 < index1) return null; // (=> lastIndexOf returned -1)
		if (index2 - index1 < 1) return null; //(must not be empty)
		return error_message.substring(index1, index2);
	}
	
	/////////////////////////////////////////////////////
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getFilteredRepo(java.lang.String, java.util.Optional, java.util.Optional)
	 */
	@Override
	public ICrudService<O> getFilteredRepo(String authorization_fieldname,
			Optional<AuthorizationBean> client_auth,
			Optional<ProjectBean> project_auth) {
		//TODO (ALEPH-14): TO BE IMPLEMENTED
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "getFilteredRepo"));
	}

	/** Utility function - will get a read-write version of a context and exit via exception if that isn't possible 
	 * @param es_context
	 * @return
	 */
	private static ElasticsearchContext.ReadWriteContext getRwContextOrThrow(final ElasticsearchContext es_context, final String method_name) {
		if (es_context instanceof ElasticsearchContext.ReadWriteContext) {
			return (ReadWriteContext)es_context;
		}
		else {
			throw new RuntimeException(ErrorUtils.get(ErrorUtils.TRIED_TO_WRITE_INTO_RO_SERVICE, method_name));
		}		
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object, boolean)
	 */
	@Override
	public CompletableFuture<Supplier<Object>> storeObject(final O new_object, final boolean replace_if_present) {
		try {
			final ReadWriteContext rw_context = getRwContextOrThrow(_state.es_context, "storeObject");
			
			final IndexRequestBuilder irb = singleObjectIndexRequest(Either.left(rw_context), Either.left(new_object), replace_if_present, false);

			// Execute and handle result
			
			final Function<IndexResponse, Supplier<Object>> success_handler = ir -> {	
				return () -> ir.getId();
			};

			// Recursive, so has some hoops to jump through (lambda can't access itself)
			final BiConsumer<Throwable, CompletableFuture<Supplier<Object>>> error_handler = new BiConsumer<Throwable, CompletableFuture<Supplier<Object>>>() {
				@Override
				public void accept(final Throwable error, final CompletableFuture<Supplier<Object>> future) {
					Patterns.match(error).andAct()						
						.when(org.elasticsearch.index.mapper.MapperParsingException.class, mpe -> {
							final Set<String> fixed_type_fields = rw_context.typeContext().fixed_type_fields();
							if (!fixed_type_fields.isEmpty()) {
								// Obtain the field name from the exception (if we fail then drop the record) 
								final String field = getFieldFromParsingException(mpe.getMessage());
								if ((null == field) || fixed_type_fields.contains(field)) {
									future.completeExceptionally(error);
									return;
								}
							}//(else roll on to...)							
							Patterns.match(rw_context.typeContext())
								.andAct()
								.when(ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext.class, auto_context -> {
									irb.setType(ElasticsearchContextUtils.getNextAutoType(auto_context.getPrefix(), irb.request().type()));
									ElasticsearchFutureUtils.wrap(
											irb.execute(),
											future,
											(ir, next_future) -> {
												next_future.complete(success_handler.apply(ir));
											},
											this);
								})
								.otherwise(() -> future.completeExceptionally(error));														
						})
						.otherwise(() -> future.completeExceptionally(error));
				}				
			};
			
			return ElasticsearchFutureUtils.wrap(irb.execute(), success_handler, error_handler);
		}
		catch (Exception e) {
			return FutureUtils.returnError(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object)
	 */
	@Override
	public CompletableFuture<Supplier<Object>> storeObject(final O new_object) {
		return storeObject(new_object, false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(final List<O> new_objects, final boolean replace_if_present) {
		try {
			final ReadWriteContext rw_context = getRwContextOrThrow(_state.es_context, "storeObjects");
			
			final BulkRequestBuilder brb = new_objects.stream()
											.reduce(
												_state.client.prepareBulk()
													.setConsistencyLevel(WriteConsistencyLevel.ONE)
													.setRefresh(CreationPolicy.AVAILABLE_IMMEDIATELY == _state.creation_policy)
												, 
												(acc, val) -> acc.add(singleObjectIndexRequest(Either.left(rw_context), Either.left(val), replace_if_present, true)),
												(acc1, acc2) -> { throw new RuntimeException("Internal logic error - Parallel not supported"); });
			
			final BiConsumer<BulkResponse, CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>>> action_handler = 
					new BiConsumer<BulkResponse, CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>>>()
			{
				// WARNING: mutable/imperative code ahead...
				long _curr_written = 0;
				List<Object> _id_list = null;
				HashMap<String, String> _mapping_failures = null; 
				
				@Override
				public void accept(final BulkResponse result, final CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> future) {
					
					if (result.hasFailures() &&
							(rw_context.typeContext() instanceof ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext)
							)
					{
						final ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext auto_context = (ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext) rw_context.typeContext();
						// Recursive builder in case I need to build a second batch of docs								
						BulkRequestBuilder brb2 = null;
						
						if (null == _id_list) {
							_id_list = new LinkedList<Object>();
						}
						HashMap<String, String> temp_mapping_failures = null;
						final Iterator<BulkItemResponse> it = result.iterator();
						while (it.hasNext()) {
							final BulkItemResponse bir = it.next();
							if (bir.isFailed()) {								
								if (bir.getFailure().getMessage().startsWith("MapperParsingException")) {
									final Set<String> fixed_type_fields = rw_context.typeContext().fixed_type_fields();
									if (!fixed_type_fields.isEmpty()) {
										// Obtain the field name from the exception (if we fail then drop the record) 
										final String field = getFieldFromParsingException(bir.getFailure().getMessage());
										if ((null == field) || fixed_type_fields.contains(field)) {
											continue;
										}
									}//(else roll on to...)																
									
									// OK this is the case where I might be able to apply auto types:
									if (null == brb2) { 
										brb2 = _state.client.prepareBulk()
													.setConsistencyLevel(WriteConsistencyLevel.ONE)
													.setRefresh(CreationPolicy.AVAILABLE_IMMEDIATELY == _state.creation_policy);
									}
									String failed_json = null;
									if (null == _mapping_failures) { // first time through, use item id to grab the objects from the original request
										if (null == temp_mapping_failures) {
											temp_mapping_failures = new HashMap<String, String>();
										}
										final ActionRequest<?> ar = brb.request().requests().get(bir.getItemId());
										if (ar instanceof IndexRequest) {											
											IndexRequest ir = (IndexRequest) ar;
											failed_json = ir.source().toUtf8();
											temp_mapping_failures.put(bir.getId(), failed_json);
										}
									}
									else { // have already grabbed all the failure _ids and stuck in a map
										failed_json = _mapping_failures.get(bir.getId());
									}									
									if (null != failed_json) {
										brb2.add(singleObjectIndexRequest(
													Either.right(Tuples._2T(bir.getIndex(), 
															ElasticsearchContextUtils.getNextAutoType(auto_context.getPrefix(), bir.getType()))), 
													Either.right(Tuples._2T(bir.getId(), failed_json)), 
													false, true));  
									}
								}
								// Ugh otherwise just silently fail I guess? 
								//(should I also look for transient errors and resubmit them after a pause?!)
							}
							else { // (this item worked)
								_id_list.add(bir.getId());
								_curr_written++;
							}							
						}
						if (null != brb2) { // found mapping errors to retry with
							if (null == _mapping_failures) // (first level of recursion)
								_mapping_failures = temp_mapping_failures;
							
							// (note that if brb2.request().requests().isEmpty() this is an internal logic error, so it's OK to throw)
							ElasticsearchFutureUtils.wrap(brb2.execute(), future, this, (error, future2) -> {
													future2.completeExceptionally(error);
													});
						}
						else { // relative success, plus we've built the list anyway
							future.complete(Tuples._2T(() -> _id_list, () -> (Long)_curr_written));
						}
					}
					else { // No errors with this iteration of the bulk request			
						_curr_written += result.getItems().length;

						if (null == _id_list) { // This is the first bulk request, no recursion on failures, so can lazily create the list in case it isn't needed
							final Supplier<List<Object>> get_objects = () -> {
								return StreamSupport.stream(result.spliterator(), false)
											.filter(bir -> !bir.isFailed())
											.map(bir -> bir.getId()).collect(Collectors.toList());
							};
							final Supplier<Long> get_count_workaround = () -> {
								return StreamSupport.stream(result.spliterator(), false)
												.filter(bir -> !bir.isFailed())
												.collect(Collectors.counting());
							};
							get_count_workaround.get();
							future.complete(Tuples._2T(get_objects, get_count_workaround));
						}
						else { // have already calculated everything so just return it							
							future.complete(Tuples._2T(() -> _id_list, () -> (Long)_curr_written));
						}
					}
				}				
			};
			
			return ElasticsearchFutureUtils.wrap(brb.execute(), 
												new CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>>(),
												action_handler,
												(error, future) -> {
													future.completeExceptionally(error);
												});
		}
		catch (Exception e) {
			return FutureUtils.returnError(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List)
	 */
	@Override
	public CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(final List<O> new_objects) {
		return storeObjects(new_objects, false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#optimizeQuery(java.util.List)
	 */
	@Override
	public CompletableFuture<Boolean> optimizeQuery(final List<String> ordered_field_list) {
		// (potentially in the future this could check the mapping and throw if the fields are not indexed?)
		return CompletableFuture.completedFuture(true);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
	 */
	@Override
	public boolean deregisterOptimizedQuery(final List<String> ordered_field_list) {
		//(just ignore this)
		return false;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Optional<O>> getObjectBySpec(final QueryComponent<O> unique_spec) {
		return getObjectBySpec(unique_spec, Arrays.asList(), false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Optional<O>> getObjectBySpec(final QueryComponent<O> unique_spec, final List<String> field_list, final boolean include) {
		try {
			//TODO (ALEPH-14): Handle case where no source is present but fields are
			
			Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query = ElasticsearchUtils.convertToElasticsearchFilter(unique_spec, _state.id_ranges_ok);
			
			final SearchRequestBuilder srb = Optional
						.of(
							_state.client.prepareSearch()
							.setIndices(_state.es_context.indexContext().getReadableIndexArray(Optional.empty()))
							.setTypes(_state.es_context.typeContext().getReadableTypeArray())
							.setQuery(QueryBuilders.constantScoreQuery(query._1()))
							.setSize(1))
						.map(s -> field_list.isEmpty() 
								? s 
								: include
									? s.setFetchSource(field_list.toArray(new String[0]), new String[0])
									: s.setFetchSource(new String[0], field_list.toArray(new String[0]))
							)
						.get();
			
			return ElasticsearchFutureUtils.wrap(srb.execute(), sr -> {
				final SearchHit[] sh = sr.getHits().hits();
				
				if (sh.length > 0) {
					final Map<String, Object> src_fields = sh[0].getSource();
					src_fields.computeIfAbsent(JsonUtils._ID, __ -> sh[0].getId());
					return Optional.ofNullable(_object_mapper.convertValue(src_fields, _state.clazz));
				}
				else {
					return Optional.empty();
				}
			},
			(err, future) -> {
				if ((err instanceof IndexMissingException) || (err instanceof SearchPhaseExecutionException)) //(this one can come up as on a read on a newly created index)
				{ 
					// just treat this like an "object not found"
					future.complete(Optional.empty());
				}
				else {
					future.completeExceptionally(err);
				}
			}
			);
		}
		catch (Exception e) {
			return FutureUtils.returnError(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object)
	 */
	@Override
	public CompletableFuture<Optional<O>> getObjectById(final Object id) {
		return getObjectById(id, Arrays.asList(), false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Optional<O>> getObjectById(final Object id, final List<String> field_list, final boolean include) {
		try {
			final List<String> indexes = _state.es_context.indexContext().getReadableIndexList(Optional.empty());
			final List<String> types = _state.es_context.typeContext().getReadableTypeList();
			if ((indexes.size() != 1) || (indexes.size() > 1)) {
				// Multi index request, so use a query (which may not always return the most recent value, depending on index refresh settings/timings)
				return getObjectBySpec(CrudUtils.anyOf(_state.clazz).when(JsonUtils._ID, id.toString()), field_list, include);			
			}
			else {
				
				final GetRequestBuilder srb = Optional
						.of(
							_state.client.prepareGet()
								.setIndex(indexes.get(0))
								.setId(id.toString())
							)
						.map(s -> (1 == types.size()) ? s.setType(types.get(0)) : s)
						.map(s -> field_list.isEmpty() 
								? s 
								: include
									? s.setFetchSource(field_list.toArray(new String[0]), new String[0])
									: s.setFetchSource(new String[0], field_list.toArray(new String[0]))
							)
						.get();
				
				return ElasticsearchFutureUtils.wrap(srb.execute(), sr -> {
					if (sr.isExists()) {
						final Map<String, Object> src_fields = sr.getSource();					
						return Optional.ofNullable(_object_mapper.convertValue(src_fields, _state.clazz));
					}
					else {
						return Optional.empty();
					}
				},
				(err, future) -> {
					if ((err instanceof IndexMissingException) || (err instanceof SearchPhaseExecutionException)) //(this one can come up as on a read on a newly created index)
					{ 
						// just treat this like an "object not found"
						future.complete(Optional.empty());
					}
					else {
						future.completeExceptionally(err);
					}
				}
				);			
			}
		}
		catch (Exception e) {
			return FutureUtils.returnError(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<ICrudService.Cursor<O>> getObjectsBySpec(final QueryComponent<O> spec) {
		return getObjectsBySpec(spec, Collections.emptyList(), false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<ICrudService.Cursor<O>> getObjectsBySpec(QueryComponent<O> spec, List<String> field_list, boolean include) {
		try {
			//TODO (ALEPH-14): Handle case where no source is present but fields are
			
			//TODO (ALEPH-14): if there's an obvious timestamp range then apply to getReadableIndexArray
			
			Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query = ElasticsearchUtils.convertToElasticsearchFilter(spec, _state.id_ranges_ok);

			final SearchRequestBuilder srb = Optional
						.of(
							_state.client.prepareSearch()
							.setIndices(_state.es_context.indexContext().getReadableIndexArray(Optional.empty()))
							.setTypes(_state.es_context.typeContext().getReadableTypeArray())
							.setQuery(QueryBuilders.constantScoreQuery(query._1()))							
							)
						.map(s -> (null != spec.getLimit())
									? s.setSize(spec.getLimit().intValue())
									: s)
						.map(s -> (null != spec.getOrderBy())
									? spec.getOrderBy().stream()
											.reduce(s, 
													(ss, sort) -> ss.addSort(sort._1(), sort._2() > 0 ? SortOrder.ASC : SortOrder.DESC), 
													(s1, s2) -> s1)
									: s)
						.map(s -> field_list.isEmpty() 
								? s 
								: include
									? s.setFetchSource(field_list.toArray(new String[0]), new String[0])
									: s.setFetchSource(new String[0], field_list.toArray(new String[0]))
							)
						.get();
			
			return ElasticsearchFutureUtils.wrap(srb.execute(), sr -> {				
				return new ElasticsearchCursor(sr);
			},
			(err, future) -> {
				if ((err instanceof IndexMissingException) || (err instanceof SearchPhaseExecutionException)) //(this one can come up as on a read on a newly created index)
				{ 	
					// just treat this like an "object not found"
					future.complete(new ElasticsearchCursor(null));
				}
				else {
					future.completeExceptionally(err);
				}
			}
			);
		}
		catch (Exception e) {
			return FutureUtils.returnError(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Long> countObjectsBySpec(QueryComponent<O> spec) {
		try {
			Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query = ElasticsearchUtils.convertToElasticsearchFilter(spec, _state.id_ranges_ok);
			
			final CountRequestBuilder crb = _state.client.prepareCount()
					.setIndices(_state.es_context.indexContext().getReadableIndexArray(Optional.empty()))
					.setTypes(_state.es_context.typeContext().getReadableTypeArray())
					.setQuery(QueryBuilders.constantScoreQuery(query._1()))
					;
			
			return ElasticsearchFutureUtils.wrap(crb.execute(), cr -> {
				return cr.getCount();
			},
			(err, future) -> {
				if ((err instanceof IndexMissingException) || (err instanceof SearchPhaseExecutionException)) //(this one can come up as on a read on a newly created index)
				{
					future.complete(0L);
				}
				else {
					future.completeExceptionally(err);
				}
			}
			);			
		}
		catch (Exception e) {
			return FutureUtils.returnError(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjects()
	 */
	@Override
	public CompletableFuture<Long> countObjects() {
		try {			
			final CountRequestBuilder crb = _state.client.prepareCount()
					.setIndices(_state.es_context.indexContext().getReadableIndexArray(Optional.empty()))
					.setTypes(_state.es_context.typeContext().getReadableTypeArray())
					;			
			
			return ElasticsearchFutureUtils.wrap(crb.execute(), cr -> {				
				return cr.getCount();
			},
			(err, future) -> {
				if ((err instanceof IndexMissingException) || (err instanceof SearchPhaseExecutionException)) //(this one can come up as on a read on a newly created index)
				{
					future.complete(0L);
				}
				else {
					future.completeExceptionally(err);
				}
			}
			);			
		}
		catch (Exception e) {
			return FutureUtils.returnError(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectById(java.lang.Object, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public CompletableFuture<Boolean> updateObjectById(Object id,
			UpdateComponent<O> update) {
		//TODO (ALEPH-14): TO BE IMPLEMENTED
		try {
			throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "updateObjectById"));
		}
		catch (Exception e) {
			return FutureUtils.returnError(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public CompletableFuture<Boolean> updateObjectBySpec(
			QueryComponent<O> unique_spec, Optional<Boolean> upsert,
			UpdateComponent<O> update) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public CompletableFuture<Long> updateObjectsBySpec(QueryComponent<O> spec,
			Optional<Boolean> upsert, UpdateComponent<O> update) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent, java.util.Optional, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Optional<O>> updateAndReturnObjectBySpec(
			QueryComponent<O> unique_spec, Optional<Boolean> upsert,
			UpdateComponent<O> update, Optional<Boolean> before_updated,
			List<String> field_list, boolean include) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectById(java.lang.Object)
	 */
	@Override
	public CompletableFuture<Boolean> deleteObjectById(final Object id) {
		try {
			final List<String> indexes = _state.es_context.indexContext().getReadableIndexList(Optional.empty());
			final List<String> types = _state.es_context.typeContext().getReadableTypeList();
			
			if ((types.size() != 1) || (indexes.size() > 1)) {
				// Multi index/type request, so use a query (which may not always return the most recent value, depending on index refresh settings/timings)
				return deleteObjectBySpec(CrudUtils.anyOf(_state.clazz).when(JsonUtils._ID, id.toString()));			
			}
			else {
				
				final DeleteRequestBuilder srb = Optional
						.of(
							_state.client.prepareDelete()
								.setIndex(indexes.get(0))
								.setId(id.toString())
							)
						.map(s -> (1 == types.size()) ? s.setType(types.get(0)) : s)
						.get();
				
				return ElasticsearchFutureUtils.wrap(srb.execute(), sr -> {
					return sr.isFound();
				},
				(err, future) -> {
					if ((err instanceof IndexMissingException) || (err instanceof SearchPhaseExecutionException)) //(this one can come up as on a read on a newly created index)
					{ 
						// just treat this like an "object not found"
						future.complete(false);
					}
					else {
						future.completeExceptionally(err);
					}
				}
				);			
			}
		}
		catch (Exception e) {
			return FutureUtils.returnError(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Boolean> deleteObjectBySpec(final QueryComponent<O> unique_spec) {
		try {
			Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query = ElasticsearchUtils.convertToElasticsearchFilter(unique_spec, _state.id_ranges_ok);
			
			final SearchRequestBuilder srb = 
					Optional
						.of(
							_state.client.prepareSearch()
							.setIndices(_state.es_context.indexContext().getReadableIndexArray(Optional.empty()))
							.setTypes(_state.es_context.typeContext().getReadableTypeArray())
							.setQuery(QueryBuilders.constantScoreQuery(query._1()))
							.setSize(1))
						.get();
			
			return ElasticsearchFutureUtils.wrap(srb.execute(), sr -> {
				final SearchHit[] sh = sr.getHits().hits();
				
				if (sh.length > 0) {
					
					final DeleteRequestBuilder drb = Optional
							.of(
								_state.client.prepareDelete()
									.setIndex(sh[0].index())
									.setId(sh[0].id())
									.setType(sh[0].type())
								)
							.get();
					
					return drb.execute().actionGet().isFound();
				}
				else {
					return false;
				}
			},
			(err, future) -> {
				if ((err instanceof IndexMissingException) || (err instanceof SearchPhaseExecutionException)) //(this one can come up as on a read on a newly created index)
				{ 
					// just treat this like an "object not found"
					future.complete(false);
				}
				else {
					future.completeExceptionally(err);
				}
			}
			);
		}
		catch (Exception e) {
			return FutureUtils.returnError(e);
		}
	}

	/** Utility class to support ping/pong buffering within lists
	 * @author Alex
	 *
	 * @param <T>
	 */
	protected static class PingPongList<T> {
		private final int _batch_size;
		private ArrayList<T> _mutable_write_to;
		private ArrayList<T> _mutable_read_from;
		public PingPongList(final int batch_size) {
			_batch_size = batch_size;
			_mutable_write_to = new ArrayList<T>(batch_size);
			_mutable_read_from = new ArrayList<T>(batch_size);
		}
		/** Adds an element to the list, returns true if the next add would lose data
		 *  (the caller should then process and clear getAboutToBeOverwrittenStream)
		 *  Sample flow: (R00,W00)F -> (R00,W50a).(R50a,W00)F -> (R50a,W50b).(R50b,W50a)T -> (R50b,00) -> (R50b,W50c).(R50c,W50b)T -> (R50c,00) etc
		 * @param t
		 * @return
		 */
		public boolean add(final T t) {
			_mutable_write_to.add(t);
			if (_mutable_write_to.size() >= _batch_size) {
				final ArrayList<T> tmp = _mutable_write_to;
				_mutable_write_to = _mutable_read_from;
				_mutable_read_from = tmp;
				return !_mutable_write_to.isEmpty();
			}
			else {
				return false;
			}
		}
		public Stream<T> getCompleteStream() { return Stream.concat(_mutable_read_from.stream(), _mutable_write_to.stream()); }
		public List<T> getAboutToBeOverwrittenList() { return _mutable_write_to; }
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Long> deleteObjectsBySpec(final QueryComponent<O> spec) {
		try {		
			Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> query = ElasticsearchUtils.convertToElasticsearchFilter(spec, _state.id_ranges_ok);
			
			final Optional<Long> maybe_size = Optional.ofNullable(spec.getLimit()).filter(x -> x > 0);
			// (don't scroll if a limit is set and we're sorting - note sorting is ignored otherwise)
			final boolean scroll = !(maybe_size.isPresent() && !Optionals.ofNullable(spec.getOrderBy()).isEmpty());
			final long max_size = maybe_size.orElse((long)Integer.MAX_VALUE).intValue();
			
			final SearchRequestBuilder srb = Optional.of(_state.client.prepareSearch()
						.setIndices(_state.es_context.indexContext().getReadableIndexArray(Optional.empty()))
						.setTypes(_state.es_context.typeContext().getReadableTypeArray())
						.setQuery(QueryBuilders.constantScoreQuery(query._1()))
						.setSize(1000)
						.setFetchSource(false)
						.setNoFields())
					.map(s -> (!scroll && (null != spec.getOrderBy()))
								? spec.getOrderBy().stream()
										.reduce(s, 
												(ss, sort) -> ss.addSort(sort._1(), sort._2() > 0 ? SortOrder.ASC : SortOrder.DESC), 
												(s1, s2) -> s1)
								: s)
					.map(s -> scroll ? 
							s.setSearchType(SearchType.SCAN).setScroll(new TimeValue(60000))
							: s 
							)
					.get()
					;			
			
			return ElasticsearchFutureUtils.wrap(srb.execute(), sr -> {
				long mutable_count = 0L; 
				final int batch_size = 50; 
				PingPongList<CompletableFuture<?>> mutable_future_batches = new PingPongList<>(batch_size);
				
				if (scroll && ((sr.getHits().totalHits() > 0) && (0 == sr.getHits().getHits().length))) { 
					//(odd workaround, if number of hits < scroll size, then the reply contains no hits, need to scroll an extra time to get it)
					sr = _state.client.prepareSearchScroll(sr.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
				}
				while ((sr.getHits().getHits().length > 0) && (mutable_count < max_size)) {
					BulkRequestBuilder bulk_request = _state.client.prepareBulk();
				    for (SearchHit sh : sr.getHits().getHits()) {
				    	bulk_request.add(
								_state.client.prepareDelete()
									.setIndex(sh.index())
									.setId(sh.id())
									.setType(sh.type()));

				    	mutable_count++; // (for now we'll just report on the _ids we found)
				    	if (mutable_count >= max_size) break;
				    }
				    // We're full, so wait for the first half of the data to complete
				    if (mutable_future_batches.add(ElasticsearchFutureUtils.wrap(bulk_request.execute(), __ -> null))) {
				    	try {
				    		CompletableFuture.allOf(mutable_future_batches.getAboutToBeOverwrittenList().stream().toArray(CompletableFuture[]::new)).join();
				    	}
				    	catch (Exception e) {} // just carry on if fails, probably more important to keep trying to delete
				    	
				    	mutable_future_batches.getAboutToBeOverwrittenList().clear();
				    }
				    if (scroll && (mutable_count < max_size)) sr = _state.client.prepareSearchScroll(sr.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
				    else break;
				}
				if (scroll) _state.client.prepareClearScroll().addScrollId(sr.getScrollId());

				//(wait for any remaining batches - this one we'll allow to error out since we've completed all our operations)
    			CompletableFuture.allOf(mutable_future_batches.getCompleteStream().toArray(CompletableFuture[]::new)).join();

				return mutable_count; //(just return an estimate)
			},
			(err, future) -> {
				if ((err instanceof IndexMissingException) || (err instanceof SearchPhaseExecutionException)) //(this one can come up as on a read on a newly created index)
				{ 
					// just treat this like an "object not found"
					future.complete(0L);
				}
				else {
					future.completeExceptionally(err);
				}
			}
			);
		}
		catch (Exception e) {
			return FutureUtils.returnError(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteDatastore()
	 */
	@Override
	public CompletableFuture<Boolean> deleteDatastore() {
		try {
			final ReadWriteContext rw_context = getRwContextOrThrow(_state.es_context, "deleteDatastore");
			
			final String[] index_list = rw_context.indexContext().getReadableIndexArray(Optional.empty());
			final boolean involves_wildcards = Arrays.stream(index_list).anyMatch(s -> s.contains("*"));
			DeleteIndexRequestBuilder dir = _state.client.admin().indices().prepareDelete(index_list);

			// First check if the indexes even exist, so can return false if they don't
			// (can bypass this if there are no wildcards, will get an exception instead)
			final CompletableFuture<Boolean> intermed = Lambdas.get(() -> {
				if (involves_wildcards) {
					final IndicesStatsRequestBuilder irb = _state.client.admin().indices().prepareStats(index_list);
					final CompletableFuture<Boolean> check_indexes =
						ElasticsearchFutureUtils.wrap(irb.execute(), 
						ir -> {
							return !ir.getIndices().isEmpty();
						},
						(err, future) -> {
							future.completeExceptionally(err);
						});
					return check_indexes;
				}
				else return CompletableFuture.completedFuture(true);
			});
			// Now try deleting the indexes
			return intermed.thenCompose(b -> {
				if (b) {
					return ElasticsearchFutureUtils.wrap(dir.execute(), dr -> {
						return true;
					},
					(err, future) -> {
						if ((err instanceof IndexMissingException) || (err instanceof SearchPhaseExecutionException)) //(this one can come up as on a read on a newly created index)
						{
							future.complete(false);
						}
						else {
							future.completeExceptionally(err);
						}
					}
					);					
				}
				else return CompletableFuture.completedFuture(false);
			});
		}
		catch (Exception e) {
			return FutureUtils.returnError(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getRawCrudService()
	 */
	@Override
	public ElasticsearchCrudService<JsonNode> getRawService() {
		return new ElasticsearchCrudService<JsonNode>(JsonNode.class, _state.es_context, Optional.of(_state.id_ranges_ok), _state.creation_policy, _state.auth_fieldname, _state.auth, _state.project, _batch_write_settings); 
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
	 */
	@Override
	public Optional<IBasicSearchService<O>> getSearchService() {
		//TODO (ALEPH-14): TO BE IMPLEMENTED
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "getSearchService"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(final Class<T> driver_class, final Optional<String> driver_options) {
		if (ElasticsearchContext.class == driver_class) return (Optional<T>) Optional.of(_state.es_context);
		else if (IMetaModel.class == driver_class) return (Optional<T>) getMetaModel(); 
		else if (IDataWriteService.IBatchSubservice.class.isAssignableFrom(driver_class)) return (Optional<T>) this.getBatchWriteSubservice(); 
		else return Optional.empty();
	}

	/** A subsystem providing a simple interface to dump JSON objects in batch into the CRUD service, at the expense of less visibility
	 * @author Alex
	 *
	 * @param <O> - the object type
	 */
	public class ElasticsearchBatchSubsystem implements IBatchSubservice<O> {
		final protected Object sync_lock = new Object(); 
		
		protected ElasticsearchBatchSubsystem() {
			// Kick off thread that handles higher speed flushing
			final ExecutorService executor = Executors.newSingleThreadExecutor();
			executor.submit(() -> {
				for (;;) {
					if (_flush_now) {
						synchronized (sync_lock) {
							_current.flush(); // (must always be non-null because _flush_now can only be set if _current exists)
							_flush_now = false;
						}
					}
					TimeUnit.MILLISECONDS.sleep(50);
				}
			});
		}
		
		@Override
		public void setBatchProperties(final Optional<Integer> max_objects, final Optional<Long> size_kb, final Optional<Duration> flush_interval, final Optional<Integer> write_threads)
		{
			BulkProcessor old = null;
			synchronized (sync_lock) {
				old = _current;
				_current = buildBulkProcessor(max_objects, size_kb, flush_interval, write_threads);
			}
			if (null != old) old.close();
		}

		protected BulkProcessor buildBulkProcessor() {
			return buildBulkProcessor(_batch_write_settings.map(DataSchemaBean.WriteSettings::batch_max_objects), 
					_batch_write_settings.map(DataSchemaBean.WriteSettings::batch_max_size_kb), 
					_batch_write_settings.map(DataSchemaBean.WriteSettings::batch_flush_interval).map(i -> Duration.of(i, ChronoUnit.SECONDS)), 
					_batch_write_settings.map(DataSchemaBean.WriteSettings::target_write_concurrency)
					);
		}
		
		/** Determines if this is actually a deletion request and what the _id is if so
		 * @param object
		 * @param is_replace_mode
		 * @return
		 */
		private String getPossibleDeletionRequest(final O object, final boolean is_replace_mode) {
			if (is_replace_mode && ObjectNode.class.isAssignableFrom(object.getClass())) {
				final ObjectNode j = (ObjectNode) object;
				if (1 == j.size()) { // ie empty... apart from...
					final JsonNode _id = j.get(JsonUtils._ID);
					if ((null != _id) && _id.isTextual()) {  // ... an _id
						return _id.asText();
					}
				}
			}
			return null;
		}
		
		@Override
		public void storeObjects(final List<O> new_objects, final boolean replace_if_present) {
			synchronized (sync_lock) {
				if (null == _current) {
					_current = buildBulkProcessor();
				}
				new_objects.stream().forEach(new_object -> {
					final String deletion_request_id = getPossibleDeletionRequest(new_object, replace_if_present);
					if (null != deletion_request_id) { // overwrite with empty object => delete
						_current.add(singleObjectDeleteRequest(Either.left((ReadWriteContext) _state.es_context), deletion_request_id,
							Either.left(new_object), true).request());
					}
					else {
						_current.add(singleObjectIndexRequest(Either.left((ReadWriteContext) _state.es_context), 
							Either.left(new_object), replace_if_present, true).request());
					}
				});
			}
		}

		@Override
		public void storeObject(final O new_object, final boolean replace_if_present) {
			synchronized (sync_lock) {
				if (null == _current) {
					_current = buildBulkProcessor();
				}			
				final String deletion_request_id = getPossibleDeletionRequest(new_object, replace_if_present);
				if (null != deletion_request_id) { // overwrite with empty object => delete
					_current.add(singleObjectDeleteRequest(Either.left((ReadWriteContext) _state.es_context), deletion_request_id,
						Either.left(new_object), true).request());
				}
				else {
					_current.add(singleObjectIndexRequest(Either.left((ReadWriteContext) _state.es_context), 
									Either.left(new_object), replace_if_present, true).request());
				}
			}
		}
		
		private boolean _flush_now = false; // (_very_ simple inter-thread comms via this mutable var, NOTE: don't let it get more complex than this without refactoring)
		
		protected BulkProcessor buildBulkProcessor(final Optional<Integer> max_objects, final Optional<Long> size_kb, final Optional<Duration> flush_interval, final Optional<Integer> write_threads) {
			return BulkProcessor.builder(_state.client, 
						new BulkProcessor.Listener() {							
							@Override
							public void beforeBulk(long exec_id, BulkRequest in) {
								return; // (nothing to do)
							}
							
							@Override
							public void afterBulk(long arg0, BulkRequest in, Throwable error) {
								return; // (nothing to exec_id but weep)
							}
							
							@Override
							public void afterBulk(long exec_id, BulkRequest in, BulkResponse out) {
								if (out.hasFailures() &&
										(_state.es_context.typeContext() instanceof ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext)
										)
								{
									final ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext auto_context = (ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext) _state.es_context.typeContext();
									final Iterator<BulkItemResponse> it = out.iterator();
									final LinkedList<Tuple2<BulkItemResponse, String>> mutable_errs = new LinkedList<>(); 
									while (it.hasNext()) {										
										final BulkItemResponse bir = it.next();
										if (bir.isFailed()) {								
											final String error_message = bir.getFailure().getMessage();
											
											if (error_message.startsWith("MapperParsingException")
													||
												error_message.startsWith("WriteFailureException; nested: MapperParsingException"))
											{
												final Set<String> fixed_type_fields = auto_context.fixed_type_fields();
												if (!fixed_type_fields.isEmpty()) {
													// Obtain the field name from the exception (if we fail then drop the record) 
													final String field = getFieldFromParsingException(error_message);
													if ((null == field) || fixed_type_fields.contains(field)) {
														continue;
													}
												}//(else roll on to...)																												
												
												final String failed_json = Lambdas.get(() -> {
													final ActionRequest<?> ar = in.requests().get(bir.getItemId());
													if (ar instanceof IndexRequest) {											
														IndexRequest ir = (IndexRequest) ar;
														return ir.source().toUtf8();
													}
													else return null;
												});
												if (null != failed_json) {
													mutable_errs.add(Tuples._2T(bir, failed_json));
												}
												
											}//(was a mapping error)
										}//(item failed)
									}//(loop over iterms)
										
									if (!mutable_errs.isEmpty()) { // Reinsert into the steam
										CompletableFuture.runAsync(() -> {
											synchronized (sync_lock) {
												_flush_now = true;
												mutable_errs.forEach(bir_json -> 
													_current.add(singleObjectIndexRequest(
																Either.right(Tuples._2T(bir_json._1().getIndex(), 
																		ElasticsearchContextUtils.getNextAutoType(auto_context.getPrefix(), bir_json._1().getType()))), 
																Either.right(Tuples._2T(bir_json._1().getId(), bir_json._2())), 
																false, true).request())
												);
											}														
										});
									}
										
								}//(has failures AND is an auto type)
								
							}//(end afterBulk)
						}//(end new Listener)
					)
					.setBulkActions(max_objects.orElse(1000))
					.setBulkSize(new ByteSizeValue(size_kb.orElse(10240L), ByteSizeUnit.KB))
					.setFlushInterval(TimeValue.timeValueSeconds(flush_interval.orElse(Duration.of(3, ChronoUnit.SECONDS)).get(ChronoUnit.SECONDS)))
					.setConcurrentRequests(1 + write_threads.orElse(0))
					.build();
		}
		
		protected BulkProcessor _current; // (note: mutable)

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService.IBatchSubservice#storeObjects(java.util.List)
		 */
		@Override
		public void storeObjects(List<O> new_objects) {
			storeObjects(new_objects, false);			
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService.IBatchSubservice#storeObject(java.lang.Object)
		 */
		@Override
		public void storeObject(O new_object) {
			storeObject(new_object, false);			
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService.IBatchSubservice#flushOutput()
		 */
		@Override
		public CompletableFuture<?> flushOutput() {
			synchronized (sync_lock) {
				if (null != _current) _current.flush(); 
			}
			// Just sleep for 1.25s
			return CompletableFuture.runAsync(Lambdas.wrap_runnable_i(() -> TimeUnit.MILLISECONDS.sleep(1250L)));
		}
	}
	protected ElasticsearchBatchSubsystem _batch_processor = null;
	
	/** A table-level interface to the CRUD store using the open MetaModel library
	 * MongoDB implementation
	 * @author acp
	 */
	public static class ElasticsearchDbMetaModel implements IMetaModel {
		protected ElasticsearchDbMetaModel(final ElasticsearchContext es_context) {
			final Client client = es_context.client();
			if ((es_context instanceof ReadWriteContext)
					&& (es_context.indexContext() instanceof ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext)
					&& (es_context.typeContext() instanceof ElasticsearchContext.TypeContext.ReadWriteTypeContext.FixedRwTypeContext))
			{
				ReadWriteContext rw_context = (ReadWriteContext) es_context;				
				_context = new ElasticSearchDataContext(client, rw_context.indexContext().getWritableIndex(Optional.empty()));
				_table = _context.getTableByQualifiedLabel(rw_context.typeContext().getWriteType());
			}
			else {
				throw new RuntimeException(ErrorUtils.METAMODEL_ELASTICSEARCH_RESTRICTIONS);
			}
		}
		public final DataContext _context; 
		public final Table _table; 
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.IMetaModel#getContext()
		 */
		public DataContext getContext() {
			return _context;
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.IMetaModel#getTable()
		 */
		public Table getTable() {
			return _table;
		}
	}
	protected ElasticsearchDbMetaModel _meta_model = null;

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getMetaModel()
	 */
	public Optional<IMetaModel> getMetaModel() {
		return Optional.of(((null == _meta_model) 
				? (_meta_model = new ElasticsearchDbMetaModel(_state.es_context)) 
				: _meta_model));		
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getCrudService()
	 */
	@Override
	public Optional<ICrudService<O>> getCrudService() {
		return Optional.of(this);
	}
	
	@Override
	public Optional<IBatchSubservice<O>> getBatchCrudSubservice() {
		if (_state.es_context instanceof ReadWriteContext) {
			if (null == _batch_processor) _batch_processor = new ElasticsearchBatchSubsystem();
			return Optional.of(_batch_processor);
		}
		else return Optional.empty();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Optional<IDataWriteService.IBatchSubservice<O>> getBatchWriteSubservice() {
		return (Optional<IDataWriteService.IBatchSubservice<O>>)(Optional<?>)getBatchCrudSubservice();
	}
}