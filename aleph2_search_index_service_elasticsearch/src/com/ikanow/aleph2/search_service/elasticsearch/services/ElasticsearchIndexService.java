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
package com.ikanow.aleph2.search_service.elasticsearch.services;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import scala.Tuple2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.ColumnarSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.TemporalSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.TimeUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean.CollidePolicy;
import com.ikanow.aleph2.search_service.elasticsearch.module.ElasticsearchIndexServiceModule;
import com.ikanow.aleph2.search_service.elasticsearch.utils.ElasticsearchIndexConfigUtils;
import com.ikanow.aleph2.search_service.elasticsearch.utils.ElasticsearchIndexUtils;
import com.ikanow.aleph2.search_service.elasticsearch.utils.SearchIndexErrorUtils;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext;
import com.ikanow.aleph2.shared.crud.elasticsearch.services.ElasticsearchCrudService.CreationPolicy;
import com.ikanow.aleph2.shared.crud.elasticsearch.services.IElasticsearchCrudServiceFactory;
import com.ikanow.aleph2.shared.crud.elasticsearch.utils.ElasticsearchContextUtils;
import com.ikanow.aleph2.shared.crud.elasticsearch.utils.ElasticsearchFutureUtils;

import fj.data.Validation;

/** Elasticsearch implementation of the SearchIndexService/TemporalService/ColumnarService
 * @author Alex
 *
 */
public class ElasticsearchIndexService implements ISearchIndexService, ITemporalService, IColumnarService, IExtraDependencyLoader {
	final static protected Logger _logger = LogManager.getLogger();

	protected final IElasticsearchCrudServiceFactory _crud_factory;
	protected final ElasticsearchIndexServiceConfigBean _config;
	
	protected final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	protected final ConcurrentHashMap<String, Date> _bucket_template_cache = new ConcurrentHashMap<>();
	
	/** Guice generated constructor
	 * @param crud_factory
	 */
	@Inject
	public ElasticsearchIndexService(
			final IElasticsearchCrudServiceFactory crud_factory,
			final ElasticsearchIndexServiceConfigBean configuration)
	{
		_crud_factory = crud_factory;
		_config = configuration;
	}
		
	/** Checks if an index/set-of-indexes spawned from a bucket
	 * @param bucket
	 */
	protected Optional<JsonNode> handlePotentiallyNewIndex(final DataBucketBean bucket, final Optional<String> secondary_buffer, final ElasticsearchIndexServiceConfigBean schema_config, final String index_type) {
		try {
			// Will need the current mapping regardless (this is just some JSON parsing so should be pretty quick):
			final XContentBuilder mapping = ElasticsearchIndexUtils.createIndexMapping(bucket, secondary_buffer, schema_config, _mapper, index_type);
			final JsonNode user_mapping = _mapper.readTree(mapping.bytes().toUtf8());
			
			final Date current_template_time = _bucket_template_cache.get(bucket._id());
			if ((null == current_template_time) || current_template_time.before(Optional.ofNullable(bucket.modified()).orElse(new Date()))) {			
				try {
					final GetIndexTemplatesRequest gt = new GetIndexTemplatesRequest().names(ElasticsearchIndexUtils.getBaseIndexName(bucket, secondary_buffer));
					final GetIndexTemplatesResponse gtr = _crud_factory.getClient().admin().indices().getTemplates(gt).actionGet();
					
					if (gtr.getIndexTemplates().isEmpty() 
							|| 
						!mappingsAreEquivalent(gtr.getIndexTemplates().get(0), user_mapping, _mapper))
					{
						// If no template, or it's changed, then update
						final String base_name = ElasticsearchIndexUtils.getBaseIndexName(bucket);
						_crud_factory.getClient().admin().indices().preparePutTemplate(base_name).setSource(mapping).execute().actionGet();
						
						_logger.info(ErrorUtils.get("Updated mapping for bucket={0}, base_index={1}", bucket.full_name(), base_name));
					}				
				}
				catch (Exception e) {
					_logger.error(ErrorUtils.getLongForm("Error updating mapper bucket={1} err={0}", e, bucket.full_name()));
				}
				_bucket_template_cache.put(bucket._id(), bucket.modified());			
			}
			return Optional.of(user_mapping);
		}
		catch (Exception e) {
			return Optional.empty();
		}
	}
	
	/** Check if a new mapping based on a schema is equivalent to a mapping previously stored (from a schema) 
	 * @param stored_mapping
	 * @param new_mapping
	 * @param mapper
	 * @return
	 * @throws JsonProcessingException
	 * @throws IOException
	 */
	protected static boolean mappingsAreEquivalent(final IndexTemplateMetaData stored_mapping, final JsonNode new_mapping, final ObjectMapper mapper) throws JsonProcessingException, IOException {		
		
		final ObjectNode stored_json_mappings = StreamSupport.stream(stored_mapping.mappings().spliterator(), false)
													.reduce(mapper.createObjectNode(), 
															Lambdas.wrap_u((acc, kv) -> (ObjectNode) acc.setAll((ObjectNode) mapper.readTree(kv.value.string()))), 
															(acc1, acc2) -> acc1); // (can't occur)
		
		final JsonNode new_json_mappings = Optional.ofNullable(new_mapping.get("mappings")).orElse(mapper.createObjectNode());
		
		final JsonNode stored_json_settings = mapper.convertValue(
												Optional.ofNullable(stored_mapping.settings()).orElse(ImmutableSettings.settingsBuilder().build())
													.getAsMap(), JsonNode.class);

		final JsonNode new_json_settings = Optional.ofNullable(new_mapping.get("settings")).orElse(mapper.createObjectNode());
				
		final ObjectNode stored_json_aliases = StreamSupport.stream(Optional.ofNullable(stored_mapping.aliases()).orElse(ImmutableOpenMap.of()).spliterator(), false)
				.reduce(mapper.createObjectNode(), 
						Lambdas.wrap_u((acc, kv) -> (ObjectNode) acc.set(kv.key, kv.value.filteringRequired()
								? mapper.createObjectNode().set("filter", mapper.readTree(kv.value.filter().string()))
								: mapper.createObjectNode()
								)),
						(acc1, acc2) -> acc1); // (can't occur)
		
		final JsonNode new_json_aliases = Optional.ofNullable(new_mapping.get("aliases")).orElse(mapper.createObjectNode());
		
		//DEBUG
//		System.out.println("1a: " + stored_json_mappings);
//		System.out.println("1b: " + new_json_mappings);
//		System.out.println(" 1: " + stored_json_mappings.equals(new_json_mappings));
//		System.out.println("2a: " + stored_json_settings);
//		System.out.println("2b: " + new_json_settings);
//		System.out.println(" 2: " + stored_json_settings.equals(new_json_settings));
//		System.out.println("3a: " + stored_json_aliases);
//		System.out.println("3b: " + new_json_aliases);
//		System.out.println(" 3: " + stored_json_aliases.equals(new_json_aliases));
		
		return stored_json_mappings.equals(new_json_mappings) && stored_json_settings.equals(new_json_settings) && stored_json_aliases.equals(new_json_aliases);		
	}
	
	////////////////////////////////////////////////////////////////////////////////
	
	// GENERIC DATA INTERFACE
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider#getDataService()
	 */
	@Override
	public Optional<IDataServiceProvider.IGenericDataService> getDataService() {
		return _data_service;
	}

	/** Implementation of GenericDataService
	 * @author alex
	 */
	public class ElasticsearchDataService implements IDataServiceProvider.IGenericDataService {

		@Override
		public <O> Optional<IDataWriteService<O>> getWritableDataService(
				Class<O> clazz, DataBucketBean bucket,
				Optional<String> options, Optional<String> secondary_buffer) {
			if (secondary_buffer.isPresent()) {
				throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "ElasticsearchDataService.getWritableDataService, secondary_buffer != Optional.empty()"));
			}
			// There's two different cases
			// 1) Multi-bucket - equivalent to the other version of getCrudService
			// 2) Single bucket - a read/write bucket
			
			if ((null != bucket.multi_bucket_children()) && !bucket.multi_bucket_children().isEmpty()) {
				throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "ElasticsearchDataService.getWritableDataService, multi_bucket_children"));
			}
			
			// If single bucket, is the search index service enabled?
			if (!Optional.ofNullable(bucket.data_schema())
					.map(ds -> ds.search_index_schema())
						.map(sis -> Optional.ofNullable(sis.enabled())
						.orElse(true))
				.orElse(false))
			{
				return Optional.empty();
			}
			
			// OK so it's a legit single bucket ... first question ... does this already exist?
			
			final ElasticsearchIndexServiceConfigBean schema_config = ElasticsearchIndexConfigUtils.buildConfigBeanFromSchema(bucket, _config, _mapper);
			
			final Optional<String> type = Optional.ofNullable(schema_config.search_technology_override()).map(t -> t.type_name_or_prefix());
			final String index_type = CollidePolicy.new_type == Optional.ofNullable(schema_config.search_technology_override())
										.map(t -> t.collide_policy()).orElse(CollidePolicy.new_type)
											? "_default_"
											: type.orElse(ElasticsearchIndexServiceConfigBean.DEFAULT_FIXED_TYPE_NAME);
			
			final Optional<JsonNode> user_mapping = handlePotentiallyNewIndex(bucket, secondary_buffer, schema_config, index_type);
			
			// Need to decide a) if it's a time based index b) an auto type index
			// And then build the context from there
			
			final Validation<String, ChronoUnit> time_period = TimeUtils.getTimePeriod(Optional.ofNullable(schema_config.temporal_technology_override())
																	.map(t -> t.grouping_time_period()).orElse(""));

			final Optional<Long> target_max_index_size_mb = Optionals.of(() -> schema_config.search_technology_override().target_index_size_mb());
			
			// Index
			final String index_base_name = ElasticsearchIndexUtils.getBaseIndexName(bucket);
			final ElasticsearchContext.IndexContext.ReadWriteIndexContext index_context = time_period.validation(
					fail -> new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext(index_base_name, target_max_index_size_mb)
					, 
					success -> new ElasticsearchContext.IndexContext.ReadWriteIndexContext.TimedRwIndexContext(index_base_name + ElasticsearchContextUtils.getIndexSuffix(success), 
										Optional.ofNullable(schema_config.temporal_technology_override().time_field()), target_max_index_size_mb)
					);			
			
			final boolean auto_type = 
					CollidePolicy.new_type == Optional.ofNullable(schema_config.search_technology_override())
						.map(t -> t.collide_policy()).orElse(CollidePolicy.new_type);
			
			final Set<String> fixed_type_fields = Lambdas.get(() -> {
				if (auto_type && user_mapping.isPresent()) {
					return ElasticsearchIndexUtils.getAllFixedFields(user_mapping.get());
				}
				else {
					return Collections.<String>emptySet();
				}				
			});
			
			// Type
			final ElasticsearchContext.TypeContext.ReadWriteTypeContext type_context =
					(auto_type && user_mapping.isPresent())
						? new ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext(Optional.empty(), type, fixed_type_fields)
						: new ElasticsearchContext.TypeContext.ReadWriteTypeContext.FixedRwTypeContext(type.orElse(ElasticsearchIndexServiceConfigBean.DEFAULT_FIXED_TYPE_NAME));
			
			final Optional<DataSchemaBean.WriteSettings> write_settings = Optionals.of(() -> bucket.data_schema().search_index_schema().target_write_settings());
			return Optional.of(_crud_factory.getElasticsearchCrudService(clazz,
									new ElasticsearchContext.ReadWriteContext(_crud_factory.getClient(), index_context, type_context),
									Optional.empty(), 
									CreationPolicy.OPTIMIZED, 
									Optional.empty(), Optional.empty(), Optional.empty(), write_settings));
		}

		@Override
		public <O> Optional<ICrudService<O>> getReadableCrudService(
				Class<O> clazz, Collection<DataBucketBean> buckets,
				Optional<String> options) {
			throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "ElasticsearchDataService.getReadableCrudService"));
		}

		@Override
		public Collection<String> getSecondaryBufferList(DataBucketBean bucket) {
			// TODO (#28): support secondary buffers
			return Collections.emptyList();
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#switchCrudServiceToPrimaryBuffer(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
		 */
		@Override
		public CompletableFuture<BasicMessageBean> switchCrudServiceToPrimaryBuffer(
				DataBucketBean bucket, Optional<String> secondary_buffer) {
			// TODO (#28): support secondary buffers
			//TODO what does this actually do? 1) updates the aliases, 2) update the data location				
			return CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage("ElasticsearchDataService", "switchCrudServiceToPrimaryBuffer", ErrorUtils.NOT_YET_IMPLEMENTED, "switchCrudServiceToPrimaryBuffer"));
		}

		@Override
		public CompletableFuture<BasicMessageBean> handleAgeOutRequest(
				DataBucketBean bucket) {
			// TODO (ALEPH-XXX)
			return CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage("ElasticsearchDataService", "handleAgeOutRequest", ErrorUtils.NOT_YET_IMPLEMENTED, "handleAgeOutRequest"));
		}

		@Override
		public CompletableFuture<BasicMessageBean> handleBucketDeletionRequest(
				DataBucketBean bucket, Optional<String> secondary_buffer,
				boolean bucket_getting_deleted) {
			if (secondary_buffer.isPresent()) {
				return CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage("ElasticsearchDataService", "handleBucketDeletionRequest", ErrorUtils.NOT_YET_IMPLEMENTED, "secondary_buffer != Optional.empty()"));				
			}
			
			Optional<ICrudService<JsonNode>> to_delete = this.getWritableDataService(JsonNode.class, bucket, Optional.empty(), Optional.empty())
															.flatMap(IDataWriteService::getCrudService);
			if (!to_delete.isPresent()) {
				
				return CompletableFuture.completedFuture(
						ErrorUtils.buildSuccessMessage("ElasticsearchDataService", "handleBucketDeletionRequest", "(No search index for bucket {0})", bucket.full_name())
						);
			}
			else { // delete the data			
				final CompletableFuture<Boolean> data_deletion_future = to_delete.get().deleteDatastore();

				final CompletableFuture<BasicMessageBean> combined_future = Lambdas.get(() -> {				
					// delete the template if fully deleting the bucket (vs purging)
					if (bucket_getting_deleted) {
						final CompletableFuture<DeleteIndexTemplateResponse> cf = ElasticsearchFutureUtils.
								<DeleteIndexTemplateResponse, DeleteIndexTemplateResponse>
									wrap(_crud_factory.getClient().admin().indices().prepareDeleteTemplate(ElasticsearchIndexUtils.getBaseIndexName(bucket)).execute(),								
											x -> x);
						
						return data_deletion_future.thenCombine(cf, (Boolean b, DeleteIndexTemplateResponse ditr) -> {
							return null; // don't care what the return value is, just care about catching exceptions
						});
					}
					else {					
						return data_deletion_future; // (see above)
					}
				})
				.thenApply(__ -> {
					return ErrorUtils.buildSuccessMessage("ElasticsearchDataService", "handleBucketDeletionRequest", "Deleted search index for bucket {0}", bucket.full_name());
				})
				.exceptionally(t -> {
					return ErrorUtils.buildErrorMessage("ElasticsearchDataService", "handleBucketDeletionRequest", ErrorUtils.getLongForm("Error deleting search index for bucket {1}: {0}", t, bucket.full_name()));
				})
				;
				return combined_future;
			}
		}
		
	}
	private final Optional<IDataServiceProvider.IGenericDataService> _data_service = Optional.of(new ElasticsearchDataService());
	
	////////////////////////////////////////////////////////////////////////////////

	// ES CLIENT ACCESS	
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(final Class<T> driver_class, final Optional<String> driver_options) {
		if (Client.class.isAssignableFrom(driver_class)) {
			return (Optional<T>) Optional.of(_crud_factory.getClient());
		}
		return Optional.empty();
	}

	////////////////////////////////////////////////////////////////////////////////

	// VALIDATION	
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.ColumnarSchemaBean)
	 */
	@Override
	public Tuple2<String, List<BasicMessageBean>> validateSchema(final ColumnarSchemaBean schema, final DataBucketBean bucket) {
		// (Performed under search index schema)
		return Tuples._2T("", Collections.emptyList());
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.TemporalSchemaBean)
	 */
	@Override
	public Tuple2<String, List<BasicMessageBean>> validateSchema(final TemporalSchemaBean schema, final DataBucketBean bucket) {
		// (time buckets aka default schema options are already validated, nothing else to do)
	
		final Validation<String, ChronoUnit> time_period = TimeUtils.getTimePeriod(Optional.ofNullable(schema)
																			.map(t -> t.grouping_time_period())
																		.orElse(""));
		
		final String time_based_suffix = time_period.validation(fail -> "", success -> ElasticsearchContextUtils.getIndexSuffix(success));
		
		return Tuples._2T(time_based_suffix, Collections.emptyList());
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean)
	 */
	@Override
	public Tuple2<String, List<BasicMessageBean>> validateSchema(final SearchIndexSchemaBean schema, final DataBucketBean bucket) {
		try {
			final String index_name = ElasticsearchIndexUtils.getBaseIndexName(bucket);
			final LinkedList<BasicMessageBean> errors = new LinkedList<BasicMessageBean>(); // (Warning mutable code)
			boolean error = false; // (Warning mutable code)
			final boolean is_verbose = is_verbose(schema);
			final ElasticsearchIndexServiceConfigBean schema_config = ElasticsearchIndexConfigUtils.buildConfigBeanFromSchema(bucket, _config, _mapper);
			
			// 1) Check the schema:
			
			try {				
				final Optional<String> type = Optional.ofNullable(schema_config.search_technology_override()).map(t -> t.type_name_or_prefix());
				final String index_type = CollidePolicy.new_type == Optional.ofNullable(schema_config.search_technology_override())
											.map(t -> t.collide_policy()).orElse(CollidePolicy.new_type)
												? "_default_"
												: type.orElse(ElasticsearchIndexServiceConfigBean.DEFAULT_FIXED_TYPE_NAME);
				
				final XContentBuilder mapping = ElasticsearchIndexUtils.createIndexMapping(bucket, Optional.empty(), schema_config, _mapper, index_type);
				if (is_verbose) {
					errors.add(ErrorUtils.buildSuccessMessage(bucket.full_name(), "validateSchema", mapping.bytes().toUtf8()));
				}
			}
			catch (Throwable e) {
				errors.add(ErrorUtils.buildErrorMessage(bucket.full_name(), "validateSchema", ErrorUtils.getLongForm("{0}", e)));
				error = true;
			}
			
			// 2) Sanity check the max size
	
			final Optional<Long> index_max_size = Optional.ofNullable(schema_config.search_technology_override().target_index_size_mb());
			if (index_max_size.isPresent()) {
				final long max = index_max_size.get();
				if ((max > 0) && (max < 25)) {
					errors.add(ErrorUtils.buildErrorMessage(bucket.full_name(), "validateSchema", SearchIndexErrorUtils.INVALID_MAX_INDEX_SIZE, max));
					error = true;
				}
				else if (is_verbose) {
					errors.add(ErrorUtils.buildSuccessMessage(bucket.full_name(), "validateSchema", "Max index size = {0} MB", max));				
				}
			}		
			return Tuples._2T(error ? "" : index_name, errors);
		}
		catch (Exception e) { // Very early error has occurred, just report that:
			return Tuples._2T("", Arrays.asList(ErrorUtils.buildErrorMessage(bucket.full_name(), "validateSchema", ErrorUtils.getLongForm("{0}", e))));
		}
	}
	
	/** Utility function - returns verboseness of schema being validated
	 * @param schema
	 * @return
	 */
	protected static boolean is_verbose(final SearchIndexSchemaBean schema) {
		return Optional.ofNullable(schema)
					.map(SearchIndexSchemaBean::technology_override_schema)
					.map(m -> m.get("verbose"))
					.filter(b -> b.toString().equalsIgnoreCase("true") || b.toString().equals("1"))
					.map(b -> true) // (if we're here then must be true/1)
				.orElse(false);
	}
	
	////////////////////////////////////////////////////////////////////////////////
	
	/** This service needs to load some additional classes via Guice. Here's the module that defines the bindings
	 * @return
	 */
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new ElasticsearchIndexServiceModule());
	}
	
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		//(done!)
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Arrays.asList(this, _crud_factory);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService#handleAgeOutRequest(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> handleAgeOutRequest(
			DataBucketBean bucket) {
		// TODO (ALEPH-14): handle age out
		return null;
	}

}
