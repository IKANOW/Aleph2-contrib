/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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

package com.ikanow.aleph2.graph.titan.services;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import scala.Tuple2;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.IReadOnlyCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.GraphAnnotationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.graph.titan.data_model.TitanGraphConfigBean;
import com.ikanow.aleph2.graph.titan.module.TitanGraphModule;
import com.ikanow.aleph2.graph.titan.utils.ErrorUtils;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/** Titan implementation of the graph service
 * @author Alex
 *
 */
public class TitanGraphService implements IGraphService, IGenericDataService, IExtraDependencyLoader {
	protected static Logger _logger = LogManager.getLogger();
	
	public static String GLOBAL_CREATED_GV = "aleph2_created_gv"; // (_G for graph as opposed to the secondary edge indices, "_ge" for graph-edge)
	public static String GLOBAL_MODIFIED_GV = "aleph2_modified_gv";
	public static String GLOBAL_PATH_INDEX_GV = "aleph2_path_query_gv";
	public static String GLOBAL_PATH_INDEX_GE = "aleph2_path_query_ge";
	public static String GLOBAL_DEFAULT_INDEX_GV = "aleph2_index_query_gv";
	protected static String UUID = System.getProperty("java.io.tmpdir") + "/titan_test_" + UuidUtils.get().getRandomUuid();
	protected final TitanGraph _titan;
	
	protected boolean _USE_ES_FOR_DEDUP_INDEXES = false;
	
	/** Guice injector
=	 */
	@Inject
	public TitanGraphService(TitanGraphConfigBean config) {
		_titan = Lambdas.get(() -> {
			try {
				
				//TODO (ALEPH-15): or allow various overrides using the standard config bean syntax
				
				//TODO: (ALEPH-15): would be interesting to allow separate table/indexes for certain buckets ("contexts" like in dedup ... eg
				// generate a unique signature for a list of dedup contexts - would need to handle incremental changes, yikes - and then name the backend/ES store based on that) 
				
				return setup(config);
			}
			catch (Throwable t) {
				_logger.error(ErrorUtils.getLongForm("Unable to open Titan graph DB: {0}", t));
				return null;
			}
		});
	}
	
	/** Mock titan c'tor to allow it to use the protected _titan property
	 * @param mock
	 */
	protected TitanGraphService(boolean mock) {
		_titan = TitanFactory.build()
						.set("storage.backend", "inmemory")
						.set("index.search.backend", "elasticsearch")
						.set("index.search.elasticsearch.local-mode", true)
						.set("index.search.directory", UUID)
						.set("index.search.cluster-name", UUID)
						.set("index.search.ignore-cluster-name", false)
						.set("index.search.elasticsearch.client-only", false)
						.set("query.force-index", true)
					.open();
	}
	
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		//TODO (ALEPH-15): also need ES if ES is enabled (/hbase if hbase is enabled, though going to make the hbase compat embedded for now)
		return Arrays.asList(this);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> maybe_driver_options) {
		
		return Patterns.match(driver_class).<Optional<T>>andReturn()
			.when(clazz -> IEnrichmentBatchModule.class.isAssignableFrom(clazz) && 
					maybe_driver_options.map(driver_opts -> driver_opts.equals("com.ikanow.aleph2.analytics.services.GraphBuilderEnrichmentService")).orElse(false),
					__ -> Optional.<T>of((T) new TitanGraphBuilderEnrichmentService()))
			.when(clazz -> TitanGraph.class.isAssignableFrom(clazz), __ -> Optional.<T>of((T) _titan))
			.otherwise(__ -> Optional.empty())
			;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public Tuple2<String, List<BasicMessageBean>> validateSchema(
			GraphSchemaBean schema, DataBucketBean bucket) {
		
		final LinkedList<BasicMessageBean> errors = new LinkedList<>();
		
		if (Optionals.ofNullable(schema.custom_decomposition_configs()).isEmpty()) {
			errors.add(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "validateSchema", ErrorUtils.DECOMPOSITION_ENRICHMENT_NEEDED, bucket.full_name()));
		}
		if (Optionals.ofNullable(schema.custom_merge_configs()).isEmpty()) {
			errors.add(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "validateSchema", ErrorUtils.MERGE_ENRICHMENT_NEEDED, bucket.full_name()));
		}	
		if (!Optionals.ofNullable(schema.deduplication_fields()).isEmpty()) {
			errors.add(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "validateSchema", ErrorUtils.NOT_YET_IMPLEMENTED, "custom:deduplication_fields"));			
		}
		if (!Optionals.ofNullable(schema.deduplication_contexts()).isEmpty()) {
			errors.add(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "validateSchema", ErrorUtils.NOT_YET_IMPLEMENTED, "custom:deduplication_contexts"));			
		}
		return Tuples._2T("",  errors);
	}

	//////////////////////////////////////////////////////////
	
	// DATA SERVICE PROVIDER / GENERIC DATA SERVICE
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider#getDataService()
	 */
	@Override
	public Optional<IGenericDataService> getDataService() {
		return Optional.of(this);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider#onPublishOrUpdate(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, boolean, java.util.Set, java.util.Set)
	 */
	@Override
	public CompletableFuture<Collection<BasicMessageBean>> onPublishOrUpdate(
			DataBucketBean bucket, Optional<DataBucketBean> old_bucket,
			boolean suspended, Set<String> data_services,
			Set<String> previous_data_services)
	{
		try {
			if (previous_data_services.contains(DataSchemaBean.GraphSchemaBean.name) && !data_services.contains(DataSchemaBean.GraphSchemaBean.name)) {
				return handleBucketDeletionRequest(bucket, Optional.empty(), false).thenApply(b -> Arrays.asList(b));
			}
			else if (data_services.contains(DataSchemaBean.GraphSchemaBean.name)) {
	
				final TitanManagement mgmt = _titan.openManagement();
				
				// First off, let's ensure that a2_p is indexed: (note these apply to both vertixes and edges)
				try {
					final PropertyKey bucket_index = mgmt.makePropertyKey(GraphAnnotationBean.a2_p).dataType(String.class).cardinality(Cardinality.SET).make();
					try {
						mgmt.buildIndex(GLOBAL_PATH_INDEX_GV, Vertex.class)
							.addKey(bucket_index, Mapping.STRING.asParameter())
							.buildMixedIndex("search");
					}
					catch (SchemaViolationException e) {} // (already indexed, this is fine/expected)				
					try {
						mgmt.buildIndex(GLOBAL_PATH_INDEX_GE, Edge.class)
							.addKey(bucket_index, Mapping.STRING.asParameter())
							.buildMixedIndex("search");
					}
					catch (SchemaViolationException e) {
						//DEBUG
						//e.printStackTrace();
					} // (already indexed, this is fine/expected)
				}
				catch (SchemaViolationException e) {
					//DEBUG
					//e.printStackTrace();
				} // (already indexed, this is fine/expected)

				// Created/modified
				try {
					mgmt.buildIndex(GLOBAL_CREATED_GV, Vertex.class)
						.addKey(mgmt.makePropertyKey(GraphAnnotationBean.a2_tc).dataType(Long.class).make())
						.buildMixedIndex("search");					
				}
				catch (SchemaViolationException e) {} // (already indexed, this is fine/expected)				
				try {
					mgmt.buildIndex(GLOBAL_MODIFIED_GV, Vertex.class)
						.addKey(mgmt.makePropertyKey(GraphAnnotationBean.a2_tm).dataType(Long.class).make())
						.buildMixedIndex("search");					
				}
				catch (SchemaViolationException e) {} // (already indexed, this is fine/expected)
				
				// Then check that the global default index is set
				Optional<List<String>> maybe_dedup_fields = Optionals.of(() -> bucket.data_schema().graph_schema().deduplication_fields());
				final Collection<BasicMessageBean> ret_val = maybe_dedup_fields.map(dedup_fields -> {
					//TODO (ALEPH-15): manage the index pointed to by the bucket's signature					
					return Arrays.asList(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "onPublishOrUpdate", ErrorUtils.NOT_YET_IMPLEMENTED, "custom:deduplication_fields"));			
				})
				.orElseGet(() -> {
					try {
						//TODO (ALEPH-15): There's a slightly tricky decision here...
						// using ES makes dedup actions "very" no longer transactional because of the index refresh
						// (in theory using Cassandra or HBase makes things transactional, though I haven't tested that)
						// Conversely not using ES puts more of the load on the smaller Cassandra/HBase clusters						
						// Need to make configurable, but default to using the transactional layer
						// Of course, if I move to eg unipop later on then I'll have to fix this
						// It's not really any different to deduplication _except_ it can happen across buckets (unlike dedup) so it's much harder to stop
						// A few possibilities:
						// 1) Have a job that runs on new-ish data (ofc that might not be easy to detect) that merges things (ofc very unclear how to do that)
						// 2) Centralize all insert actions
						// Ah here's some more interest - looks like Hbase and Cassandra's eventual consistency can cause duplicates:
						// http://s3.thinkaurelius.com/docs/titan/1.0.0/eventual-consistency.html ... tldr: basically need to have regular "clean up jobs" and live with transient issues
						// (or use a consistent data store - would also require a decent amount of code here because our dedup strategy is not perfect)
						
						if (_USE_ES_FOR_DEDUP_INDEXES) {
							mgmt.buildIndex(GLOBAL_DEFAULT_INDEX_GV, Vertex.class)
								.addKey(mgmt.makePropertyKey(GraphAnnotationBean.name).dataType(String.class).make(), Mapping.TEXTSTRING.asParameter())
								.addKey(mgmt.makePropertyKey(GraphAnnotationBean.type).dataType(String.class).make(), Mapping.STRING.asParameter())
								.buildMixedIndex("search");
						}
						else { // use the storage backend which should have better consistency properties
							mgmt.buildIndex(GLOBAL_DEFAULT_INDEX_GV, Vertex.class)
								.addKey(mgmt.makePropertyKey(GraphAnnotationBean.name).dataType(String.class).make())	
								.addKey(mgmt.makePropertyKey(GraphAnnotationBean.type).dataType(String.class).make())	
								.buildCompositeIndex();
							
							//(in this case, also index "name" as an ES field to make it easier to search over)
							mgmt.buildIndex(GLOBAL_DEFAULT_INDEX_GV + "_TEXT", Vertex.class) 
							.addKey(mgmt.makePropertyKey(GraphAnnotationBean.name).dataType(String.class).make(), Mapping.TEXT.asParameter())
							.buildMixedIndex("search");													
						}
					}
					catch (SchemaViolationException e) {} // (already indexed, this is fine/expected)
					return Collections.emptyList();
				});
				
				//TODO (ALEPH-15): allow other indexes (etc) via the schema override				
				mgmt.commit();
				
				//TODO (ALEPH-15): want to complete the future only once the indexing steps are done?
				return CompletableFuture.completedFuture(ret_val); 
			}
			else {
				return CompletableFuture.completedFuture(Collections.emptyList()); 			
			}
		}
		catch (Throwable t) {
			return CompletableFuture.completedFuture(Arrays.asList(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "onPublishOrUpdate", ErrorUtils.getLongForm("{0}", t))));
		}		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getWritableDataService(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, java.util.Optional)
	 */
	@Override
	public <O> Optional<IDataWriteService<O>> getWritableDataService(
			Class<O> clazz, DataBucketBean bucket, Optional<String> options,
			Optional<String> secondary_buffer) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getReadableCrudService(java.lang.Class, java.util.Collection, java.util.Optional)
	 */
	@Override
	public <O> Optional<IReadOnlyCrudService<O>> getReadableCrudService(
			Class<O> clazz, Collection<DataBucketBean> buckets,
			Optional<String> options) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getUpdatableCrudService(java.lang.Class, java.util.Collection, java.util.Optional)
	 */
	@Override
	public <O> Optional<ICrudService<O>> getUpdatableCrudService(
			Class<O> clazz, Collection<DataBucketBean> buckets,
			Optional<String> options) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getSecondaryBuffers(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
	 */
	@Override
	public Set<String> getSecondaryBuffers(DataBucketBean bucket,
			Optional<String> intermediate_step) {
		return Collections.emptySet();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getPrimaryBufferName(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
	 */
	@Override
	public Optional<String> getPrimaryBufferName(DataBucketBean bucket,
			Optional<String> intermediate_step) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#switchCrudServiceToPrimaryBuffer(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> switchCrudServiceToPrimaryBuffer(
			DataBucketBean bucket, Optional<String> secondary_buffer,
			Optional<String> new_name_for_ex_primary,
			Optional<String> intermediate_step) {
		return CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "switchCrudServiceToPrimaryBuffer", ErrorUtils.BUFFERS_NOT_SUPPORTED, bucket.full_name()));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#handleAgeOutRequest(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> handleAgeOutRequest(
			DataBucketBean bucket) {
		// TODO (ALEPH-15): implement various temporal handling features (don't return error though, just do nothing)
		return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(this.getClass().getSimpleName(), "handleAgeOutRequest", ErrorUtils.NOT_YET_IMPLEMENTED, "handleAgeOutRequest"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#handleBucketDeletionRequest(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, boolean)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> handleBucketDeletionRequest(
			DataBucketBean bucket, Optional<String> secondary_buffer,
			boolean bucket_or_buffer_getting_deleted) {
		
		if (secondary_buffer.isPresent()) {
			return CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "handleBucketDeletionRequest", ErrorUtils.BUFFERS_NOT_SUPPORTED, bucket.full_name()));			
		}
		
		//TODO (ALEPH-15): At some point need to be able for services to (optionally) request batch enrichment jobs - eg would be much nicer to fire this off as a distributed job
		
		return CompletableFuture.runAsync(() -> {
			
			try { Thread.sleep(1000L); } catch (Exception e) {} // just check the indexes have refreshed...
			
			final TitanTransaction tx = _titan.buildTransaction().start();

			//DEBUG
			//final com.fasterxml.jackson.databind.ObjectMapper titan_mapper = _titan.io(org.apache.tinkerpop.gremlin.structure.io.IoCore.graphson()).mapper().create().createMapper();
			
			@SuppressWarnings("unchecked")
			final Stream<TitanVertex> vertices_to_check = Optionals.<TitanVertex>streamOf(tx.query().has(GraphAnnotationBean.a2_p, bucket.full_name()).vertices(), false);
			vertices_to_check.forEach(v -> {
				{
					final Iterator<VertexProperty<String>> props = v.<String>properties(GraphAnnotationBean.a2_p);
					while (props.hasNext()) {
						final VertexProperty<String> prop = props.next();
						if (bucket.full_name().equals(prop.value())) {
							prop.remove();
						}
					}
				}
				{
					final Iterator<VertexProperty<String>> props = v.<String>properties(GraphAnnotationBean.a2_p);
					if (!props.hasNext()) { // can delete this bucket
						v.remove();
					}
				}
			});
			@SuppressWarnings("unchecked")
			final Stream<TitanEdge> edges_to_check = Optionals.<TitanEdge>streamOf(tx.query().has(GraphAnnotationBean.a2_p, bucket.full_name()).edges(), false);
			edges_to_check.forEach(e -> {
				e.remove(); // (can only have one edge so delete it)
			});
			
			tx.commit();
		})
		.thenApply(__ -> 
			ErrorUtils.buildSuccessMessage(this.getClass().getSimpleName(), "handleBucketDeletionRequest", "Completed", "handleBucketDeletionRequest")
					)
		.exceptionally(t -> 
			ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "handleBucketDeletionRequest", ErrorUtils.getLongForm("{0}", t), "handleBucketDeletionRequest")
					)
		;
		
	}

	//////////////////////////////////////////////////////
	
	// Configuration utils
		
	/** This service needs to load some additional classes via Guice. Here's the module that defines the bindings
	 * @return
	 */
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new TitanGraphModule());
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader#youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules()
	 */
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// (done see above)		
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#createRemoteConfig(com.typesafe.config.Config)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Config createRemoteConfig(Optional<DataBucketBean> maybe_bucket, Config local_config) {
		if (null == _titan) return local_config; // (titan is disabled, just pass through)
		
		final Config distributed_config = 
				ConfigFactory.parseMap((AbstractMap<String, ?>)(AbstractMap<?, ?>)new ConfigurationMap(_titan.configuration()));
		
		return local_config.withValue(TitanGraphConfigBean.PROPERTIES_ROOT, distributed_config.root());
	}

	/** Builds a Titan graph from the config bean
	 * @param config
	 * @return
	 */
	protected TitanGraph setup(TitanGraphConfigBean config) {
		if (null != config.config_override()) {
			return TitanFactory.open(new MapConfiguration(config.config_override()));
		}
		else {
			final String path = Optional.of(config.config_path_name())
					.map(p -> (p.matches("^[a-zA-Z]:.*") || p.startsWith(".") || p.startsWith("/"))
								? p
								: ModuleUtils.getGlobalProperties().local_yarn_config_dir() + "/" + p
					)
					.get()
					;					
			return TitanFactory.open(path);
		}		
	}
}
