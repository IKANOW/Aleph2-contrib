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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.GraphAnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.graph.titan.data_model.GraphConfigBean;
import com.ikanow.aleph2.graph.titan.utils.TitanGraphBuildingUtils;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TransactionBuilder;

/** Service for building the edges and vertices from the incoming records and updating any existing entries in the database
 *  NOTE: there should be a non-tech-specific GraphBuilderEnrichmentService in the analytics context library that wraps this one
 *  so that user code doesn't reference anything titan specific (the analytics context will grab the right one using the getUnderlyingArtefacts from the GraphService)
 * @author Alex
 *
 */
public class TitanGraphBuilderEnrichmentService implements IEnrichmentBatchModule {

	protected final SetOnce<IEnrichmentBatchModule> _custom_graph_decomp_handler = new SetOnce<>();
	protected final SetOnce<GraphDecompEnrichmentContext> _custom_graph_decomp_context = new SetOnce<>();
	protected final SetOnce<IEnrichmentBatchModule> _custom_graph_merge_handler = new SetOnce<>();
	protected final SetOnce<GraphMergeEnrichmentContext> _custom_graph_merge_context = new SetOnce<>();
	protected final SetOnce<GraphSchemaBean> _config = new SetOnce<>();
		
	protected final SetOnce<IServiceContext> _service_context = new SetOnce<>();
	protected final SetOnce<Tuple2<String, ISecurityService>> _security_context = new SetOnce<>();
	protected final SetOnce<IBucketLogger> _logger = new SetOnce<>();
	protected final SetOnce<TitanGraph> _titan = new SetOnce<>();
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageInitialize(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean, scala.Tuple2, java.util.Optional)
	 */
	@Override
	public void onStageInitialize(IEnrichmentModuleContext context,
			DataBucketBean bucket, EnrichmentControlMetadataBean control,
			Tuple2<ProcessingStage, ProcessingStage> previous_next,
			Optional<List<String>> next_grouping_fields) {
	
		final GraphConfigBean dedup_config = BeanTemplateUtils.from(Optional.ofNullable(control.config()).orElse(Collections.emptyMap()), GraphConfigBean.class).get();
		
		final GraphSchemaBean graph_schema = Optional.ofNullable(dedup_config.graph_schema_override()).orElse(bucket.data_schema().graph_schema()); //(exists by construction)
		_config.set(BeanTemplateUtils.clone(graph_schema)
				.with(GraphSchemaBean::custom_finalize_all_objects, Optional.ofNullable(graph_schema.custom_finalize_all_objects()).orElse(false))
				.with(GraphSchemaBean::deduplication_fields, Optional.ofNullable(graph_schema.deduplication_fields()).orElse(Arrays.asList(GraphAnnotationBean.name, GraphAnnotationBean.type)))
				.done()
				);
		
		// Get the Titan graph
		
		_service_context.set(context.getServiceContext());
		_logger.set(context.getLogger(Optional.of(bucket)));
		_security_context.set(Tuples._2T(bucket.owner_id(), _service_context.get().getSecurityService()));
		
		_service_context.get()
			.getService(IGraphService.class, Optional.ofNullable(graph_schema.service_name()))
			.flatMap(graph_service -> graph_service.getUnderlyingPlatformDriver(TitanGraph.class, Optional.empty()))
			.ifPresent(titan -> _titan.set(titan));
			;		
		
		// Set up decomposition enrichment
		
		final Optional<EnrichmentControlMetadataBean> custom_decomp_config =  Optionals.ofNullable(graph_schema.custom_decomposition_configs()).stream().findFirst();
		custom_decomp_config.ifPresent(cfg -> {
			final Optional<String> entry_point = Optional.ofNullable(cfg.entry_point())
					.map(Optional::of)
					.orElseGet(() -> {
						// Get the shared library bean:
						
						return BucketUtils.getBatchEntryPoint(							
							context.getServiceContext().getCoreManagementDbService().readOnlyVersion().getSharedLibraryStore()
								.getObjectBySpec(CrudUtils.anyOf(SharedLibraryBean.class)
											.when(SharedLibraryBean::_id, cfg.module_name_or_id())
											.when(SharedLibraryBean::path_name, cfg.module_name_or_id())
										)
								.join()
								.map(bean -> (Map<String, SharedLibraryBean>)ImmutableMap.of(cfg.module_name_or_id(), bean))
								.orElse(Collections.<String, SharedLibraryBean>emptyMap())
								,
								cfg);
					});
			
			entry_point.ifPresent(Lambdas.wrap_consumer_u(ep -> 
			_custom_graph_decomp_handler.set((IEnrichmentBatchModule) Class.forName(ep, true, Thread.currentThread().getContextClassLoader()).newInstance())));

			_custom_graph_decomp_context.set(new GraphDecompEnrichmentContext(context));
			
			_custom_graph_decomp_handler.optional().ifPresent(base_module -> base_module.onStageInitialize(_custom_graph_decomp_context.get(), bucket, cfg, previous_next, next_grouping_fields));
		});
		
		// Set up merging enrichment
		
		final Optional<EnrichmentControlMetadataBean> custom_merge_config =  Optionals.ofNullable(graph_schema.custom_merge_configs()).stream().findFirst();
		custom_merge_config.ifPresent(cfg -> {
			final Optional<String> entry_point = Optional.ofNullable(cfg.entry_point())
					.map(Optional::of)
					.orElseGet(() -> {
						// Get the shared library bean:
						
						return BucketUtils.getBatchEntryPoint(							
							context.getServiceContext().getCoreManagementDbService().readOnlyVersion().getSharedLibraryStore()
								.getObjectBySpec(CrudUtils.anyOf(SharedLibraryBean.class)
											.when(SharedLibraryBean::_id, cfg.module_name_or_id())
											.when(SharedLibraryBean::path_name, cfg.module_name_or_id())
										)
								.join()
								.map(bean -> (Map<String, SharedLibraryBean>)ImmutableMap.of(cfg.module_name_or_id(), bean))
								.orElse(Collections.<String, SharedLibraryBean>emptyMap())
								,
								cfg);
					});
			
			entry_point.ifPresent(Lambdas.wrap_consumer_u(ep -> 
			_custom_graph_merge_handler.set((IEnrichmentBatchModule) Class.forName(ep, true, Thread.currentThread().getContextClassLoader()).newInstance())));

			_custom_graph_merge_context.set(new GraphMergeEnrichmentContext(context));
			
			_custom_graph_merge_handler.optional().ifPresent(base_module -> base_module.onStageInitialize(_custom_graph_merge_context.get(), bucket, cfg, previous_next, next_grouping_fields));
		});
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onObjectBatch(java.util.stream.Stream, java.util.Optional, java.util.Optional)
	 */
	@Override
	public void onObjectBatch(Stream<Tuple2<Long, IBatchRecord>> batch,
			Optional<Integer> batch_size, Optional<JsonNode> grouping_key) {
		
		// Get user assets:
		
		final List<ObjectNode> vertices_and_edges = 
				TitanGraphBuildingUtils.buildGraph_getUserGeneratedAssets(batch, batch_size, grouping_key, 
						_custom_graph_decomp_handler.optional().map(handler -> Tuples._2T(handler, _custom_graph_decomp_context.get())));

		for (int i = 0; i < 5; ++i) { //(at most 5 attempts)
			try {
				// Create transaction
				
				final TransactionBuilder tx_b = _titan.get().buildTransaction();
				final TitanTransaction mutable_tx = tx_b.start();
				final Stream<ObjectNode> copy_vertices_and_edges = vertices_and_edges.stream().map(o -> o.deepCopy());
				
				// Fill in transaction
				
				TitanGraphBuildingUtils.buildGraph_handleMerge(mutable_tx, _config.get(), _security_context.get(), _logger.get(), 
						_custom_graph_merge_handler.optional().map(handler -> Tuples._2T(handler, _custom_graph_merge_context.get()))
						, 
						TitanGraphBuildingUtils.buildGraph_collectUserGeneratedAssets(mutable_tx, _config.get(), 
								_security_context.get(), _logger.get(), 
								copy_vertices_and_edges
						));
				
				// Attempt to commit
				
				mutable_tx.commit();
			}
			catch (TitanException e) {
				//TODO
				
				// Handle different transaction failures
				
				// (If it's a versioning conflict then try again)
			}
		}			
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageComplete(boolean)
	 */
	@Override
	public void onStageComplete(boolean is_original) {
		
		_custom_graph_decomp_handler.optional().ifPresent(handler -> handler.onStageComplete(is_original));
		_custom_graph_merge_handler.optional().ifPresent(handler -> handler.onStageComplete(is_original));
	}

}
