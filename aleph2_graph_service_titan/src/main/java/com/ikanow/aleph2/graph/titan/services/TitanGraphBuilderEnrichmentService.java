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
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
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
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.graph.titan.data_model.GraphBuilderConfigBean;
import com.ikanow.aleph2.graph.titan.utils.TitanGraphBuildingUtils;
import com.ikanow.aleph2.graph.titan.utils.TitanGraphBuildingUtils.MutableStatsBean;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.diskstorage.TemporaryBackendException;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;

/** Service for building the edges and vertices from the incoming records and updating any existing entries in the database
 *  NOTE: there should be a non-tech-specific GraphBuilderEnrichmentService in the analytics context library that wraps this one
 *  so that user code doesn't reference anything titan specific (the analytics context will grab the right one using the getUnderlyingArtefacts from the GraphService)
 * @author Alex
 *
 */
public class TitanGraphBuilderEnrichmentService implements IEnrichmentBatchModule {
	public final static String UUID = UuidUtils.get().getRandomUuid().split("-")[4];
	
	protected final SetOnce<IEnrichmentBatchModule> _custom_graph_decomp_handler = new SetOnce<>();
	protected final SetOnce<GraphDecompEnrichmentContext> _custom_graph_decomp_context = new SetOnce<>();
	protected final SetOnce<IEnrichmentBatchModule> _custom_graph_merge_handler = new SetOnce<>();
	protected final SetOnce<GraphMergeEnrichmentContext> _custom_graph_merge_context = new SetOnce<>();
	protected final SetOnce<GraphSchemaBean> _config = new SetOnce<>();
	protected final SetOnce<DataBucketBean> _bucket = new SetOnce<>();
		
	protected final SetOnce<IServiceContext> _service_context = new SetOnce<>();
	protected final SetOnce<Tuple2<String, ISecurityService>> _security_context = new SetOnce<>();
	protected final SetOnce<IBucketLogger> _logger = new SetOnce<>();
	protected final SetOnce<MutableStatsBean> _mutable_stats = new SetOnce<>();
	protected final SetOnce<TitanGraph> _titan = new SetOnce<>();
	
	protected final Set<ObjectNode> _mutable_new_vertex_keys = new HashSet<>();
	
	// Special test mode
	protected LinkedList<TitanException> _MUTABLE_TEST_ERRORS = new LinkedList<>();
	
	protected Long[] _BACKOFF_TIMES_MS = { 1500L, 3000L, 6000L, 12000L, 24000L };
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageInitialize(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean, scala.Tuple2, java.util.Optional)
	 */
	@Override
	public void onStageInitialize(IEnrichmentModuleContext context,
			DataBucketBean bucket, EnrichmentControlMetadataBean control,
			Tuple2<ProcessingStage, ProcessingStage> previous_next,
			Optional<List<String>> next_grouping_fields) {
	
		final GraphBuilderConfigBean dedup_config = BeanTemplateUtils.from(Optional.ofNullable(control.config()).orElse(Collections.emptyMap()), GraphBuilderConfigBean.class).get();
		
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
		_bucket.set(bucket);
		_mutable_stats.set(new MutableStatsBean());
		
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

			_custom_graph_decomp_context.set(new GraphDecompEnrichmentContext(context, _config.get()));
			
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

			_custom_graph_merge_context.set(new GraphMergeEnrichmentContext(context, _config.get()));
			
			_custom_graph_merge_handler.optional().ifPresent(base_module -> base_module.onStageInitialize(_custom_graph_merge_context.get(), bucket, cfg, previous_next, next_grouping_fields));
		});
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onObjectBatch(java.util.stream.Stream, java.util.Optional, java.util.Optional)
	 */
	@Override
	public void onObjectBatch(Stream<Tuple2<Long, IBatchRecord>> batch, Optional<Integer> batch_size, Optional<JsonNode> grouping_key)
	{		
		// Get user assets:
		
		final List<ObjectNode> vertices_and_edges = 
				TitanGraphBuildingUtils.buildGraph_getUserGeneratedAssets(batch, batch_size, grouping_key, 
						_custom_graph_decomp_handler.optional().map(handler -> Tuples._2T(handler, _custom_graph_decomp_context.get())));

		final MutableStatsBean mutable_stats = new MutableStatsBean();
		
		tryRecoverableTransaction(mutable_tx -> {
			final Stream<ObjectNode> copy_vertices_and_edges = vertices_and_edges.stream().map(o -> o.deepCopy());
			
			// Fill in transaction
			
			mutable_stats.reset();
			
			TitanGraphBuildingUtils.buildGraph_handleMerge(mutable_tx, _config.get(), _security_context.get(), _logger.optional(), mutable_stats,
					_mutable_new_vertex_keys,
					_custom_graph_merge_handler.optional().map(handler -> Tuples._2T(handler, _custom_graph_merge_context.get()))
					, 
					_bucket.get(),
					TitanGraphBuildingUtils.buildGraph_collectUserGeneratedAssets(mutable_tx, _config.get(), 
							_security_context.get(), _logger.optional(), 
							_bucket.get(), mutable_stats,
							copy_vertices_and_edges
					));					

			//(test mode for errors)
			if (!_MUTABLE_TEST_ERRORS.isEmpty()) {
				throw _MUTABLE_TEST_ERRORS.pop();
			}			
		},
		() -> {
			_logger.optional().ifPresent(logger -> {
				logger.log(Level.DEBUG,
						ErrorUtils.lazyBuildMessage(true, 
								() -> "GraphBuilderEnrichmentService",
								() -> "system.onObjectBatch",
								() -> null, 
								() -> ErrorUtils.get("Graph stats: V_emitted={0} V_matched={1} V_created={2} V_updated={3} V_errors={4} E_emitted={5} E_matched={6} E_created={7} E_updated={8} E_errors={9} (uuid={10})",
										mutable_stats.vertices_emitted, mutable_stats.vertex_matches_found, mutable_stats.vertices_created, mutable_stats.vertices_updated, mutable_stats.vertex_errors,
										mutable_stats.edges_emitted, mutable_stats.vertex_matches_found, mutable_stats.edges_created, mutable_stats.edges_updated, mutable_stats.edge_errors,
										UUID
										), 
								() -> BeanTemplateUtils.toMap(mutable_stats)));
			});		
			_mutable_stats.get().combine(mutable_stats);		
		});		
	}

	protected void tryRecoverableTransaction(
			final Consumer<TitanTransaction> transaction,
			final Runnable on_success
			)
	{
		final int MAX_ATTEMPT_NUM = 4;
		IntStream.range(0, 1 + MAX_ATTEMPT_NUM).boxed().filter(i -> { //(at most 5 attempts)
			try {
				// Create transaction
				
				//TRACE
//				System.err.println(new java.util.Date().toString() + ": GRABBING TRANS " + i);
				
				final TitanTransaction mutable_tx = _titan.get().newTransaction();
				try {
					transaction.accept(mutable_tx);
				}
				catch (Exception e) { //(close the transaction without saving)
					mutable_tx.rollback();
					throw e;
				}
				
				//TRACE
				//System.err.println(new java.util.Date().toString() + ": COMMITTING TRANS " + i);				
				
				// Attempt to commit
				mutable_tx.commit();

				//TRACE
				//System.err.println(new java.util.Date().toString() + ": COMMITTED TRANS " + i);				
				
				on_success.run();
				
				return true; // (ie ends the loop)
			}
			catch (TitanException e) {
				if ((i >= MAX_ATTEMPT_NUM) || !isRecoverableError(e)) {

					//DEBUG
					//System.err.println(new java.util.Date().toString() + ": HERE2 NON_RECOV: " + i + " vs " + MAX_ATTEMPT_NUM);
					//e.printStackTrace();					
					
					_logger.optional().ifPresent(logger -> {
						logger.log(Level.ERROR,
								ErrorUtils.lazyBuildMessage(false, 
										() -> "GraphBuilderEnrichmentService",
										() -> "system.onStageComplete",
										() -> null, 
										() -> ErrorUtils.getLongForm("Failed to commit transaction due to local conflicts, attempt_num={1} error={0} (uuid={2})", e, i, UUID),
										() -> null));
					});					
					throw e;
				}

				// If we're here, we're going to retry the transaction
				
				//DEBUG
//				System.err.println(new java.util.Date().toString() + ": HERE3 RECOVERABLE");
				
				_logger.optional().ifPresent(logger -> {
					logger.log(Level.DEBUG,
							ErrorUtils.lazyBuildMessage(false, 
									() -> "GraphBuilderEnrichmentService",
									() -> "system.onStageComplete",
									() -> null, 
									() -> ErrorUtils.get("Failed to commit transaction due to local conflicts, attempt_num={0} (uuid={1})", i, UUID),
									() -> null));
				});
				
				try { Thread.sleep(_BACKOFF_TIMES_MS[i]); } catch (Exception interrupted) {}				
				return false;
				
				// (If it's a versioning conflict then try again)
			}
			//TRACE:
//			catch (Throwable x) {
//				System.err.println(new java.util.Date().toString() + ": HERE1 OTHER ERR");
//				x.printStackTrace();
//				throw x;
//			}
		})
		.findFirst() // ie stop as soon as we have successfully transacted
		;				
	}
	
	/** Utility to check for recoverable errors
	 * @param e
	 * @return
	 */
	private static boolean isRecoverableError_internal(Throwable e) {
		// (temporary backend encompasses temporary locking)
		return (PermanentLockingException.class.isAssignableFrom(e.getClass()) || TemporaryBackendException.class.isAssignableFrom(e.getClass()));
				
	}
	
	/** Utility to check for recoverable errors
	 * @param e
	 * @return
	 */
	protected static boolean isRecoverableError(TitanException e) {
		if (null != e.getCause()) {
			if (isRecoverableError_internal(e.getCause())) // versioning error, repeat transaction					
			{
				return true;				
			}
			else if ((null != e.getCause().getCause()) && isRecoverableError_internal(e.getCause().getCause())) //versioning error, repeat transaction
			{
				return true;
			}
		}
		return false;
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageComplete(boolean)
	 */
	@Override
	public void onStageComplete(boolean is_original) {
		
		// First off sleep somewhere between 0 and 2s to ensure the blocks get out of sync
		if (is_original) {
			final Random random_generator = new Random(new Date().getTime());
			try { Thread.sleep(random_generator.nextInt(2000)); } catch (Exception e) {}
		}
		
		final int batch_size = 250;
		
		// OK now going to start merging new records one final time:
		
		final MutableStatsBean global_combine_stats = new MutableStatsBean();
		final MutableStatsBean combine_stats = new MutableStatsBean();
		Iterators.partition(_mutable_new_vertex_keys.iterator(), batch_size)
			.forEachRemaining(batch -> {						
				tryRecoverableTransaction(tx -> {
					
					final MutableStatsBean per_batch_stats = new MutableStatsBean();
					
					final Map<JsonNode, List<Vertex>> grouped_vertices = 
							TitanGraphBuildingUtils.getGroupedVertices(batch, tx, 
									_config.get().deduplication_fields(), 
									vertex -> Optionals.streamOf(vertex.properties(GraphAnnotationBean.a2_p), false)
															.anyMatch(p -> _bucket.get().full_name().equals(p.value()))
									);
										
					TitanGraphBuildingUtils.mergeDuplicates(tx, _bucket.get().full_name(), grouped_vertices, per_batch_stats);
					
					global_combine_stats.combine(per_batch_stats);
					combine_stats.reset();
					combine_stats.combine(per_batch_stats);
				},
				() -> {
					//TODO stats have slightly different meaning (use different bean?)
					_logger.optional().ifPresent(logger -> {
						logger.log(Level.DEBUG,
								ErrorUtils.lazyBuildMessage(true, 
										() -> "GraphBuilderEnrichmentService",
										() -> "system.onStageComplete",
										() -> null, 
										() -> ErrorUtils.get("Batch merge stats: V_emitted={0} V_matched={1} V_created={2} V_updated={3} V_errors={4} E_emitted={5} E_matched={6} E_created={7} E_updated={8} E_errors={9} (uuid={10})",
												combine_stats.vertices_emitted, combine_stats.vertex_matches_found, combine_stats.vertices_created, combine_stats.vertices_updated, combine_stats.vertex_errors,
												combine_stats.edges_emitted, combine_stats.vertex_matches_found, combine_stats.edges_created, combine_stats.edges_updated, combine_stats.edge_errors,
												UUID
												), 
										() -> BeanTemplateUtils.toMap(_mutable_stats.get())));
					});
				}
				);
			});		

		//TODO stats have slightly different meaning
		_logger.optional().ifPresent(logger -> {
			logger.log(Level.INFO,
					ErrorUtils.lazyBuildMessage(true, 
							() -> "GraphBuilderEnrichmentService",
							() -> "system.onStageComplete",
							() -> null, 
							() -> ErrorUtils.get("Final merge stats: V_emitted={0} V_matched={1} V_created={2} V_updated={3} V_errors={4} E_emitted={5} E_matched={6} E_created={7} E_updated={8} E_errors={9} (uuid={10})",
									global_combine_stats.vertices_emitted, global_combine_stats.vertex_matches_found, global_combine_stats.vertices_created, global_combine_stats.vertices_updated, global_combine_stats.vertex_errors,
									global_combine_stats.edges_emitted, global_combine_stats.vertex_matches_found, global_combine_stats.edges_created, global_combine_stats.edges_updated, global_combine_stats.edge_errors,
									UUID
									), 
							() -> BeanTemplateUtils.toMap(_mutable_stats.get())));
		});		
		
		_logger.optional().ifPresent(logger -> {
			logger.log(Level.INFO,
					ErrorUtils.lazyBuildMessage(true, 
							() -> "GraphBuilderEnrichmentService",
							() -> "system.onStageComplete",
							() -> null, 
							() -> ErrorUtils.get("Graph stats: V_emitted={0} V_matched={1} V_created={2} V_updated={3} V_errors={4} E_emitted={5} E_matched={6} E_created={7} E_updated={8} E_errors={9} (uuid={10})",
									_mutable_stats.get().vertices_emitted, _mutable_stats.get().vertex_matches_found, _mutable_stats.get().vertices_created, _mutable_stats.get().vertices_updated, _mutable_stats.get().vertex_errors,
									_mutable_stats.get().edges_emitted, _mutable_stats.get().vertex_matches_found, _mutable_stats.get().edges_created, _mutable_stats.get().edges_updated, _mutable_stats.get().edge_errors,
									UUID
									), 
							() -> BeanTemplateUtils.toMap(_mutable_stats.get())));
		});
		
		_custom_graph_decomp_handler.optional().ifPresent(handler -> handler.onStageComplete(is_original));
		_custom_graph_merge_handler.optional().ifPresent(handler -> handler.onStageComplete(is_original));
	}

}
