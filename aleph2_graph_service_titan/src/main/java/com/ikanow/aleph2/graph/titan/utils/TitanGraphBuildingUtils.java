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

package com.ikanow.aleph2.graph.titan.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.ikanow.aleph2.core.shared.utils.BatchRecordUtils;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.GraphAnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.graph.titan.services.GraphDecompEnrichmentContext;
import com.ikanow.aleph2.graph.titan.services.GraphMergeEnrichmentContext;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.attribute.Contain;

import fj.data.Either;
import fj.data.Validation;

/** Collection of utilities for building Titan graph elements from a batch of data objects
 * @author Alex
 *
 */
public class TitanGraphBuildingUtils {
	final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());

	/** Handy utility class for keep track of stats for logging purposes
	 * @author Alex
	 */
	public static class MutableStatsBean {
		public long vertices_emitted = 0L;
		public long edges_emitted = 0L;
		public long vertices_created = 0L;
		public long edges_created = 0L;
		public long vertices_updated = 0L;
		public long edges_updated = 0L;
		public long vertex_errors = 0L;
		public long edge_errors = 0L;
		public long vertex_matches_found = 0L;
		public long edge_matches_found = 0L;
		
		/** Mutable reset
=		 */
		public void reset() {
			vertices_emitted = 0L;
			edges_emitted = 0L;
			vertices_created = 0L;
			edges_created = 0L;
			vertices_updated = 0L;
			edges_updated = 0L;
			vertex_errors = 0L;
			edge_errors = 0L;
			vertex_matches_found = 0L;
			edge_matches_found = 0L;			
		}
		
		/** Mutable update method
		 * @param per_batch
		 */
		public void combine(final MutableStatsBean per_batch) {
			vertices_emitted += per_batch.vertices_emitted;
			edges_emitted += per_batch.edges_emitted;
			vertices_created += per_batch.vertices_created;
			edges_created += per_batch.edges_created;
			vertices_updated += per_batch.vertices_updated;
			edges_updated += per_batch.edges_updated;
			vertex_errors += per_batch.vertex_errors;
			edge_errors += per_batch.edge_errors;
			vertex_matches_found += per_batch.vertex_matches_found;
			edge_matches_found += per_batch.edge_matches_found;
		}
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////
	////////////////////////////////
	
	// UTILS - TOP LEVEL LOGIC
	
	/** (1/3) Calls user code to extract vertices and edges
	 * @param batch
	 * @param batch_size
	 * @param grouping_key
	 * @param maybe_decomposer
	 * @return
	 */
	public static List<ObjectNode> buildGraph_getUserGeneratedAssets(
			final Stream<Tuple2<Long, IBatchRecord>> batch,
			final Optional<Integer> batch_size, 
			final Optional<JsonNode> grouping_key,
			final Optional<Tuple2<IEnrichmentBatchModule, GraphDecompEnrichmentContext>> maybe_decomposer
			)
	{
		// First off build the edges and vertices
		
		maybe_decomposer.ifPresent(handler -> handler._1().onObjectBatch(batch, batch_size, grouping_key));
		
		final List<ObjectNode> vertices_and_edges = maybe_decomposer.map(context -> context._2().getAndResetVertexList()).orElse(Collections.emptyList());
		
		return vertices_and_edges;		
	}
	
	/** (2/3) Creates a stream of user-generated assets together (grouped by vertex key) with associated data 
	 * @param tx
	 * @param config
	 * @param security_service
	 * @param logger
	 * @param vertices_and_edges
	 * @return
	 */
	public static Stream<Tuple4<ObjectNode, List<ObjectNode>, List<ObjectNode>, List<Vertex>>> buildGraph_collectUserGeneratedAssets(
			final TitanTransaction tx, 
			final GraphSchemaBean config,
			final Tuple2<String, ISecurityService> security_service,
			final Optional<IBucketLogger> logger,
			final DataBucketBean bucket,
			final MutableStatsBean mutable_stats,
			final Stream<ObjectNode> vertices_and_edges)
	{		
		// Convert the list of vertexes into a mega query - will have a false positive rate to keep the query simple  
		
		final Map<ObjectNode, Tuple2<List<ObjectNode>, List<ObjectNode>>> nodes_to_get = groupNewEdgesAndVertices(config, mutable_stats, vertices_and_edges);	
		
		final Map<JsonNode, List<Vertex>> grouped_vertices = 
				getGroupedVertices(nodes_to_get.keySet(), tx, 
						config.deduplication_fields(), vertex -> isAllowed(bucket.full_name(), security_service, vertex));
		
		//TRACE:
		//System.err.println(new Date().toString() + ": DUPS=" + grouped_vertices);
		
		// Match up the user vertices with the ones obtained from the system
		
		return nodes_to_get.entrySet().stream()
			.map(kv -> Tuples._4T(kv.getKey(), kv.getValue()._1(), kv.getValue()._2(), grouped_vertices.getOrDefault(kv.getKey(), Collections.emptyList())))
			;
		
	}
	
	/** (3/3) Merges user generated edges/vertices with the ones already in the system 
	 * @param tx
	 * @param config
	 * @param security_service
	 * @param logger
	 * @param maybe_merger
	 * @param mergeable
	 */
	public static void buildGraph_handleMerge(
				final TitanTransaction tx,
				final GraphSchemaBean config,
				final Tuple2<String, ISecurityService> security_service,
				final Optional<IBucketLogger> logger,
				final MutableStatsBean mutable_stats,
				final Collection<ObjectNode> mutable_new_vertex_keys,
				final Optional<Tuple2<IEnrichmentBatchModule, GraphMergeEnrichmentContext>> maybe_merger,
				final DataBucketBean bucket,
				final Stream<Tuple4<ObjectNode, List<ObjectNode>, List<ObjectNode>, List<Vertex>>> mergeable
			)
	{	
		final org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper titan_mapper = tx.io(IoCore.graphson()).mapper().create().createMapper();
		final Multimap<JsonNode, Edge> mutable_existing_edge_endpoint_store = LinkedHashMultimap.create(); //(lazy simple way of handling 1.3/2)
		final Map<ObjectNode, Vertex> mutable_per_merge_cached_vertices = new HashMap<>();
		
		mergeable.forEach(t4 -> { 

			//TODO (ALEPH-15): handling properties: add new properties and:
			// remove any properties of any vertex/edge over which the user does not have read permission .. and then re-combine later
			
			final ObjectNode key = t4._1();
			final List<ObjectNode> vertices = t4._2();
			final List<ObjectNode> edges = t4._3();
			final List<Vertex> existing_vertices = t4._4();
			mutable_stats.vertex_matches_found += existing_vertices.size();
			
			// 1) First step is to sort out the _vertices_, here's the cases:
			
			// 1.1) If there's no matching vertices then create a new vertex and get the id (via a merge if finalize is set)
			//      (overwrite the _id then map to a Vertex)
			// 1.2) If there are >0 matching vertices (and only one incoming vertex) then we run a merge in which the user "has to do" the following:
			// 1.2.a) pick the winning vertex (or emit the current one to create a "duplicate node"?)
			// 1.2.a.1) (Allow user to delete the others if he has permission, by the usual emit "id" only - but don't automatically do it because it gets complicated what to do with the other _bs)
			// 1.2.b) copy any properties from the original objects into the winner and remove any so-desired properties

			final long prev_created = mutable_stats.vertices_created; //(nasty hack, see below)
			
			final Optional<Vertex> maybe_vertex_winner = 
					invokeUserMergeCode(tx, config, security_service, logger, maybe_merger, titan_mapper, mutable_stats, Vertex.class, bucket.full_name(), key, vertices, existing_vertices, Collections.emptyMap())
						.stream()
						.findFirst()
						;
			
			maybe_vertex_winner.ifPresent(vertex_winner -> {
				mutable_per_merge_cached_vertices.put(key, vertex_winner);
				
				//(slighty nasty hack, use stats to see if a vertex was created vs updated...)
				if (mutable_stats.vertices_created > prev_created) {
					mutable_new_vertex_keys.add(key);
				}
				
				// 1.3) Tidy up (mutate) the edges				

				// 1.3.1) Make a store of all the existing edges (won't worry about in/out, it will sort itself out)
				
				Stream.of(
						Optionals.streamOf(vertex_winner.edges(Direction.IN), false),
						Optionals.streamOf(vertex_winner.edges(Direction.OUT), false),
						Optionals.streamOf(vertex_winner.edges(Direction.BOTH), false))
						.flatMap(__ -> __)
						.forEach(e -> {
							mutable_existing_edge_endpoint_store.put(key, e);
						});						
				
				// 1.3.2) Handle incoming edges:
				
				final Map<ObjectNode, List<ObjectNode>> grouped_edges = finalEdgeGrouping(key, vertex_winner, edges);
				
				// 2) By here we have a list of vertices and we've mutated the edges to fill in the _inV and _outV
				// 2.1) Now get the potentially matching edges from each of the selected vertices:
				// 2.1.1) If there's no matching edges (and only one incoming edge) then create a new edge (via a merge if finalize is set)
				// 2.1.2) If there are >0 matching edges then run a merge against the edges, pick the current one
				
				// OK now for any resolved edges (ie grouped_edges), match up with the mutable store (which will be correctly populated by construction):
				
				grouped_edges.entrySet().stream().forEach(kv -> {
					
					final Function<String, Map<Object, Edge>> getEdges = in_or_out -> 
						Optionals.ofNullable(mutable_existing_edge_endpoint_store.get(kv.getKey().get(in_or_out)))
							.stream()
							.filter(e -> labelMatches(kv.getKey(), e)) 
							.filter(e -> isAllowed(bucket.full_name(), security_service, e)) // (check authorized)
							.collect(Collectors.toMap(e -> e.id(), e -> e))
							;
					final Map<Object, Edge> in_existing = getEdges.apply(GraphAnnotationBean.inV);
					final Map<Object, Edge> out_existing = getEdges.apply(GraphAnnotationBean.outV);
					
					final List<Edge> existing_edges = BucketUtils.isTestBucket(bucket)
							? Collections.emptyList()
							: Stream.of(
										Maps.difference(in_existing, out_existing).entriesInCommon().values().stream(),
										in_existing.values().stream().filter(e -> e.inVertex() == e.outVertex()) // (handle the case where an edge starts/ends at the same node)
								)		
								.flatMap(__ -> __)
								.collect(Collectors.toList())
								;

					mutable_stats.edge_matches_found += existing_edges.size();
							
					invokeUserMergeCode(tx, config, security_service, logger, maybe_merger, titan_mapper, mutable_stats, Edge.class, bucket.full_name(), kv.getKey(), kv.getValue(), existing_edges, mutable_per_merge_cached_vertices);
				});
			});
				
		});
		
		//TRACE
		//System.err.println(new Date() + ": VERTICES FOUND = " + mutable_existing_vertex_store);
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////
	////////////////////////////////
	
	// UTILS - HIGH LEVEL
	
	/** Utility to get the vertices in the DB matching the specified keys TODO: move to intermediate utils  
	 * @param keys
	 * @param bucket_filter
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static final Map<JsonNode, List<Vertex>> getGroupedVertices(
			final Collection<ObjectNode> keys,
			final TitanTransaction tx,
			final List<String> key_fields,
			final Predicate<Vertex> vertex_filter
			)
	{
		final Stream<TitanVertex> dups = Lambdas.get(() -> {
			final Map<String, Set<Object>> dedup_query_builder = 		
					keys.stream()
						.flatMap(j -> Optionals.streamOf(j.fields(), false))
						.collect(Collectors.groupingBy(kv -> kv.getKey(), Collectors.mapping(kv -> jsonNodeToObject(kv.getValue()), Collectors.toSet())));
						;		
			
			//TODO (ALEPH-15): would be nice to support custom "fuzzier" queries, since we're doing a dedup stage to pick the actual winning vertices anyway
			// that way you could say query on tokenized-version of name and get anyone with the same first or last name (say) and then pick the most likely
			// one based on the graph ... of course you'd probably want the full graph for that, so it might end up being better served as a "self-analytic" to do is part
			// of post processing?
			// (NOTE: same remarks apply for edges)
			// (NOTE: currently I've been going in the opposite direction, ie enforcing only one vertex per keyset per bucket ... otherwise it's going to get really 
			//  confusing when you try to merge all the different versions that Titan creates because of the lack of an upsert function....)
						
			final TitanGraphQuery<?> matching_nodes_query = dedup_query_builder.entrySet().stream()
				.reduce(tx.query()
						,
						(query, kv) -> query.has(kv.getKey(), Contain.IN, kv.getValue())
						,
						(query1, query2) -> query1 // (can't occur since reduce not parallel)
						);		
			
			return Optionals.streamOf(matching_nodes_query.vertices(), false);
		});
		
		// Remove false positives, un-authorized nodes, and group by key
				
		final Map<JsonNode, List<Vertex>> grouped_vertices = dups 
					.map(vertex -> Tuples._2T((Vertex) vertex, getElementProperties(vertex, key_fields)))
					.filter(vertex_key -> keys.contains(vertex_key._2())) // (remove false positives)
					.filter(vertex_key -> vertex_filter.test(vertex_key._1())) // (remove un-authorized nodes)
					.collect(Collectors.groupingBy(t2 -> (JsonNode) t2._2(),  // (group by key)
								Collectors.mapping(t2 -> t2._1(), Collectors.toList())))
					;
		
		
		
		return grouped_vertices; 
	}
	
	/** Separates out edges/vertices, groups by key
	 * @param config
	 * @param vertices_and_edges
	 * @return
	 */
	protected static Map<ObjectNode, Tuple2<List<ObjectNode>, List<ObjectNode>>> groupNewEdgesAndVertices(
			final GraphSchemaBean config,
			final MutableStatsBean stats,
			final Stream<ObjectNode> vertices_and_edges)
	{
		final Map<ObjectNode, Tuple2<List<ObjectNode>, List<ObjectNode>>> nodes_to_get = 		
				vertices_and_edges
					.filter(o -> o.has(GraphAnnotationBean.type))
					.<Tuple3<ObjectNode, ObjectNode, Boolean>>flatMap(o -> {
						final JsonNode type = o.get(GraphAnnotationBean.type);
						
						if ((null == type) || !type.isTextual()) return Stream.empty();
						if (GraphAnnotationBean.ElementType.edge.toString().equals(type.asText())) {
							stats.edges_emitted++;
							// Grab both edges from both ends:
							return Stream.concat(
									Optional.ofNullable(o.get(GraphAnnotationBean.inV)).map(k -> Stream.of(Tuples._3T(convertToObject(k, config), o, false))).orElse(Stream.empty()), 
									Optional.ofNullable(o.get(GraphAnnotationBean.outV)).map(k -> Stream.of(Tuples._3T(convertToObject(k, config), o, false))).orElse(Stream.empty()));
						}
						else if (GraphAnnotationBean.ElementType.vertex.toString().equals(type.asText())) {
							stats.vertices_emitted++;
							return Optional.ofNullable(o.get(GraphAnnotationBean.id)).map(k -> Stream.of(Tuples._3T(convertToObject(k, config), o, true))).orElse(Stream.empty());
						}
						else return Stream.empty();
					})
					.collect(Collectors.groupingBy(t3 -> t3._1() // group by key
							,
							Collectors.collectingAndThen(
									Collectors.<Tuple3<ObjectNode, ObjectNode, Boolean>>partitioningBy(t3 -> t3._3()) // group by edge/vertex
										,
										m -> Tuples._2T(m.get(true).stream().map(t3 -> t3._2()).collect(Collectors.toList()) // convert group edge/vertex to pair of lists
														, 
														m.get(false).stream().map(t3 -> t3._2()).collect(Collectors.toList()))
							)))
							;

		return nodes_to_get;
	}
	
	/** Previously our edges were double-grouped by in and out keys - now we can group them by the (in/out) pair
	 * @param key
	 * @param vertex_winner
	 * @param mutable_edges
	 * @return
	 */
	protected static Map<ObjectNode, List<ObjectNode>> finalEdgeGrouping(
			final ObjectNode key,
			final Vertex vertex_winner,
			final List<ObjectNode> mutable_edges
			)
	{
		final Map<ObjectNode, List<ObjectNode>> grouped_edges = mutable_edges.stream().filter(mutable_edge -> {
			
			final JsonNode in_key = mutable_edge.get(GraphAnnotationBean.inV);
			final JsonNode out_key = mutable_edge.get(GraphAnnotationBean.outV);
			
			final JsonNode matching_key = in_key.equals(key) // (has to be one of the 2 by construction)
											? in_key 
											: out_key; 
			final JsonNode off_key = !in_key.equals(key) // (doesn't match this key)
										? in_key 
										: (!out_key.equals(key) ? out_key : null); // (a vertex can have an edge be to itself)
			
			if (null == off_key) {
				mutable_edge.put(GraphAnnotationBean.inV, (Long) vertex_winner.id());
				mutable_edge.put(GraphAnnotationBean.outV, (Long) vertex_winner.id());
				mutable_edge.put(GraphAnnotationBean.inVLabel, key); // (internal, see below)
				mutable_edge.put(GraphAnnotationBean.outVLabel, key); // (internal, see below)
			}
			else {
				mutable_edge.put((matching_key == in_key) ? GraphAnnotationBean.inV : GraphAnnotationBean.outV, (Long) vertex_winner.id());
				mutable_edge.put((matching_key == in_key) ? GraphAnnotationBean.inVLabel : GraphAnnotationBean.outVLabel, key); // (internal, see below)
			}
			
			return ((null == off_key) || off_key.isIntegralNumber());
		})
		.collect(Collectors.groupingBy((ObjectNode mutable_edge) -> { // These edges are "promoted" - can call the user merge on them
			final ObjectNode edge_key = 
					Optional.of(_mapper.createObjectNode())
												.map(o -> (ObjectNode) o.set(GraphAnnotationBean.label, mutable_edge.get(GraphAnnotationBean.label)))
												.map(o -> Optional.ofNullable(mutable_edge.remove(GraphAnnotationBean.inVLabel))
																	.map(k -> (ObjectNode) o.set(GraphAnnotationBean.inV, k))
																	.orElse(o)
												)
												.map(o -> Optional.ofNullable(mutable_edge.remove(GraphAnnotationBean.outVLabel))
																	.map(k -> (ObjectNode) o.set(GraphAnnotationBean.outV, k))
																	.orElse(o)
												)
												.get()
												;
			
			return edge_key;
		}));		
		return grouped_edges;
	}
	
	/** Calls user merge on the various possibly duplicate elements, and sorts out user responses
	 * @param tx
	 * @param config
	 * @param security_service
	 * @param logger
	 * @param maybe_merger
	 * @param titan_mapper
	 * @param element_type
	 * @param key
	 * @param new_elements
	 * @param existing_elements
	 * @return
	 */
	protected static <O extends Element> List<O> invokeUserMergeCode(
			final TitanTransaction tx,
			final GraphSchemaBean config,
			final Tuple2<String, ISecurityService> security_service,
			final Optional<IBucketLogger> logger,
			final Optional<Tuple2<IEnrichmentBatchModule, GraphMergeEnrichmentContext>> maybe_merger,
			final org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper titan_mapper,
			final MutableStatsBean mutable_stats
			,
			final Class<O> element_type,
			final String bucket_path,
			final ObjectNode key,
			final Collection<ObjectNode> new_elements,
			final Collection<O> existing_elements,
			final Map<ObjectNode, Vertex> mutable_existing_vertex_store			
			)
	{
		if (existing_elements.isEmpty() && (1 == new_elements.size()) && !config.custom_finalize_all_objects()) {
			
			return validateUserElement(new_elements.stream().findFirst().get(), config)
					.bind(el -> addGraphSON2Graph(bucket_path, key, el, mutable_existing_vertex_store, Collections.emptyMap(), tx, element_type, mutable_stats)) 
					.<List<O>>validation(
							fail -> {
								if (Vertex.class.isAssignableFrom(element_type)) mutable_stats.vertex_errors++; 
								else if (Edge.class.isAssignableFrom(element_type)) mutable_stats.edge_errors++;
								
								logger.ifPresent(l -> l.inefficientLog(Level.DEBUG, 
										BeanTemplateUtils.clone(fail)
											.with(BasicMessageBean::source, "GraphBuilderEnrichmentService")
											.with(BasicMessageBean::command, "system.onObjectBatch")
										.done()));
//(keep this here for c/p purposes .. if want to attach an expensive "details" object then could do that by copying the fields across one by one)								
//								logger.ifPresent(l -> l.log(Level.DEBUG,								
//										ErrorUtils.lazyBuildMessage(true, () -> "GraphBuilderEnrichmentService", 
//												() -> "system.onObjectBatch", 
//												() -> null, 
//												() -> ErrorUtils.get("MESSAGE", params),
//												() -> null)
//												));						
								
								return Collections.emptyList();
							},
							success -> Arrays.<O>asList(success)
							)
							;
		}
		else {	
			// (just gives me the elements indexed by their ids so we can get them back again later)
			// (we'll convert to string as a slightly inefficient way of ensuring the same code can handle both edge andv vertex cases)
			final Map<String, Optional<O>> mutable_existing_element_vs_id_store = 
					existing_elements.stream().collect(Collectors.toMap(e -> e.id().toString(), e -> Optional.of(e)));								
			
			return maybe_merger.<List<O>>map(merger -> {				
				
				final Stream<Tuple2<Long, IBatchRecord>> in_stream = 
						Stream.concat(
								new_elements.stream().map(j -> Tuples._2T(0L, new BatchRecordUtils.JsonBatchRecord(j))), 
								existing_elements.stream()
													.sorted((a, b) -> ((Long)a.id()).compareTo((Long)b.id())) // (ensure first found element has the lowest id)
													.map(v -> _mapper.convertValue(titan_mapper.convertValue(v, Map.class), JsonNode.class))
													.map(j -> Tuples._2T(0L, new BatchRecordUtils.InjectedJsonBatchRecord(j)))
						);

				merger._2().initializeMerge(element_type);
				
				merger._1().onObjectBatch(in_stream, Optional.of(new_elements.size()), Optional.of(key));
						
				return merger._2().getAndResetElementList()
							.stream()
							.map(o -> addGraphSON2Graph(bucket_path, key, o, mutable_existing_vertex_store, mutable_existing_element_vs_id_store, tx, element_type, mutable_stats))
							.<O>flatMap(v -> v.validation(
													fail -> {
														if (Vertex.class.isAssignableFrom(element_type)) mutable_stats.vertex_errors++; 
														else if (Edge.class.isAssignableFrom(element_type)) mutable_stats.edge_errors++;
														
														logger.ifPresent(l -> l.inefficientLog(Level.DEBUG, 
																BeanTemplateUtils.clone(fail)
																	.with(BasicMessageBean::source, "GraphBuilderEnrichmentService")
																	.with(BasicMessageBean::command, "system.onObjectBatch")
																.done()));
														
														return Stream.empty(); 
													},
													success -> Stream.of(success))
									)
							.collect(Collectors.toList())
							;
			})
			.orElse(Collections.emptyList())
			;
		}
	}
	
	/** Creates a Titan element (vertex/edge) from the (validated) graphSON and adds to the graph
	 * @param graphson_to_add
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected static <O extends Element> Validation<BasicMessageBean, O> addGraphSON2Graph(
			final String bucket_path,
			final JsonNode key,
			final JsonNode graphson_to_add, 
			final Map<ObjectNode, Vertex> mutable_existing_vertex_store,
			final Map<String, Optional<O>> mutable_existing_element_vs_id_store,
			final TitanTransaction tx, 
			Class<O> element_type, final MutableStatsBean mutable_stats)
	{
		final long now = new Date().getTime();
		if (Vertex.class.isAssignableFrom(element_type))  {
			
			// If it's an existing vertex obv don't want to create a new one...	
			final String vid = Optional.ofNullable(graphson_to_add.get(GraphAnnotationBean.id)).filter(id -> id.isIntegralNumber()).map(id -> id.asLong()).map(id -> id.toString()).orElse(null);
			final Vertex v = (Vertex) Optional.ofNullable(mutable_existing_element_vs_id_store.get(vid))
												.map(vv -> { // (update stats)
													mutable_stats.vertices_updated++;
													return vv;
												})
												.map(Optional::get)
												.orElseGet(() -> {
													mutable_stats.vertices_created++;
													return (O) tx.addVertex(graphson_to_add.get(GraphAnnotationBean.label).asText());
												});
			
			insertProperties(v, Optional.ofNullable((ObjectNode) graphson_to_add.get(GraphAnnotationBean.properties))); // (is object by construction, see previous validation) 
			v.property(Cardinality.set, GraphAnnotationBean.a2_p, bucket_path);
			
			// Create/update timestamps:
			v.property(GraphAnnotationBean.a2_tm, now);
			v.property(GraphAnnotationBean.a2_tc).orElseGet(() -> v.property(GraphAnnotationBean.a2_tc, now));
			
			return Validation.success((O) v);
		}
		else { // Edge
			final JsonNode inV = key.get(GraphAnnotationBean.inV);
			final JsonNode outV = key.get(GraphAnnotationBean.outV);
			
			// If it's an existing edge obv don't want to create a new one...			
			final String eid = Optional.ofNullable(graphson_to_add.get(GraphAnnotationBean.id)).map(id -> jsonNodeToObject(id)).map(id -> id.toString()).orElse(null);
			Optional<O> maybe_edge = 
					Optional.ofNullable(mutable_existing_element_vs_id_store.get(eid))
							.map(ee -> {
								mutable_stats.edges_updated++;
								return ee;								
							})
							.orElseGet(() -> {
								return Optional.ofNullable(mutable_existing_vertex_store.get(outV))
									.map(out_v -> Tuples._2T(out_v, mutable_existing_vertex_store.get(inV)))
									.filter(t2 -> null != t2._2())
									.map(t2 -> {
										mutable_stats.edges_created++;
										return (O) t2._1().addEdge(graphson_to_add.get(GraphAnnotationBean.label).asText(), t2._2());
									});
							})
					;

			return maybe_edge.<Validation<BasicMessageBean, O>>map(edge -> {
				
				// OK edge permissions are a bit less pleasant because it only supports a cardinality of 1 .. the user has to either
				// "piggy back" off an existing edge (but in that case it can be deleted if that bucket is deleted, edge queries might not locate it), 
				// or create their own (but can't just tag an existing one)
				// TODO (ALEPH-15): maybe later on can support space-separated lists up to a certain size together with Lucene indexing? (Or add a different property name?)
				insertProperties(edge, Optional.ofNullable((ObjectNode) graphson_to_add.get(GraphAnnotationBean.properties)));
				edge.property(GraphAnnotationBean.a2_p).orElseGet(() -> edge.property(GraphAnnotationBean.a2_p, bucket_path));
				
				// Create/update timestamps: (still create these, though they aren't indexed)
				edge.property(GraphAnnotationBean.a2_tm, now);
				edge.property(GraphAnnotationBean.a2_tc).orElseGet(() -> edge.property(GraphAnnotationBean.a2_tc, now));
				
				return Validation.success((O) edge);
			})
			.orElseGet(() -> {
				//(error stats updated in parent function)
				return Validation.fail(ErrorUtils.buildErrorMessage(TitanGraphBuildingUtils.class.getSimpleName(), "addGraphSON2Graph", ErrorUtils.MISSING_VERTEX_FOR_EDGE, inV, outV));
			})
			;
		}
	}	

	private static final Set<String> _RESERVED_PROPERTIES = 
			ImmutableSet.of(GraphAnnotationBean.a2_p, GraphAnnotationBean.a2_tm, GraphAnnotationBean.a2_tc, GraphAnnotationBean.a2_r);
	
	/** Tidy up duplicates created because of the lack of consistency in deduplication (+lack of upsert!)
	 * @param tx
	 * @param grouped_vertices
	 * @param mutable_stats_per_batch
	 */
	public static void mergeDuplicates(final TitanTransaction tx,
			final String bucket_path,
			final Map<JsonNode, List<Vertex>> grouped_vertices, 
			final MutableStatsBean mutable_stats_per_batch)
	{
		grouped_vertices.entrySet().stream().filter(kv -> !kv.getValue().isEmpty()).forEach(kv -> {
			
			final Stream<Vertex> vertices = kv.getValue().stream().sorted((a, b) -> postProcSortingMethod(a, b));
			final Iterator<Vertex> it = vertices.iterator();
			if (it.hasNext()) {
				final long matches_found = kv.getValue().size() - 1;
				mutable_stats_per_batch.vertex_matches_found += matches_found; //(#vertices)
				if (matches_found > 0) {
					mutable_stats_per_batch.vertices_updated++;//(#keys)
				}
				
				final Vertex merge_into = it.next();
				if (it.hasNext()) {
					mutable_stats_per_batch.vertices_updated++;					
				}
				it.forEachRemaining(v -> {
					// special case: add all buckets, update times etc
					Optionals.streamOf(v.properties(GraphAnnotationBean.a2_p), false).map(vp -> vp.value())
								.forEach(val -> merge_into.property(Cardinality.set, GraphAnnotationBean.a2_p, val));
					merge_into.property(GraphAnnotationBean.a2_tm, new Date().getTime());
					
					// copy vertex properties into the "merge_into" vertex
					Optionals.streamOf(v.properties(), false)
						.filter(vp -> !_RESERVED_PROPERTIES.contains(vp.key())) // (ie don't overwrite system properties)
						.forEach(vp -> merge_into.property(vp.key(), vp.value()));
					
					// OK edges are the difficult bit
					mergeEdges(bucket_path, Direction.IN, false, merge_into, v, mutable_stats_per_batch);
					mergeEdges(bucket_path, Direction.OUT, false, merge_into, v, mutable_stats_per_batch);
					
					// (finally remove this vertex)
					// (previously - commened out code, we just removed the bucket, but since we're trying to remove singletons, we'll always delete)
					//Optionals.streamOf(v.properties(GraphAnnotationBean.a2_p), false).filter(vp -> bucket_path.equals(vp.value())).forEach(vp -> vp.remove());
					//if (!v.properties(GraphAnnotationBean.a2_p).hasNext()) v.remove();
					v.remove();
				});
			}
			
		});
	}
	
	/** Merge edges from one vertex into another
	 * @param bucket_path
	 * @param dir
	 * @param delete_edges - if true then deletes the edges, always used false because deleting all the vertices
	 * @param merge_into
	 * @param merge_from
	 * @param mutable_stats_per_batch
	 */
	protected static void mergeEdges(final String bucket_path, final Direction dir, boolean delete_edges, final Vertex merge_into, final Vertex merge_from, final MutableStatsBean mutable_stats_per_batch) {
		// First get a map of other side:
		final Map<Object, Edge> merge_into_edges =
				Optionals.streamOf(merge_into.edges(dir), false).collect(Collectors.toMap(e -> e.inVertex() == merge_into ? e.outVertex().id() : e.inVertex().id(), e -> e));		
		
		// Now compare against the incoming edges:
		Optionals.streamOf(merge_from.edges(dir), false)
			.filter(edge -> Optionals.streamOf(edge.properties(GraphAnnotationBean.a2_p), false).anyMatch(ep -> bucket_path.equals(ep.value()))) // (belongs to me)
			.forEach(edge -> {
				mutable_stats_per_batch.edge_matches_found++;
								
				final Vertex other_end = (edge.inVertex() == merge_from) ? edge.outVertex() : edge.inVertex(); 
				
				//TRACE
				//System.out.println(dir + ": " + merge_into.label() + " - " + edge.label() + " - " + other_end.label());
				
				final Edge edge_merge_into = merge_into_edges.computeIfAbsent(other_end.id(), k -> {
					mutable_stats_per_batch.edges_updated++;
					final Edge new_edge = (Direction.OUT == dir) ? merge_into.addEdge(edge.label(), other_end) : other_end.addEdge(edge.label(), merge_into);
					new_edge.property(GraphAnnotationBean.a2_p, bucket_path);
					return new_edge;
				});
				// copy other edge properties into the "merge_into" edge
				Optionals.streamOf(edge.properties(), false)
					.filter(ep -> !_RESERVED_PROPERTIES.contains(ep.key())) // (ie don't overwrite system properties)
					.forEach(ep -> edge_merge_into.property(ep.key(), ep.value()));
				
				// Now finally remove the edge
				if (delete_edges) edge.remove();
			});
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////
	////////////////////////////////
	
	// UTILS - LOW LEVEL
	
	/** Creates a JSON object out of the designated vertex properties
	 * @param el
	 * @param fields
	 * @return
	 */
	protected static JsonNode getElementProperties(final Element el, final Collection<String> fields) {
		return fields.stream()
				.map(f -> Tuples._2T(f, el.property(f)))
				.filter(t2 -> (null != t2._2()) && t2._2().isPresent())
				.reduce(_mapper.createObjectNode(), (o, t2) -> insertIntoObjectNode(t2._1(), t2._2(), o), (o1, o2) -> o2)
				;
	}
	
	/** Inserts one of a few different of property into an edge or vertex
	 *  TODO (ALEPH-15): Doesn't currently handle a few things: eg multi cardinality, permissions 
	 * @param mutable_element
	 * @param maybe_props
	 */
	protected static void insertProperties(final Element mutable_element, final Optional<ObjectNode> maybe_props) {
		maybe_props.ifPresent(props -> {
			Optionals.streamOf(props.fields(), false).forEach(prop -> { 
				Optional.ofNullable(denestProperties(prop.getValue())).ifPresent(val -> 
					val.either(
							o -> {
								return mutable_element.property(prop.getKey(), o); 
							},
							oo -> {
								if (oo.length > 0) {
									Patterns.match(oo[0]).andAct() // (see jsonNodeToObject - these are the only possibilities)
										.when(String.class, __ -> mutable_element.property(prop.getKey(), Arrays.stream(oo).toArray(String[]::new)))
										.when(Long.class, __ -> mutable_element.property(prop.getKey(), Arrays.stream(oo).toArray(Long[]::new)))
										.when(Double.class, __ -> mutable_element.property(prop.getKey(), Arrays.stream(oo).toArray(Double[]::new)))
										.when(Boolean.class, __ -> mutable_element.property(prop.getKey(), Arrays.stream(oo).toArray(Boolean[]::new)))
										;
								}
								return null;
							}));
			});
		});
	}
	
	/** Recursive function to pull out potentially nested properties
	 * @param property
	 * @return
	 */
	protected static Either<Object, Object[]> denestProperties(final JsonNode property) {
		if (property.isArray()) { // (Arrays of objects (except length 1) are currently discarded here, no support for cardinality, see above)
			if (1 == property.size()) { // (this is what all the existing properties look like after conversion to graphson)
				return denestProperties(property.get(0));
			}
			else {
				return Either.right(Optionals.streamOf(property.iterator(), false).map(j -> jsonNodeToObject(j)).filter(j -> null != j).toArray());
			}
		}
		else if (property.isObject()) {
			return denestProperties(property.get(GraphAnnotationBean.value));
		}
		else {
			return Either.left(jsonNodeToObject(property));
		}
	}
	
	/** Inserts a single-valued vertex property into a JSON object
	 * @param f
	 * @param vp
	 * @param o
	 * @return
	 */
	protected static ObjectNode insertIntoObjectNode(final String f, final Property<Object> vp, final ObjectNode o) {
		return Patterns.match(vp.value()).<ObjectNode>andReturn()
			.when(String.class, s -> o.put(f, s))
			.when(Double.class, d -> o.put(f, d))
			.when(Integer.class, i -> o.put(f, i))
			.when(Long.class, i -> o.put(f, i))
			.when(Boolean.class, b -> o.put(f, b))
			.otherwise(__ -> o)
			;	
	}
	
	/** Inserts a single-valued vertex property into a JSON object
	 * @param f
	 * @param vp
	 * @param o
	 * @return
	 */
	protected static Object jsonNodeToObject(final JsonNode j) {
		return Patterns.match().<Object>andReturn()
			.when(() -> null == j, () -> j)
			.when(() -> j.isTextual(), () -> j.asText())
			.when(() -> j.isDouble(), () -> j.asDouble())
			.when(() -> j.isIntegralNumber(), () -> j.asLong())
			.when(() -> j.isBoolean(), () -> j.asBoolean())
			.otherwise(__ -> null)
			;
	}	
	
	/** Key can be a single value if there's only one value in the dedup context
	 * @param j
	 * @param config
	 * @return
	 */
	protected static ObjectNode convertToObject(final JsonNode j, final GraphSchemaBean config) {
		if (j.isObject()) {
			return (ObjectNode)j;
		}
		else {
			final ObjectNode o = _mapper.createObjectNode();
			o.set(config.deduplication_fields().stream().findFirst().get(), j);			
			return (ObjectNode) o;
		}
	}
	
	/** Checks a Gremlin vertex or edge label against a GraphSON one
	 * @param user_key
	 * @param candidate
	 * @return
	 */
	protected static boolean labelMatches(final ObjectNode user_key, final Element candidate) {
		return candidate.label().equals(user_key.get(GraphAnnotationBean.label).asText()); // (exists by construction)
	}
	
	/** Checks is a user is allowed to connect to this node
	 *  (also prevents any bucket from connecting to any other _test bucket_ ... apart from itself obv)
	 * @param this_bucket
	 * @param security_service
	 * @param element
	 * @return
	 */
	protected static boolean isAllowed(final String this_bucket, final Tuple2<String, ISecurityService> security_service, final Element element) {
		
		// (odd expression because allMatch/anyMatch have the wrong behavior when the stream is empty, ie want it to allow in those cases, no security applied)
		return !Optionals.streamOf(element.<String>properties(GraphAnnotationBean.a2_p), false)
					.filter(p -> { //(note p.value() will always exist since using "properties" ie will return empty iterable if not)
						// Cases: (1) always allow bucket of the same name
						//        (2) if not then can't start with "/aleph2_testing/"
						//        (3) security match						
						return	(this_bucket.equals(p.value())  // (1,2)
									? false // (always "pass"(==false) in these cases) 
									: 
									(this_bucket.startsWith(BucketUtils.TEST_BUCKET_PREFIX) || p.value().startsWith(BucketUtils.TEST_BUCKET_PREFIX))
											// (else "pass"(==false) unless either start with /aleph_testing
								)
								||
								!security_service._2().isUserPermitted(security_service._1(), buildPermission(p.value())); //(3)
					})
					.findFirst().isPresent();
	}
	
	/** See https://github.com/IKANOW/Aleph2/wiki/(Detailed-Design)-Aleph2-security-roles-and-permissions#built-in-permissions---core
	 * @return
	 */
	protected static String buildPermission(String bucket_path) {
		return ROLE_PREFIX + bucket_path.replace("/", ":"); 
				
	}
	
	private static String ROLE_PREFIX = DataBucketBean.class.getSimpleName() + ":" + ISecurityService.ACTION_READ_WRITE;
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////
	////////////////////////////////
	
	// UTILS - PUBLIC UTILS
	
	/** Quick/fast validation of user generated elements (vertices and edges)
	 * @param o
	 * @return
	 */
	public static Validation<BasicMessageBean, ObjectNode> validateUserElement(final ObjectNode o, GraphSchemaBean config) {
		// Same as validation for merged elements, and in addition:
		// - For vertices:
		//   - id is set and is consistent with dedup fields
		// - For edges
		//   - "properties" are valid, and don't have illegal permissions

		return validateMergedElement(o, config).bind(success -> {

			final JsonNode type = o.get(GraphAnnotationBean.type); // (exists and is valid by construction of validateMergedElement)
			
			final Stream<String> keys_to_check = Patterns.match(type.asText()).<Stream<String>>andReturn()
					.when(t -> GraphAnnotationBean.ElementType.edge.toString().equals(t), __ -> Stream.of(GraphAnnotationBean.inV, GraphAnnotationBean.outV))
					.otherwise(__ -> Stream.of(GraphAnnotationBean.id))
					;
			
			final Optional<BasicMessageBean> first_error = keys_to_check.<BasicMessageBean>map(k -> {
				final JsonNode key = o.get(k);
				
				if (null == key) {
					return ErrorUtils.buildErrorMessage("GraphBuilderEnrichmentService", "system.onObjectBatch", ErrorUtils.MISSING_OR_BADLY_FORMED_FIELD, k, "missing");					
				}
				else if (!key.isIntegralNumber() && !key.isObject()) {
					return ErrorUtils.buildErrorMessage("GraphBuilderEnrichmentService", "system.onObjectBatch", ErrorUtils.MISSING_OR_BADLY_FORMED_FIELD, k, "not_long_or_object");					
				}
				
				if (key.isObject()) { // user specified id, check its format is valid					
					if (!config.deduplication_fields().stream().allMatch(f -> key.has(f))) {
						return ErrorUtils.buildErrorMessage("GraphBuilderEnrichmentService", "system.onObjectBatch", ErrorUtils.MISSING_OR_BADLY_FORMED_FIELD, k, "missing_key_subfield");
					}
				}
				return null;
			})
			.filter(b -> null != b)
			.findFirst();
			
			return first_error.<Validation<BasicMessageBean, ObjectNode>>map(b -> Validation.fail(b)).orElseGet(() -> Validation.success(o));
		});
	}
	
	/** Quick/fast validation of user chosen elements from the merge list
	 * @param o
	 * @return
	 */
	public static Validation<BasicMessageBean, ObjectNode> validateMergedElement(final ObjectNode o, GraphSchemaBean config) {
		// Some basic validation: 
		// - is "type" either edge or vertex
		// - "label" is set
		// - properties are valid 
		// - (for edges _don't_ to validate inV/outV because all that's already been chosen and can't be chosen at this point .. ditto vertices and ids)

		// Type is set to one of edge/vertex
		{
			final JsonNode type = o.get(GraphAnnotationBean.type);
			if (null == type) {
				return Validation.fail(ErrorUtils.buildErrorMessage("GraphBuilderEnrichmentService", "system.onObjectBatch", ErrorUtils.MISSING_OR_BADLY_FORMED_FIELD, GraphAnnotationBean.type, "missing"));
			}
			else if (!type.isTextual()) {
				return Validation.fail(ErrorUtils.buildErrorMessage("GraphBuilderEnrichmentService", "system.onObjectBatch", ErrorUtils.MISSING_OR_BADLY_FORMED_FIELD, GraphAnnotationBean.type, "not_text"));			
			}
			else if (!GraphAnnotationBean.ElementType.edge.toString().equals(type.asText()) && !GraphAnnotationBean.ElementType.vertex.toString().equals(type.asText())) {
				return Validation.fail(ErrorUtils.buildErrorMessage("GraphBuilderEnrichmentService", "system.onObjectBatch", ErrorUtils.MISSING_OR_BADLY_FORMED_FIELD, GraphAnnotationBean.type, "not_edge_or_vertex"));						
			}
		}
		// Label is set
		{
			final JsonNode label = o.get(GraphAnnotationBean.label);
			if (null == label) {
				return Validation.fail(ErrorUtils.buildErrorMessage("GraphBuilderEnrichmentService", "system.onObjectBatch", ErrorUtils.MISSING_OR_BADLY_FORMED_FIELD, GraphAnnotationBean.label, "missing"));
			}
			else if (!label.isTextual()) {
				return Validation.fail(ErrorUtils.buildErrorMessage("GraphBuilderEnrichmentService", "system.onObjectBatch", ErrorUtils.MISSING_OR_BADLY_FORMED_FIELD, GraphAnnotationBean.label, "not_text"));			
			}
		}				
		// Sort out properties
		
		final JsonNode properties = o.get(GraphAnnotationBean.properties);
		if (null != properties) { // (properties is optional)
			//TODO (ALEPH-15): sort out properties later (TODO including permissions, or does that happen elsewhere?)
			// - Check if is an object
			// - Loop over fields:
			//   - check each value is an array or an object (replace with a nested array if an object)
			//     - (if it's atomic, convert into an object of the right sort?)
			//   - for each value in the array, check it's an object (see above mutation step)
			//   - check it's value is one of the supported types
			//   - if it has a properties field, check it is an object of string/string
			//   - (Will just discard _b so can ignore): TODO: sort out setting it elsewhere
			
			if (!properties.isObject()) {
				return Validation.fail(ErrorUtils.buildErrorMessage("GraphBuilderEnrichmentService", "system.onObjectBatch", ErrorUtils.MISSING_OR_BADLY_FORMED_FIELD, GraphAnnotationBean.properties, "not_object"));							
			}
		}
		
		return Validation.success(o);
	}
	
	/** Util to sort vertices (as part of the cleanup node merge), so that any vertices with >1 bucket has highest prio, then sort by id
	 * @param v1
	 * @param v2
	 * @return
	 */
	private static int postProcSortingMethod(final Vertex v1, final Vertex v2) {
		final boolean v1_has_extra_buckets = Optionals.streamOf(v1.properties(GraphAnnotationBean.a2_p), false).skip(1).findFirst().isPresent();
		final boolean v2_has_extra_buckets = Optionals.streamOf(v2.properties(GraphAnnotationBean.a2_p), false).skip(1).findFirst().isPresent();
		if (v1_has_extra_buckets && !v2_has_extra_buckets) {
			return -1; // (v1 "wins" ie is lower)
		}
		else if (!v1_has_extra_buckets && v2_has_extra_buckets) {
			return 1; // (v2 wins)			
		}
		else return ((Long)v1.id()).compareTo((Long)v2.id()); // (same, order by id)
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////
	////////////////////////////////

	// Some example TinkerPop3 objects
	
	// see that the edges are in and out of the vertices vs having the _type (and the properties are duplicated...)
//	{"id":4216,"label":"test2","inE":{"test_v1_v2":[{"id":"1zm-39s-4r9-394","outV":4240,"properties":{"edge_prop":"edge_prop_val"}}]}}
//	{"id":4240,"label":"test1","outE":{"test_v1_v2":[{"id":"1zm-39s-4r9-394","inV":4216,"properties":{"edge_prop":"edge_prop_val"}}]},"properties":{"protected":[{"id":"1le-39s-2dh","value":"by_me","properties":{"test_meta":"test_meta_value"}}],"unprotected":[{"id":"176-39s-1l1","value":"hai"}]}}

	// Here's the vertex/edge versions:
//	{"id":4216,"label":"test2"}
//	{"id":"1zm-39s-4r9-394","label":"test_v1_v2","type":"edge","inVLabel":"test2","outVLabel":"test1","inV":4216,"outV":4240,"properties":{"edge_prop":"edge_prop_val"}}
	
//	{"id":4240,"label":"test1","properties":{"protected":[{"id":"1le-39s-2dh","value":"by_me","properties":{"test_meta":"test_meta_value"}}],"unprotected":[{"id":"176-39s-1l1","value":"hai"}]}}
//	{"id":"1zm-39s-4r9-394","label":"test_v1_v2","type":"edge","inVLabel":"test2","outVLabel":"test1","inV":4216,"outV":4240,"properties":{"edge_prop":"edge_prop_val"}}
	
}
