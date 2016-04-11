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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import scala.Tuple2;
import scala.Tuple4;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.objects.data_import.GraphAnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.graph.titan.services.GraphMergeEnrichmentContext;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TransactionBuilder;
import com.thinkaurelius.titan.core.attribute.Contain;

import fj.data.Validation;

/** Collection of utilities for building Titan graph elements from a batch of data objects
 * @author Alex
 *
 */
public class TitanGraphBuildingUtils {
	final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());

	/** TODO: need to decompose this even further 
	 * @param titan
	 * @param vertices
	 * @param maybe_merger
	 */
	@SuppressWarnings("unchecked")
	public static void buildGraph(final Collection<String> dedup_fields, final TitanGraph titan, List<JsonNode> vertices_and_edges, Optional<GraphMergeEnrichmentContext> maybe_merger) {
		final TransactionBuilder tx_b = titan.buildTransaction();
		final TitanTransaction tx = tx_b.start();
		
		// Convert the list of vertexes into a mega query - will have a false positive rate to keep the query simple  
		
		final Map<JsonNode, Tuple2<List<JsonNode>, List<JsonNode>>> nodes_to_get = null; //TODO build this should be a map<JsonNode, collection<JsonNode>>
		final Map<String, Set<Object>> dedup_query_builder = null; //TODO build
		
		//TODO: hmm this needs to include destination vertices also? using hte outV in the vertices
		// (i guess i shouldn't allow any outVs that don't have vertices ... or in fact in that case I should auto create the empty vertex?
		//  that way if you don't have a strong opinion about the vertex properties then you can just go ahead and create the edges....) 
		
		final TitanGraphQuery<?> matching_nodes_query = dedup_query_builder.entrySet().stream()
			.reduce(tx.query()
					,
					(query, kv) -> query.has(kv.getKey(), Contain.IN, kv.getValue())
					,
					(query1, query2) -> query1 // (can't occur
					);		
		
		// Remove false positives and group by key
		
		//TODO (handle property ACL?!)
		
		final Map<JsonNode, List<Tuple2<TitanVertex, JsonNode>>> grouped_vertices = 
				Optionals.streamOf(matching_nodes_query.vertices(), false)
					.map(vertex -> Tuples._2T(vertex, getVertexProperties(vertex, dedup_fields)))
					.filter(vertex_key -> nodes_to_get.keySet().contains(vertex_key._2())) // (remove false positives)
					.collect(Collectors.groupingBy(t2 -> (JsonNode) t2._2())) // (group by key)
					;
		
		// Match up the user vertices with the ones obtained from the system
		
		nodes_to_get.entrySet().stream()
			.map(kv -> Tuples._4T(kv.getKey(), kv.getValue()._1(), kv.getValue()._2(), grouped_vertices.getOrDefault(kv.getKey(), Collections.emptyList())))
			;
		
	}
	
	/** Another big function that needs to be massively decomposed/lots more args/etc
	 * @param mergeable - first param is key, second is list of vertices, third is list of edges, forth is list of (existing vertices, (key again - ignore))
	 */
	public static void handleMerge(
				final TitanTransaction tx,
				final GraphSchemaBean config,
				final ISecurityService security_service,
				final IBucketLogger logger,
				final Optional<GraphMergeEnrichmentContext> maybe_merger,
				final Stream<Tuple4<JsonNode, List<ObjectNode>, List<ObjectNode>, List<Tuple2<TitanVertex, JsonNode>>>> mergeable
			)
	{		
		mergeable.forEach(t4 -> { // TODO: convert this into a non-nested pipeline with a map/flatMap here

			final JsonNode key = t4._1();
			final List<ObjectNode> vertices = t4._2();
			final List<ObjectNode> edges = t4._3();
			final List<Tuple2<TitanVertex, JsonNode>> existing_vertices = t4._4();
			
			// 1) First step is to sort out the _vertices_, here's the cases:
			
			// 1.1) If there's no matching vertices then create a new vertex and get the id (via a merge if finalize is set)
			//      (overwrite the _id then map to a Vertex)
			// 1.2) If there are >0 matching vertices (and only one incoming vertex) then we run a merge in which the user "has to do" the following:
			// 1.2.a) pick the winning vertex (or emit the current one to create a "duplicate node"?)
			// 1.2.a.1) (Allow user to delete the others if he has permission, by the usual emit "id" only - but don't automatically do it because it gets complicated what to do with the other _bs)
			// 1.2.b) copy any properties from the original objects into the winner and remove any so-desired properties

			//TODO: map this to Vertex in this block
			final Optional<Vertex> maybe_vertex_winner = Lambdas.get(() -> {
				if (existing_vertices.isEmpty() && (1 == vertices.size()) && !config.custom_finalize_all_objects()) {
					
					return mutatePartialGraphSON(vertices.stream().findFirst().get())
							.bind(vertex -> addGraphSON2Graph(vertex, tx))
							.validation(
									fail -> {
										//TODO log error to DEBUG										
										return Optional.empty();
									},
									success -> Optional.of(success)
									)
									;
				}
				else {					
					return maybe_merger.map(merger -> {						
						//TODO: build stream, call maybe_merger.map.onObjectBatch, tidy up results
						//TODO: handle deletes
						return null; //TODO
					})
					;
				}
			});
			maybe_vertex_winner.ifPresent(vertex_winner -> {

				// 1.3) Add winning vertex to the graph

				//tx.add
				
				//TODO (add test to check when the vertex gets an "id")
				
				// 1.4) Tidy up (mutate) the edges				
				
				edges.stream().forEach(mutable_edge -> {
					
					final JsonNode in_key = mutable_edge.get(GraphAnnotationBean.inV);
					final JsonNode out_key = mutable_edge.get(GraphAnnotationBean.outV);
					
					final JsonNode matching_key = (in_key == key) ? in_key : out_key; // (has to be one of the 2 by construction)
					final JsonNode off_key = (in_key != key) ? in_key : (out_key != key ? out_key : null); // (a vertex can have an edge be to itself)
					
					mutable_edge.put( (matching_key == in_key) ? GraphAnnotationBean.inV : GraphAnnotationBean.outV , (Long) vertex_winner.id() );
					
					// 1.4.1) Once we've tidied up the edge, check if we're ready for that edge to move to stage 2):
					
					if ((null == off_key) || off_key.isLong()) {
						
						//TODO: this is a bit nested, change this to an optional pipeline...
						
						// 2) By here we have a list of vertices and we've mutated the edges to fill in the _inV and _outV
						// 2.1) Now get the potentially matching edges from each of the selected vertices:
						// 2.1.1) If there's no matching edges (and only one incoming edge) then create a new edge (via a merge if finalize is set)
						// 2.1.2) If there are >0 matching edges then run a merge against the edges, pick the current one
						
						//X) ok one thing I haven't considered here is the bucket situation:
						// ... filter any buckets over which the user does not have read permission
						// if the user attempts to modify a bucket over which he does not have write permission then ..? (TODO: silently fail? or create a new edge in that case)
						// remove any properties of any vertex/edge over which the user does not have read permission .. and then re-combine later
						// the user can manipulate the _bs, but can only _add_, never remove  
						// And then also add the bucket path to _b everywhere
						
					}
					
				});
				
				//TODO
			});
				
		});
		//TODO: also if in test mode then don't allow any link merging, everything stays completely standalone
	}
	
	
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
	
	////////////////////////////////
	
	// UTILS
	
	/**
	 * @param in
	 * @return
	 */
	protected static Validation<BasicMessageBean, JsonNode> mutatePartialGraphSON(final JsonNode in) {
		return null; //TODO
	}
	
	/**
	 * @param in
	 * @return
	 */
	protected static Validation<BasicMessageBean, Vertex> addGraphSON2Graph(final JsonNode in, final TitanTransaction tx) {
		return null; //TODO
	}
	
	/** Creates a JSON object out of the designated vertex properties
	 * @param v
	 * @param fields
	 * @return
	 */
	protected static JsonNode getVertexProperties(final Vertex v, final Collection<String> fields) {
		return fields.stream()
				.map(f -> Tuples._2T(f, v.property(f)))
				.filter(t2 -> null != t2._2())
				.reduce(_mapper.createObjectNode(), (o, t2) -> insertIntoObjectNode(t2._1(), t2._2(), o), (o1, o2) -> o2)
				;
	}
	
	/** Inserts a single-valued vertex property into a JSON object
	 * @param f
	 * @param vp
	 * @param o
	 * @return
	 */
	protected static ObjectNode insertIntoObjectNode(final String f, final VertexProperty<Object> vp, final ObjectNode o) {
		return Patterns.match(vp.value()).<ObjectNode>andReturn()
			.when(String.class, s -> o.put(f, s))
			.when(Double.class, d -> o.put(f, d))
			.when(Integer.class, i -> o.put(f, i))
			.when(Long.class, i -> o.put(f, i))
			.when(Boolean.class, b -> o.put(f, b))
			.otherwise(__ -> o)
			;
		
	}
	
}
