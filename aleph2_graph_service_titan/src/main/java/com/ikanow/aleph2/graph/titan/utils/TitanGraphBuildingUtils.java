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

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import scala.Tuple2;
import scala.Tuple4;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
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
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;

/** Collection of utilities for building Titan graph elements from a batch of data objects
 * @author Alex
 *
 */
public class TitanGraphBuildingUtils {
	final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());

	/** TODO: need a property that says: ignore/overwrite/etc
	 *  Hmm one alternative is that the decomposition basically returns "here are the objects to query"...
	 *  ...then the query occurs
	 *  ...then the merge passes in all edges and you can mess about with them as you so desire?
	 *  What I _was_ going with was that you specify the graph that you want...
	 *  ...then any conflicts get sorted out in the merge 
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
		
		
		//https://github.com/tinkerpop/blueprints/wiki/GraphSON-Reader-and-Writer-Library
		// graphson .. basically can use complex objects
		// Vertex:
		// MANDATORY: "_type": "vertex"
		// MANDATORY: "_id": if string ... then converts across "aleph2 id" ie "name"/"type" .. or could have like "_id": [ "name", "type" ] (or even put this in the options)
		// OPTIONAL: <property>: string/val/array or { "type": <name>, "value": <val>, "_meta": { "_b": set of bucket paths ACL, ... } }
		// (standard defaults: name/type)
		
		// Edge:
		// MANDATORY: "_type": "edge"
		// MANDATORY: "_label"
		// MANDATORY: "_inV" ... specify minimal set of attributes, eg { "name", "type": }
		// MANDATORY: "_outV" ... ditto
		// OPTIONAL: "weight"
		// OPTIONAL: <property>: string/val/array 
		
		// (can't set) "_b": set of bucket paths
		
		// { "_type": "vertex", "_a2id": "Alex Blah/type", "robbery": "x", "say_what": [ "a", "b" ], "complex": { "property": "blah", "metadata": { "bucket": "/blah" } }
		// { "_type": "edge", "_inV", "Alex Blah/type", "_outV": "ip-addr/IP", 
		
		// Get a list of all the vertices
		//titan.query().
		
	}
	
	////////////////////////////////
	
	//TODO: need to figure out what meta properties look like ASAP 
	
	//TODO; ugh ok this was Tinkerpop2 ... Tinkerpop3 is a bit different:

	// see that the edges are in and out of the vertices vs having the _type (and the properties are duplicated...)
//	{"id":4216,"label":"test2","inE":{"test_v1_v2":[{"id":"1zm-39s-4r9-394","outV":4240,"properties":{"edge_prop":"edge_prop_val"}}]}}
//	{"id":4240,"label":"test1","outE":{"test_v1_v2":[{"id":"1zm-39s-4r9-394","inV":4216,"properties":{"edge_prop":"edge_prop_val"}}]},"properties":{"protected":[{"id":"1le-39s-2dh","value":"by_me","properties":{"test_meta":"test_meta_value"}}],"unprotected":[{"id":"176-39s-1l1","value":"hai"}]}}

	// Here's the vertex/edge versions:
//	{"id":4216,"label":"test2"}
//	{"id":"1zm-39s-4r9-394","label":"test_v1_v2","type":"edge","inVLabel":"test2","outVLabel":"test1","inV":4216,"outV":4240,"properties":{"edge_prop":"edge_prop_val"}}
	
//	{"id":4240,"label":"test1","properties":{"protected":[{"id":"1le-39s-2dh","value":"by_me","properties":{"test_meta":"test_meta_value"}}],"unprotected":[{"id":"176-39s-1l1","value":"hai"}]}}
//	{"id":"1zm-39s-4r9-394","label":"test_v1_v2","type":"edge","inVLabel":"test2","outVLabel":"test1","inV":4216,"outV":4240,"properties":{"edge_prop":"edge_prop_val"}}
	
	//TODO: need to see what sorts of things are pulled in
	
	public static void handleMerge(
				final List<Tuple4<JsonNode, List<JsonNode>, List<JsonNode>, List<Tuple2<TitanVertex, JsonNode>>>> mergeable
			)
	{
		
		// 1) First step is to sort out the _vertices_, here's the cases:
		
		// 1.1) If there's no matching vertices then create a new vertex and get the id (via a merge if finalize is set)
		//      (overwrite the _id then map to a Vertex)
		
		// 1.2) If there are >0 matching vertices then we run a merge in which the user "has to do" the following:
		// 1.2.a) pick the winning vertex (or emit the current one to create a "duplicate node"?) TODO: have a policy setting as to whether to allow that, if not then discard
		// TODO: have a policy setting as to whether to delete the others, if allowed....
		// 1.2.b) copy any properties from the original objects into the winner and remove any so-desired properties
		
		// 2) By here we have a list of vertices and we've mutated the edges to fill in the _inV and _outV
		
	}
	
	
	////////////////////////////////
	
	// UTILS
	
	/** Creates a JSON object out of the designated vertex properties
	 * @param v
	 * @param fields
	 * @return
	 */
	public static JsonNode getVertexProperties(final Vertex v, final Collection<String> fields) {
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
	private static ObjectNode insertIntoObjectNode(final String f, final VertexProperty<Object> vp, final ObjectNode o) {
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
