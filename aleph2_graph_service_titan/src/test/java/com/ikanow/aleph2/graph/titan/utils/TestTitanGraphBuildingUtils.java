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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.junit.Test;

import scala.Tuple2;
import scala.Tuple4;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockSecurityService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.GraphAnnotationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.graph.titan.services.GraphDecompEnrichmentContext;
import com.ikanow.aleph2.graph.titan.services.GraphMergeEnrichmentContext;
import com.ikanow.aleph2.graph.titan.services.SimpleGraphDecompService;
import com.ikanow.aleph2.graph.titan.services.SimpleGraphMergeService;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.schema.TitanManagement;

import fj.data.Validation;

/**
 * @author Alex
 *
 */
public class TestTitanGraphBuildingUtils {
	final protected static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////
	////////////////////////////////
	
	// UTILS - PUBLIC UTILS
	
	@Test
	public void test_validateMergedElement() {
		
		// Graph schema (currently ignored)
		final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class).done().get();
		
		// Valid object:
		final ObjectNode base_test = _mapper.createObjectNode();
		base_test.put(GraphAnnotationBean.label, "test_label");
		base_test.put(GraphAnnotationBean.type, GraphAnnotationBean.ElementType.vertex.toString());
		
		// 1) Check it validates:
		{
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateMergedElement(base_test, graph_schema);
			assertTrue("Should be valid: " + ret_val.validation(fail -> fail.message(), success -> ""), ret_val.isSuccess());
			assertEquals("Should return the inserted object: " + ret_val.success() + " vs " + base_test, base_test.toString(), ret_val.success().toString());
		}
		// 2) Couple of other valid incarnations:
		{
			final ObjectNode test = base_test.deepCopy();
			test.put(GraphAnnotationBean.label, "test_label");
			test.put(GraphAnnotationBean.type, GraphAnnotationBean.ElementType.edge.toString());
			test.put(GraphAnnotationBean.properties, _mapper.createObjectNode());
			
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateMergedElement(test, graph_schema);
			assertTrue("Should be valid: " + ret_val.validation(fail -> fail.message(), success -> ""), ret_val.isSuccess());
			assertEquals("Should return the inserted object: " + ret_val.success() + " vs " + test, test.toString(), ret_val.success().toString());
		}
		
		// 3.1) No type:
		{			
			final ObjectNode test = base_test.deepCopy();
			test.remove(GraphAnnotationBean.type);
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateMergedElement(test, graph_schema);
			assertFalse("Should be invalid", ret_val.isSuccess());
		}
		// 3.2) Type not a string
		{			
			final ObjectNode test = base_test.deepCopy();
			test.put(GraphAnnotationBean.type, 4L);
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateMergedElement(test, graph_schema);
			assertFalse("Should be invalid", ret_val.isSuccess());
		}
		// 3.3) Type the wrong string
		{
			final ObjectNode test = base_test.deepCopy();
			test.put(GraphAnnotationBean.type, "banana");
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateMergedElement(test, graph_schema);
			assertFalse("Should be invalid", ret_val.isSuccess());			
		}
		// 4.1) No label:
		{			
			final ObjectNode test = base_test.deepCopy();
			test.remove(GraphAnnotationBean.label);
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateMergedElement(test, graph_schema);
			assertFalse("Should be invalid", ret_val.isSuccess());
		}
		// 4.2) label not a string
		{			
			final ObjectNode test = base_test.deepCopy();
			test.put(GraphAnnotationBean.label, 4L);
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateMergedElement(test, graph_schema);
			assertFalse("Should be invalid", ret_val.isSuccess());
		}
		// 5) Properties not an object
		{			
			final ObjectNode test = base_test.deepCopy();
			test.put(GraphAnnotationBean.properties, 4L);
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateMergedElement(test, graph_schema);
			assertFalse("Should be invalid", ret_val.isSuccess());
		}
	}

	@Test
	public void test_validateUserElement() {
				
		// Graph schema
		final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class)
					.with(GraphSchemaBean::deduplication_fields, Arrays.asList(GraphAnnotationBean.name, GraphAnnotationBean.type))
				.done().get();
		
		// Valid object:
		final ObjectNode base_test = _mapper.createObjectNode();
		base_test.put(GraphAnnotationBean.label, "test_label");
		base_test.put(GraphAnnotationBean.type, GraphAnnotationBean.ElementType.vertex.toString());
		base_test.put(GraphAnnotationBean.id, 0L);
		
		// 1.1) Check it validates:
		{
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateUserElement(base_test, graph_schema);
			assertTrue("Should be valid: " + ret_val.validation(fail -> fail.message(), success -> ""), ret_val.isSuccess());
			assertEquals("Should return the inserted object: " + ret_val.success() + " vs " + base_test, base_test.toString(), ret_val.success().toString());
		}
		// 1.2) Other valid incarnations
		{
			final ObjectNode test = base_test.deepCopy();
			test.put(GraphAnnotationBean.id, _mapper.createObjectNode().put("name", "a").put("type", "b"));
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateUserElement(test, graph_schema);
			assertEquals("Should return the inserted object: " + ret_val.success() + " vs " + test, test.toString(), ret_val.success().toString());			
		}
		//(edge)
		{
			final ObjectNode test = base_test.deepCopy();
			test.remove(GraphAnnotationBean.id);
			test.put(GraphAnnotationBean.type, GraphAnnotationBean.ElementType.edge.toString());
			test.put(GraphAnnotationBean.inV, _mapper.createObjectNode().put("name", "a").put("type", "b"));
			test.put(GraphAnnotationBean.outV, 0L);
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateUserElement(test, graph_schema);
			assertEquals("Should return the inserted object: " + ret_val.success() + " vs " + test, test.toString(), ret_val.success().toString());			
		}
		// 2) Check fails out if the "validateMergedElement" bit fails
		{
			final ObjectNode test = base_test.deepCopy();
			test.put(GraphAnnotationBean.type, "rabbit");
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateUserElement(test, graph_schema);
			assertFalse("Should be invalid", ret_val.isSuccess());			
		}
		// 3.1) id not present
		{
			final ObjectNode test = base_test.deepCopy();
			test.remove(GraphAnnotationBean.id);
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateUserElement(test, graph_schema);
			assertFalse("Should be invalid", ret_val.isSuccess());			
		}
		// 3.2) id not a long/object
		{
			final ObjectNode test = base_test.deepCopy();
			test.put(GraphAnnotationBean.id, 1.5);
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateUserElement(test, graph_schema);
			assertFalse("Should be invalid", ret_val.isSuccess());			
		}
		// 3.3) id is an object but doesn't match the dedup fields
		{
			final ObjectNode test = base_test.deepCopy();
			test.put(GraphAnnotationBean.type, GraphAnnotationBean.ElementType.edge.toString());
			test.put(GraphAnnotationBean.inV, _mapper.createObjectNode().put("name", "a"));
			test.put(GraphAnnotationBean.outV, 0L);
			final Validation<BasicMessageBean, ObjectNode> ret_val = TitanGraphBuildingUtils.validateUserElement(test, graph_schema);
			assertFalse("Should be invalid", ret_val.isSuccess());			
		}
		
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////
	////////////////////////////////
	
	// UTILS - LOW LEVEL 
	
	@Test
	public void test_getElementProperties() {
		// Graph schema
		final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class)
					.with(GraphSchemaBean::deduplication_fields, Arrays.asList(GraphAnnotationBean.name, GraphAnnotationBean.type))
				.done().get();
		
		final TitanGraph titan = getSimpleTitanGraph();
		final TitanTransaction tx = titan.buildTransaction().start();

		Vertex v = tx.addVertex("test");
		v.property("name", "name_val");
		v.property("type", "type_val");
		v.property("misc", "misc_val");
		final JsonNode result = TitanGraphBuildingUtils.getElementProperties(v, graph_schema.deduplication_fields());
		
		final ObjectNode key = _mapper.createObjectNode();
		key.put("name", "name_val");
		key.put("type", "type_val");
		
		assertEquals(key.toString(), result.toString());
		
		tx.commit();
	}
	
	@Test
	public void test_insertIntoObjectNode() {
		final TitanGraph titan = getSimpleTitanGraph();
		final TitanTransaction tx = titan.buildTransaction().start();

		Vertex v = tx.addVertex("test");
		v.property("long", 3L);
		v.property("integer", 3);
		v.property("boolean", true);
		v.property("double", 3.1);
		v.property("str", "string");
		
		final ObjectNode graphson = _mapper.createObjectNode();
		
		TitanGraphBuildingUtils.insertIntoObjectNode("long", v.property("long"), graphson);
		TitanGraphBuildingUtils.insertIntoObjectNode("integer", v.property("integer"), graphson);
		TitanGraphBuildingUtils.insertIntoObjectNode("boolean", v.property("boolean"), graphson);
		TitanGraphBuildingUtils.insertIntoObjectNode("double", v.property("double"), graphson);
		TitanGraphBuildingUtils.insertIntoObjectNode("str", v.property("str"), graphson);

		final ObjectNode user_graphson = _mapper.createObjectNode();
		user_graphson.put("long", 3L);
		user_graphson.put("integer", 3);
		user_graphson.put("boolean", true);
		user_graphson.put("double", 3.1);
		user_graphson.put("str", "string");

		assertEquals(graphson.toString(), user_graphson.toString());
		
		tx.commit();
	}
	
	@Test
	public void test_jsonNodeToObject() throws JsonProcessingException, IOException {
		assertEquals(3L, ((Number)TitanGraphBuildingUtils.jsonNodeToObject(_mapper.readTree("3"))).longValue());
		assertEquals(3.1, ((Double)TitanGraphBuildingUtils.jsonNodeToObject(_mapper.readTree("3.1"))).doubleValue(), 0.00000001);
		assertEquals(true, ((Boolean)TitanGraphBuildingUtils.jsonNodeToObject(_mapper.readTree("true"))).booleanValue());
		assertEquals("alex", TitanGraphBuildingUtils.jsonNodeToObject(new TextNode("alex")));
	}
	
	@Test
	public void test_convertToObject() {
	
		// Graph schema
		final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class)
					.with(GraphSchemaBean::deduplication_fields, Arrays.asList(GraphAnnotationBean.name, GraphAnnotationBean.type))
				.done().get();
		
		final ObjectNode graphson = _mapper.createObjectNode().put("name", "alex");
		
		assertEquals(graphson, TitanGraphBuildingUtils.convertToObject(graphson, graph_schema));
		assertEquals(graphson.toString(), TitanGraphBuildingUtils.convertToObject(new TextNode("alex"), graph_schema).toString());
	}
	
	@Test
	public void test_labelMatches() {
		final TitanGraph titan = getSimpleTitanGraph();
		final TitanTransaction tx = titan.buildTransaction().start();

		final Vertex test_vertex = tx.addVertex("test_vertex");				
		{
			final ObjectNode graphson = _mapper.createObjectNode().put("label", "test_vertex");
			assertTrue(TitanGraphBuildingUtils.labelMatches(graphson, test_vertex));
		}
		{
			final ObjectNode graphson = _mapper.createObjectNode().put("label", "test_vertex2");
			assertFalse(TitanGraphBuildingUtils.labelMatches(graphson, test_vertex));
		}		
		tx.commit();		
	}
	
	@Test
	public void test_isAllowed() {
		final MockSecurityService mock_security_service = new MockSecurityService();
		final TitanGraph titan = getSimpleTitanGraph();
		final TitanTransaction tx = titan.buildTransaction().start();
		// No permissions
		{
			final Vertex test_vertex = tx.addVertex("test_vertex");		
			assertTrue(TitanGraphBuildingUtils.isAllowed(Tuples._2T("nobody", mock_security_service), test_vertex));
		}
		// Fails/succeeds based on permission
		{
			final Vertex test_vertex = tx.addVertex("test_vertex");		
			test_vertex.property(GraphAnnotationBean.a2_p, "/test");
			assertFalse(TitanGraphBuildingUtils.isAllowed(Tuples._2T("nobody", mock_security_service), test_vertex));			

			mock_security_service.setGlobalMockRole("nobody:DataBucketBean:read,write:test:*", true);
			assertTrue(TitanGraphBuildingUtils.isAllowed(Tuples._2T("nobody", mock_security_service), test_vertex));			
		}
		// Just double check with edge...
		{
			final Vertex test_edge = tx.addVertex("test_vertex");		
			test_edge.property(GraphAnnotationBean.a2_p, "/test");
			assertTrue(TitanGraphBuildingUtils.isAllowed(Tuples._2T("nobody", mock_security_service), test_edge));			
			
		}
		tx.commit();		
	}
	
	@Test
	public void test_buildPermission() {
		assertEquals("DataBucketBean:read,write:alex:test", TitanGraphBuildingUtils.buildPermission("/alex/test"));
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////
	////////////////////////////////
	
	// UTILS - HIGH LEVEL

	@Test
	public void test_groupNewEdgesAndVertices() {
		
		// Graph schema
		final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class)
					.with(GraphSchemaBean::deduplication_fields, Arrays.asList(GraphAnnotationBean.name, GraphAnnotationBean.type))
				.done().get();

		final ObjectNode key1 = _mapper.createObjectNode()
									.put(GraphAnnotationBean.name, "1.1.1.1")
									.put(GraphAnnotationBean.type, "ip-addr");

		final ObjectNode key2 = _mapper.createObjectNode()
									.put(GraphAnnotationBean.name, "host.com")
									.put(GraphAnnotationBean.type, "domain-name");
		
		
		final List<ObjectNode> test_vertices_and_edges = Arrays.asList(
				(ObjectNode) _mapper.createObjectNode()
					.put(GraphAnnotationBean.type, GraphAnnotationBean.ElementType.vertex.toString())
					.put(GraphAnnotationBean.label, "1.1.1.1")
					.set(GraphAnnotationBean.id, key1
							)
				,
				(ObjectNode) _mapper.createObjectNode() //(invalid node, no type)
					.put(GraphAnnotationBean.label, "1.1.1.2")
					.set(GraphAnnotationBean.id, _mapper.createObjectNode()
													.put(GraphAnnotationBean.name, "1.1.1.2")
													.put(GraphAnnotationBean.type, "ip-addr")
							)
				,
				(ObjectNode) _mapper.createObjectNode()
					.put(GraphAnnotationBean.label, "host.com")
					.put(GraphAnnotationBean.type, GraphAnnotationBean.ElementType.vertex.toString())
					.set(GraphAnnotationBean.id, key2
						)
				,
				(ObjectNode) ((ObjectNode) _mapper.createObjectNode()
					.put(GraphAnnotationBean.type, GraphAnnotationBean.ElementType.edge.toString())
					.put(GraphAnnotationBean.label, "dns-connection")
					.set(GraphAnnotationBean.inV, _mapper.createObjectNode()
													.put(GraphAnnotationBean.name, "host.com")
													.put(GraphAnnotationBean.type, "domain-name")
							))
					.set(GraphAnnotationBean.outV, _mapper.createObjectNode()
													.put(GraphAnnotationBean.name, "1.1.1.1")
													.put(GraphAnnotationBean.type, "ip-addr")
							)
				);
				
		final Map<ObjectNode, Tuple2<List<ObjectNode>, List<ObjectNode>>> ret_val =
				TitanGraphBuildingUtils.groupNewEdgesAndVertices(graph_schema, test_vertices_and_edges.stream());
		
		
		assertEquals(2, ret_val.keySet().size());
		
		final Tuple2<List<ObjectNode>, List<ObjectNode>> ret_val_1 = ret_val.getOrDefault(key1, Tuples._2T(Collections.emptyList(), Collections.emptyList()));		
		final Tuple2<List<ObjectNode>, List<ObjectNode>> ret_val_2 = ret_val.getOrDefault(key2, Tuples._2T(Collections.emptyList(), Collections.emptyList()));
		
		assertEquals(1, ret_val_1._1().size());
		assertEquals("1.1.1.1", ret_val_1._1().get(0).get(GraphAnnotationBean.label).asText());
		assertEquals(1, ret_val_1._2().size());
		assertEquals("dns-connection", ret_val_1._2().get(0).get(GraphAnnotationBean.label).asText());
		assertEquals(1, ret_val_2._1().size());
		assertEquals("host.com", ret_val_2._1().get(0).get(GraphAnnotationBean.label).asText());
		assertEquals(1, ret_val_2._2().size());
		assertEquals("dns-connection", ret_val_2._2().get(0).get(GraphAnnotationBean.label).asText());
	}
	
	@Test
	public void test_finalEdgeGrouping() {
		final TitanGraph titan = getSimpleTitanGraph();
		final TitanTransaction tx = titan.buildTransaction().start();
		
		final Vertex v1 = tx.addVertex("1.1.1.1");
		final Vertex v2 = tx.addVertex("host.com");
		
		final ObjectNode key1 = _mapper.createObjectNode()
				.put(GraphAnnotationBean.name, "1.1.1.1")
				.put(GraphAnnotationBean.type, "ip-addr");

		final ObjectNode key2 = _mapper.createObjectNode()
				.put(GraphAnnotationBean.name, "host.com")
				.put(GraphAnnotationBean.type, "domain-name");

		final List<ObjectNode> mutable_edges = Arrays.asList(
				(ObjectNode) ((ObjectNode) _mapper.createObjectNode()
						.put(GraphAnnotationBean.type, GraphAnnotationBean.ElementType.edge.toString())
						.put(GraphAnnotationBean.label, "dns-connection")
						.set(GraphAnnotationBean.inV, _mapper.createObjectNode()
														.put(GraphAnnotationBean.name, "host.com")
														.put(GraphAnnotationBean.type, "domain-name")
								))
						.set(GraphAnnotationBean.outV, _mapper.createObjectNode()
														.put(GraphAnnotationBean.name, "1.1.1.1")
														.put(GraphAnnotationBean.type, "ip-addr")
								)
								,
				(ObjectNode) ((ObjectNode) _mapper.createObjectNode()
						.put(GraphAnnotationBean.type, GraphAnnotationBean.ElementType.edge.toString())
						.put(GraphAnnotationBean.label, "self-connect")
						.set(GraphAnnotationBean.inV, _mapper.createObjectNode()
														.put(GraphAnnotationBean.name, "1.1.1.1")
														.put(GraphAnnotationBean.type, "ip-addr")
								))
						.set(GraphAnnotationBean.outV, _mapper.createObjectNode()
														.put(GraphAnnotationBean.name, "1.1.1.1")
														.put(GraphAnnotationBean.type, "ip-addr")
								)
				);

		// Some other handy objects
		final List<ObjectNode> dup_mutable_edges = mutable_edges.stream().map(o -> o.deepCopy()).collect(Collectors.toList());
		final ObjectNode self_link = (ObjectNode) ((ObjectNode) _mapper.createObjectNode().set(GraphAnnotationBean.inV, key1)).set(GraphAnnotationBean.outV, key1);
		final ObjectNode normal_link = (ObjectNode) ((ObjectNode) _mapper.createObjectNode().set(GraphAnnotationBean.inV, key2)).set(GraphAnnotationBean.outV, key1);		
		
		// First pass, should grab the self-connector
		{
			final Map<ObjectNode, List<ObjectNode>> ret_val_pass = TitanGraphBuildingUtils.finalEdgeGrouping(key1, v1, mutable_edges);
			
			// First pass through, snags the edge that points to itself
			assertEquals(1, ret_val_pass.size());
			assertEquals(1, ret_val_pass.getOrDefault(self_link, Collections.emptyList()).size());
			final ObjectNode val = ret_val_pass.get(self_link).get(0);
			assertEquals("self-connect", val.get(GraphAnnotationBean.label).asText());
			assertEquals(v1.id(), val.get(GraphAnnotationBean.inV).asLong());
			assertEquals(v1.id(), val.get(GraphAnnotationBean.outV).asLong());
		}
		// Second pass, connects the other edge up
		{
			final Map<ObjectNode, List<ObjectNode>> ret_val_pass = TitanGraphBuildingUtils.finalEdgeGrouping(key2, v2, mutable_edges);
			
			// First pass through, snags the edge that points to itself
			assertEquals(2, ret_val_pass.size());
			assertEquals("Should hav enormal key: " + ret_val_pass.keySet() + " vs " + normal_link, 1, ret_val_pass.getOrDefault(normal_link, Collections.emptyList()).size());
			final ObjectNode val = ret_val_pass.get(normal_link).get(0);
			assertEquals("dns-connection", val.get(GraphAnnotationBean.label).asText());
			assertEquals(v2.id(), val.get(GraphAnnotationBean.inV).asLong());
			assertEquals(v1.id(), val.get(GraphAnnotationBean.outV).asLong());
		}
		
		// Another first pass, but with the other key so nothing emits
		{
			final Map<ObjectNode, List<ObjectNode>> ret_val_pass = TitanGraphBuildingUtils.finalEdgeGrouping(key2, v2, dup_mutable_edges);
			assertEquals(0, ret_val_pass.size());
		}
		
		tx.commit();
	}
	
	@Test
	public void test_invokeUserMergeCode() {
		// This has a massive set of params, here we go:
		final TitanGraph titan = getSimpleTitanGraph();
		final TitanTransaction tx = titan.buildTransaction().start();
		// Graph schema
		final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class)
					.with(GraphSchemaBean::deduplication_fields, Arrays.asList(GraphAnnotationBean.name, GraphAnnotationBean.type))
				.done().get();
		// Security service
		final MockSecurityService mock_security = new MockSecurityService();
		final String user = "nobody";
		//TODO: add param to make this work
		// Simple merger
		final Optional<Tuple2<IEnrichmentBatchModule, GraphMergeEnrichmentContext>> maybe_merger = 
				Optional.of(
						Tuples._2T((IEnrichmentBatchModule) new SimpleGraphMergeService(),
									new GraphMergeEnrichmentContext(null, graph_schema)
						));
		maybe_merger.ifPresent(t2 -> t2._1().onStageInitialize(t2._2(), null, null, null, null));
		// special titan mapper
		final ObjectMapper titan_mapper = titan.io(IoCore.graphson()).mapper().create().createMapper();
		// Bucket
		final String bucket_path = "/test/security";
		// Key
		final ObjectNode key = _mapper.createObjectNode().put(GraphAnnotationBean.name, "alex").put(GraphAnnotationBean.type, "person"); 
		// New elements
		//TODO
		final List<ObjectNode> new_vertices = Arrays.asList();
		final List<ObjectNode> new_edges = Arrays.asList();
		// Existing elements
		//TODO
		final List<Tuple2<Vertex, JsonNode>> existing_vertices = Arrays.asList();
		final List<Tuple2<Edge, JsonNode>> existing_edges = Arrays.asList();
		// State
		//TODO
		final Map<JsonNode, Vertex> mutable_existing_vertex_store = new HashMap<>();
		
		// This is a huge one...
		// Vertices
		{
			@SuppressWarnings("unused")
			List<Vertex> v = 
					TitanGraphBuildingUtils.invokeUserMergeCode(
							tx, graph_schema, Tuples._2T(user, mock_security), Optional.empty(), 
							maybe_merger, titan_mapper, Vertex.class, bucket_path, key, new_vertices, existing_vertices, mutable_existing_vertex_store);
			
			//TODO: test results
		}
		// Edges
		{
			@SuppressWarnings("unused")
			List<Edge> e = 
					TitanGraphBuildingUtils.invokeUserMergeCode(
							tx, graph_schema, Tuples._2T(user, mock_security), Optional.empty(), 
							maybe_merger, titan_mapper, Edge.class, bucket_path, key, new_edges, existing_edges, mutable_existing_vertex_store);			
			
			//TODO: test results
		}
		//TODO any edge cases to test?
	}
	
	@Test
	public void test_addGraphSON2Graph() {		
		final String bucket_path = "/test/tagged";
		// Titan
		final TitanGraph titan = getSimpleTitanGraph();
		final TitanTransaction tx = titan.buildTransaction().start();
		TitanManagement mgmt = titan.openManagement();		
		mgmt.makePropertyKey(GraphAnnotationBean.a2_p).dataType(String.class).cardinality(Cardinality.SET).make();
		mgmt.commit();
		// Key
		final ObjectNode key = _mapper.createObjectNode().put(GraphAnnotationBean.name, "alex").put(GraphAnnotationBean.type, "person"); 
		// State
		//TODO
		final Map<JsonNode, Vertex> mutable_existing_vertex_store = new HashMap<>();
		// GraphSON to add:
		final ObjectNode vertex_to_add = _mapper.createObjectNode().put(GraphAnnotationBean.label, "test_vertex");
		final ObjectNode edge_to_add = _mapper.createObjectNode().put(GraphAnnotationBean.label, "test_edge");
		
		{
			@SuppressWarnings("unused")
			Validation<BasicMessageBean, Vertex> ret_val = 
					TitanGraphBuildingUtils.addGraphSON2Graph(bucket_path, key, vertex_to_add, mutable_existing_vertex_store, tx, Vertex.class);
		}
		{
			@SuppressWarnings("unused")
			Validation<BasicMessageBean, Edge> ret_val = 
					TitanGraphBuildingUtils.addGraphSON2Graph(bucket_path, key, edge_to_add, mutable_existing_vertex_store, tx, Edge.class);
		}
		
		tx.commit();
		
		//TODO validate graph
	}

	//TODO: make sure the test case not connecting anything is handled
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////
	////////////////////////////////
	
	// UTILS - TOP LEVEL LOGIC

	@Test
	public void test_buildGraph_getUserGeneratedAssets() {
		
		// (this is a pretty simple method so will use to test the decomposing service also)
		
		final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class)
					.with(GraphSchemaBean::deduplication_fields, Arrays.asList(GraphAnnotationBean.name, GraphAnnotationBean.type))
				.done().get();
		
		final Stream<Tuple2<Long, IBatchRecord>> batch = Stream.empty();
		
		final Optional<Tuple2<IEnrichmentBatchModule, GraphDecompEnrichmentContext>> maybe_decomposer = 
				Optional.of(
						Tuples._2T((IEnrichmentBatchModule) new SimpleGraphDecompService(),
									new GraphDecompEnrichmentContext(null, graph_schema)
						));
		//TODO
		//maybe_decomposer.ifPresent(t2 -> t2._1().onStageInitialize(t2._2(), null, null, null, null));
		
		@SuppressWarnings("unused")
		final List<ObjectNode> ret_val = TitanGraphBuildingUtils.buildGraph_getUserGeneratedAssets(
				batch, Optional.empty(), Optional.empty(), maybe_decomposer
				);
		
		//TODO
	}
	
	@Test
	public void test_buildGraph_collectUserGeneratedAssets() {
		// Titan
		final TitanGraph titan = getSimpleTitanGraph();
		final TitanTransaction tx = titan.buildTransaction().start();
		// Graph schema
		final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class)
				.with(GraphSchemaBean::deduplication_fields, Arrays.asList(GraphAnnotationBean.name, GraphAnnotationBean.type))
			.done().get();
		// Security service
		final MockSecurityService mock_security = new MockSecurityService();
		final String user = "nobody";
		//TODO: set permission
		// Bucket
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/security")
				//TODO: probably need something else as well?
				.done().get();
		// Stream of objects (TODO: eg get from test_buildGraph_getUserGeneratedAssets)
		final Stream<ObjectNode> vertices_and_edges = Stream.empty();
		
		@SuppressWarnings("unused")
		final Stream<Tuple4<ObjectNode, List<ObjectNode>, List<ObjectNode>, List<Tuple2<Vertex, JsonNode>>>> ret_val = 
				TitanGraphBuildingUtils.buildGraph_collectUserGeneratedAssets(tx, graph_schema, Tuples._2T(user, mock_security), Optional.empty(), bucket, vertices_and_edges)
				;
		//TODO		
		
		tx.commit();
	}
	
	@Test
	public void test_buildGraph_handleMerge() {
		// Titan
		final TitanGraph titan = getSimpleTitanGraph();
		final TitanTransaction tx = titan.buildTransaction().start();
		// Graph schema
		final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class)
				.with(GraphSchemaBean::deduplication_fields, Arrays.asList(GraphAnnotationBean.name, GraphAnnotationBean.type))
			.done().get();
		// Security service
		final MockSecurityService mock_security = new MockSecurityService();
		final String user = "nobody";
		//TODO: set permission
		// Bucket
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/security")
				//TODO: probably need something else as well?
				.done().get();
		// Simple merger
		final Optional<Tuple2<IEnrichmentBatchModule, GraphMergeEnrichmentContext>> maybe_merger = 
				Optional.of(
						Tuples._2T((IEnrichmentBatchModule) new SimpleGraphMergeService(),
									new GraphMergeEnrichmentContext(null, graph_schema)
						));
		maybe_merger.ifPresent(t2 -> t2._1().onStageInitialize(t2._2(), null, null, null, null));
		// Input TODO
		final Stream<Tuple4<ObjectNode, List<ObjectNode>, List<ObjectNode>, List<Tuple2<Vertex, JsonNode>>>> mergeable = Stream.empty();
		
		{
			TitanGraphBuildingUtils.buildGraph_handleMerge(tx, graph_schema, Tuples._2T(user, mock_security), Optional.empty(), maybe_merger, bucket, mergeable);
			//TODO		
		}
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////
	////////////////////////////////
	
	// (Test utilities)
	
	protected TitanGraph getSimpleTitanGraph() {
		final TitanGraph titan = TitanFactory.build().set("storage.backend", "inmemory").open();
		return titan;		
	}
}
