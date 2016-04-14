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
import java.util.Optional;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockSecurityService;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.GraphAnnotationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;

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
	
	// (Test utilities)
	
	protected TitanGraph getSimpleTitanGraph() {
		final TitanGraph titan = TitanFactory.build().set("storage.backend", "inmemory").open();
		return titan;		
	}
	
	
	
}
