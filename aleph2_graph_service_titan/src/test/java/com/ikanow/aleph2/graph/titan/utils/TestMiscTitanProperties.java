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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.Cardinality;

import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.core.schema.TitanManagement;

/**
 * @author Alex
 *
 */
public class TestMiscTitanProperties {

	protected static String showGraph(final TitanGraph titan) throws IOException {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		titan.io(IoCore.graphson()).writer().create().writeGraph(baos, titan);		
		return baos.toString();		
	}
	protected static String showElement(final TitanGraph titan, final Element element) throws IOException {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		if (Vertex.class.isAssignableFrom(element.getClass())) {
			titan.io(IoCore.graphson()).writer().create().writeVertex(baos, (Vertex) element);
		}
		else {
			titan.io(IoCore.graphson()).writer().create().writeEdge(baos, (Edge) element);			
		}
		return baos.toString();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test_elementProperties() throws IOException, InterruptedException {
		
		try {
			FileUtils.deleteDirectory(new File(System.getProperty("java.io.tmpdir") + "/titan-test"));
		}
		catch (Exception e) {}
		
		TitanGraph titan = TitanFactory.build()
				.set("storage.backend", "inmemory")
				.set("index.search.backend", "elasticsearch")
				.set("index.search.elasticsearch.local-mode", true)
				.set("index.search.directory", System.getProperty("java.io.tmpdir") + "/titan-test")
				.set("index.search.elasticsearch.client-only", false)
				//.set("query.force-index", true) //(disabled for testing)
			.open();
		
		{
			TitanManagement mgmt = titan.openManagement();
			// Without ES:
			//mgmt.makePropertyKey("paths").dataType(String.class).cardinality(Cardinality.SET).make();
			//with ES as a search back-end, can do SET/LIST
			mgmt.buildIndex("pathQuery", Vertex.class).addKey(mgmt.makePropertyKey("paths").dataType(String.class).cardinality(Cardinality.SET).make()).buildMixedIndex("search");
			//.addKey("_b", Mapping.STRING.asParameter()).buildMixedIndex("search")
			mgmt.commit();
		}
		
		// Just check I can do this multiple times:
		try {
			TitanManagement mgmt = titan.openManagement();
			mgmt.makePropertyKey("paths").dataType(String.class).cardinality(Cardinality.SET).make();
			mgmt.commit();		
		}
		catch (SchemaViolationException e) {} // (can but throws this exception, which is fine)
		
		buildSmallGraph(titan);
		
		final TitanTransaction tx = titan.buildTransaction().start();
		
		final TitanVertex v = Optionals.<TitanVertex>streamOf(tx.query().has("type", "rabbit").vertices(), false).findFirst().get();
		
		// These will fail because the property has not been declared
//		v.property(org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.set, "animal", "mouse");
//		v.property(org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.set, "animal", "cat");
//		v.property(org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.set, "animal", "mouse");
		// This all works as expected
		v.property(org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.set, "paths", "mouse");
		v.property(org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.set, "paths", "cat");
		v.property(org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.set, "paths", "mouse");
		// These will overwrite
//		v.property("_b", "mouse");
//		v.property("_b", "cat");
//		v.property("_b", "mouse");
		
		// This does not work, so properties will be protected with a single bucket access 
		v.property("type", "rabbit", "paths", Arrays.asList("a", "b")); //"[a, b]" (ie a string of a list)
		//v.property("type", "rabbit", "_b", Stream.of("a", "b").toArray()); // [L;Object]
		
		System.out.println(showElement(titan, v));
		tx.commit();

		// OK let's double check how we retrieve a list of properties
		
		final TitanVertex v2 = Optionals.<TitanVertex>streamOf(titan.query().has("type", "rabbit").vertices(), false).findFirst().get();		
		
		// Use "properties" with a single key to get the list of buckets
		System.out.println("paths = " + Optionals.streamOf(v2.properties("_b"), false).map(vp -> vp.value().toString()).collect(Collectors.joining(";")));
		
		// Double check a query on _b works
		assertEquals(1L, Optionals.streamOf(titan.query().has("paths", "cat").vertices(), false).count());
		Thread.sleep(3000L); // (wait for ES to complete)
		//assertEquals(1L, Optionals.streamOf(titan.indexQuery("pathQuery", "v.paths:cat").vertices(), false).count());
		assertEquals(1L, Optionals.streamOf(titan.indexQuery("pathQuery", "v.paths:(rabbit cat)").vertices(), false).count());
		
		//TODO: what about a joint property + bucket query?
		//TODO: need to figure out how to handle analyzed vs non-analyzed, I think it's TEXT vs STRING?
		// https://groups.google.com/forum/#!topic/aureliusgraphs/VGv-RJwt8zI
		// graph.makeKey("name").dataType(String.class).indexed("search", Element.class, new Parameter[]{Parameter.of(Mapping.MAPPING_PREFIX,Mapping.STRING)}) .single().make();
	}
	
	
	@SuppressWarnings("unchecked")
	@Test
	public void test_someBasicGraphBehavior() throws IOException {
	
		// Test some basic properties and transferring to/from GraphSON
		
		TitanGraph titan = TitanFactory.build()
				.set("storage.backend", "inmemory")
				//.set("query.force-index", true) //(disabled for testing)
			.open();
		
		buildSmallGraph(titan);
		
		final TitanTransaction tx = titan.buildTransaction().start();
		
		tx.query().vertices().forEach(v -> System.out.println(v.keys() + " ... " + v.label() + " .. " + v.id()));

		{
			System.out.println("---- entire graph ------");
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			titan.io(IoCore.graphson()).writer().create().writeGraph(baos, titan);
			System.out.println(baos.toString());
		}

		
		System.out.println("---- per vertex ------");
		
		tx.query().vertices().forEach(Lambdas.wrap_consumer_i(v -> {
			{
				System.out.println("type = " + v.getClass().toString());
				final ByteArrayOutputStream baos = new ByteArrayOutputStream();
				titan.io(IoCore.graphson()).writer().create().writeVertex(baos, v);
				System.out.println(baos.toString());
			}		
			Optionals.streamOf(v.edges(Direction.BOTH), false).forEach(Lambdas.wrap_consumer_i(e -> {
				final ByteArrayOutputStream baos = new ByteArrayOutputStream();
				titan.io(IoCore.graphson()).writer().create().writeEdge(baos, e);
				System.out.println(baos.toString());				
			}));
		}));
		
		
		// OK here's a nice example of how to convert a vertex to JsonNode
		Optionals.streamOf(tx.query().vertices(), false).findFirst()
			.map(v -> titan.io(IoCore.graphson()).mapper().create().createMapper().convertValue(v, JsonNode.class))
			.ifPresent(j -> System.out.println("?? " + j.toString()));
		

		// Looking at how to import vertices ..  
		final String s1 = "{\"id\":4200,\"label\":\"test2\",\"type\":\"vertex\",\"properties\":{\"set\":[{\"id\":\"1l9-38o-5j9\",\"value\":[\"val1\",\"val2\"]}],\"type\":[{\"id\":\"171-38o-4qt\",\"value\":\"rabbit\"}]}}";
		final ObjectMapper m = titan.io(IoCore.graphson()).mapper().create().createMapper();
		// confirmation that can't go JsonNode -> vertex directly
		//final CacheVertex v1 = m.convertValue(s1, CacheVertex.class);
		//final StandardVertex v1 = m.convertValue(m.readTree(s1), StandardVertex.class);		
		//m.convertValue(m.readTree(s1), org.apache.tinkerpop.gremlin.structure.util.star.StarGraph.StarVertex.class);
		// But string -> vertex does work
		final Vertex read_vertex1 = titan.io(IoCore.graphson()).reader().create().readVertex(new ByteArrayInputStream(s1.getBytes(StandardCharsets.UTF_8)), v -> v.get());
		System.out.println("read: " + read_vertex1.getClass().toString() + ": " + m.convertValue(read_vertex1, JsonNode.class).toString());
		// (note the properties _have_ to be that complicated)
			
		
		System.out.println("---- property query ------");
		
		Optionals.streamOf(tx.query().has("type", "rabbit").vertices(), false).findFirst()
			.map(v -> titan.io(IoCore.graphson()).mapper().create().createMapper().convertValue(v, JsonNode.class))
			.ifPresent(j -> System.out.println("?? " + j.toString()));
		
		System.out.println("---- label query, returns nothing (can query edge labels, not vertex labels) ------");
			
		Optionals.streamOf(tx.query().has("label", "test2").vertices(), false).findFirst()
			.map(v -> titan.io(IoCore.graphson()).mapper().create().createMapper().convertValue(v, JsonNode.class))
			.ifPresent(j -> System.out.println("?? " + j.toString()));
	}
	
	@Test
	public void test_someGraphErrors() throws IOException {
	
		// Test some graph errors
		
		TitanGraph titan = TitanFactory.build()
				.set("storage.backend", "inmemory")
				.set("query.force-index", true) //(disabled for testing)
			.open();
		
		buildSmallGraph(titan);
		
		// 1) Check what happens if you try to do a non-indexed query:
		
		try {
			Optionals.streamOf(titan.query().vertices(), false).findFirst();
		}
		catch (TitanException e) {
			System.out.println("Threw expected titan exception: " + e.getClass().toString() + " / cause = " + e.getCause());
		}
		
		// 2) Play around with indexes
		// I THINK THIS DOESN'T WORK BECAUSE INDEXES AREN'T SUPPORTED IN THIS VERSION OF TITAN...
		
		// This fails because type isn't unique:
		try {
			titan.openManagement().makePropertyKey("type").dataType(String.class).make();
		}
		catch (com.thinkaurelius.titan.core.SchemaViolationException e) {
			System.out.println("Threw expected titan exception: " + e.getClass().toString() + " / cause = " + e.getCause());		
		}
		try {
			final TitanManagement mgmt = titan.openManagement();
			final PropertyKey type_key = mgmt.getPropertyKey("type");
			mgmt.buildIndex("byType", Vertex.class).addKey(type_key).buildCompositeIndex();
			//(this fails, as you'd expect)
			//mgmt.updateIndex(mgmt.getGraphIndex("byType"), SchemaAction.REINDEX).get();
			mgmt.commit();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		// (Reindex existing data)
		try {
			for (int i = 0; i < 10; ++i) {
				com.thinkaurelius.titan.graphdb.database.management.ManagementSystem.awaitGraphIndexStatus(titan, "byType");
				
				final TitanManagement mgmt = titan.openManagement();
				final PropertyKey type_key = mgmt.getPropertyKey("type");
				final SchemaStatus status = mgmt.getGraphIndex("byType").getIndexStatus(type_key);
				if (status != SchemaStatus.INSTALLED) break;
				System.out.println(status.toString());
				Thread.sleep(250L);
			}
			{
				final TitanManagement mgmt = titan.openManagement();
				mgmt.updateIndex(mgmt.getGraphIndex("byType"), SchemaAction.REINDEX).get();
				mgmt.commit();
			}
		}
		catch (Exception e) {
			System.out.println("Threw expected titan exception: " + e.getClass().toString() + " / cause = " + e.getCause());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test_concurrentChanges_conflicting() throws IOException {
		
		// Test some graph errors
		
		TitanGraph titan = TitanFactory.build()
				.set("storage.backend", "inmemory")
				.set("query.force-index", false) //(not supported)
			.open();
		
		buildSmallGraph(titan);
		
		final Supplier<TitanTransaction> build_trans = () -> {
			final TitanTransaction tx = titan.buildTransaction().start();		
			Optionals.<Vertex>streamOf(tx.query().has("type", "rabbit").vertices(), false).forEach(v -> v.property("change", "something"));
			return tx;
		};
		final TitanTransaction tx1 = build_trans.get();
		
		final TitanTransaction tx2 = titan.buildTransaction().start();
		Optionals.<Vertex>streamOf(tx2.query().has("type", "rabbit").vertices(), false).forEach(v -> v.property("change", "something_else"));
		tx2.commit();
		
		try {
			tx1.commit();
		}
		catch (TitanException e) {
			System.out.println("Threw expected titan exception: " + e.getClass().toString() + " / cause = " + e.getCause() + " .. "  + e.getCause().getCause());
			assertEquals(com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException.class, e.getCause().getCause().getClass());
		}
		// Check can retry:
		build_trans.get().commit();
	}	
	
	@SuppressWarnings("unchecked")
	@Test
	public void test_concurrentChanges_nonConflicting() throws IOException {
		
		// Test some graph errors
		
		TitanGraph titan = TitanFactory.build()
				.set("storage.backend", "inmemory")
				.set("query.force-index", false) //(not supported)
			.open();
		
		buildSmallGraph(titan);
		
		final Supplier<TitanTransaction> build_trans = () -> {
			final TitanTransaction tx = titan.buildTransaction().start();		
			Optionals.<Vertex>streamOf(tx.query().has("type", "rabbit").vertices(), false).forEach(v -> v.property("change", "something"));
			return tx;
		};
		final TitanTransaction tx1 = build_trans.get();
		
		final TitanTransaction tx2 = titan.buildTransaction().start();
		Optionals.<Vertex>streamOf(tx2.query().hasNot("type", "rabbit").vertices(), false).forEach(v -> v.property("change", "something_else"));

		{
			System.out.println("---- entire graph ... tx1 ------");
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			titan.io(IoCore.graphson()).writer().create().writeGraph(baos, tx1);
			System.out.println(baos.toString());
		}
		{
			System.out.println("---- entire graph ... tx2 ------");
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			titan.io(IoCore.graphson()).writer().create().writeGraph(baos, tx2);
			System.out.println(baos.toString());
		}		
		
		tx2.commit();
		
		// I was expecting this to work, but it still fails ... might be an issue with the inmemory storage again?
		try {
			tx1.commit();
		}
		catch (TitanException e) {
			System.out.println("Threw expected titan exception: " + e.getClass().toString() + " / cause = " + e.getCause() + " .. "  + e.getCause().getCause());
			assertEquals(com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException.class, e.getCause().getCause().getClass());
		}
	}	
	
	//////////////////////////////////////
	
	public void buildSmallGraph(TitanGraph test) {
		
		final TitanTransaction tx = test.buildTransaction().start();
		
		final TitanVertex v1 = tx.addVertex("test1");
		v1.property("unprotected", "hai");
		v1.property("protected", "by_me", "test_meta", "test_meta_value");
		final TitanVertex v2 = tx.addVertex("test2");
		
		// Check ids are assigned immediately:
		System.out.println("Assigned vertices with ids " + Arrays.asList(v1.id(), v2.id()));
		assertTrue(Long.class.isAssignableFrom(v1.id().getClass()));
		assertTrue(Long.class.isAssignableFrom(v2.id().getClass()));
		
		// how to multiple?
		v2.property("type", "rabbit");
		v2.property("set", Arrays.asList("val1", "val2").toArray());
		final TitanEdge e1 = v1.addEdge("test_v1_v2", v2);
		e1.property("edge_prop", "edge_prop_val");
		
		tx.commit();
	}
	
	
	
}
