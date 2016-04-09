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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Supplier;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.thinkaurelius.titan.core.PropertyKey;
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

	@SuppressWarnings("unchecked")
	@Test
	public void test_someBasicGraphProperties() throws IOException {
	
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
		// how to multiple?
		v2.property("type", "rabbit");
		v2.property("set", Arrays.asList("val1", "val2").toArray());
		final TitanEdge e1 = v1.addEdge("test_v1_v2", v2);
		e1.property("edge_prop", "edge_prop_val");
		
		tx.commit();
	}
	
	
	
}
