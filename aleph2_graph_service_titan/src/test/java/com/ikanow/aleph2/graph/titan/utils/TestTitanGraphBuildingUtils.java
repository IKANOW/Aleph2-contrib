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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.graphdb.vertices.CacheVertex;
import com.thinkaurelius.titan.graphdb.vertices.StandardVertex;

/**
 * @author Alex
 *
 */
public class TestTitanGraphBuildingUtils {

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
		//final CacheVertex v1 = m.convertValue(s1, CacheVertex.class);
		//final StandardVertex v1 = m.convertValue(m.readTree(s1), StandardVertex.class);		
		final Vertex read_vertex1 = titan.io(IoCore.graphson()).reader().create().readVertex(new ByteArrayInputStream(s1.getBytes(StandardCharsets.UTF_8)), v -> v.get());
		System.out.println("read: " + m.convertValue(read_vertex1, JsonNode.class).toString());
		// (note the properties _have_ to be that complicated)
	}
	
	
	
	//TODO: don't forget to check what happens if you mess around with concurrent transactions (should fail, retry and run again)
	
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
		
		//TODO (some other vertices)
		
		tx.commit();
	}
	
	
	
}
