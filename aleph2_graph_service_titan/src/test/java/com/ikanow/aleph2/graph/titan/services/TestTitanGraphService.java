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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.elasticsearch.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.codepoetics.protonpack.StreamUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.GraphAnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.graph.titan.data_model.TitanGraphConfigBean;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.typesafe.config.Config;

/**
 * @author Alex
 *
 */
public class TestTitanGraphService extends TestTitanCommon {

	@SuppressWarnings("unchecked")
	@Before
	public void setup() throws InterruptedException {
		super.setup();
		
		// Wipe anything existing in the graph
		final TitanTransaction tx = _titan.buildTransaction().start();
		Optionals.<TitanVertex>streamOf(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").vertices(), false).forEach(v -> v.remove());
		Optionals.<TitanEdge>streamOf(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").edges(), false).forEach(v -> v.remove());
		tx.commit();
		System.out.println("Sleeping while waiting to cleanse ES");
		Thread.sleep(2000L);
	}
	
	@Test
	public void test_getUnderlyingPlatformDriver() {
		
		assertEquals(Arrays.asList(_mock_graph_db_service), _mock_graph_db_service.getUnderlyingArtefacts());
		
		assertEquals(Optional.empty(), _mock_graph_db_service.getUnderlyingPlatformDriver(String.class, Optional.empty()));
		assertEquals(Optional.empty(), _mock_graph_db_service.getUnderlyingPlatformDriver(IEnrichmentBatchModule.class, Optional.empty()));
		assertEquals(Optional.empty(), _mock_graph_db_service.getUnderlyingPlatformDriver(IEnrichmentBatchModule.class, Optional.of("rabbit")));
		assertTrue(
				_mock_graph_db_service.getUnderlyingPlatformDriver(IEnrichmentBatchModule.class, Optional.of("com.ikanow.aleph2.analytics.services.GraphBuilderEnrichmentService")).map(b -> b.getClass())
				.filter(c -> TitanGraphBuilderEnrichmentService.class.isAssignableFrom(c)).isPresent());
	}
	
	@Test
	public void test_validateSchema() {

		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/validate/schema")
				.done().get();			
		
		// Errors:
		{
			final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class)
					.with(GraphSchemaBean::custom_decomposition_configs, Arrays.asList())
					.with(GraphSchemaBean::deduplication_fields, Arrays.asList("x"))
					.with(GraphSchemaBean::deduplication_contexts, Arrays.asList("/x"))
					.with(GraphSchemaBean::custom_decomposition_configs, Arrays.asList())
					.done().get();
			
			final Tuple2<String, List<BasicMessageBean>> l = _mock_graph_db_service.validateSchema(graph_schema, bucket);
			assertEquals(4, l._2().size());
		}
		// Works
		{
			final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get();
			final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class)
						.with(GraphSchemaBean::custom_decomposition_configs, Arrays.asList(control))
						.with(GraphSchemaBean::custom_merge_configs, Arrays.asList(control))
					.done().get();
			
			final Tuple2<String, List<BasicMessageBean>> l = _mock_graph_db_service.validateSchema(graph_schema, bucket);
			assertEquals(0, l._2().size());
		}
		
	}
	
	@Test
	public void test_onPublishOrUpdate() {
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/validate/schema")
			.done().get();			
	
		// do nothing
		{
			CompletableFuture<Collection<BasicMessageBean>> ret_val =
					_mock_graph_db_service.onPublishOrUpdate(bucket, Optional.empty(), false, Collections.emptySet(), Collections.emptySet());
			
			assertEquals(Collections.emptyList(), ret_val.join());
		}
		// create all the indexes
		{
			CompletableFuture<Collection<BasicMessageBean>> ret_val =
					_mock_graph_db_service.onPublishOrUpdate(bucket, Optional.empty(), false, ImmutableSet.of(GraphSchemaBean.name), Collections.emptySet());
			
			assertEquals(Collections.emptyList(), ret_val.join());
			
			// But also now check the Titan indexes have all been created:
			
			final TitanManagement mgmt = _titan.openManagement();
			Stream<TitanGraphIndex> v_indexes = StreamUtils.stream(mgmt.getGraphIndexes(Vertex.class));
			Stream<TitanGraphIndex> e_indexes = StreamUtils.stream(mgmt.getGraphIndexes(Edge.class));
			assertEquals(4L, v_indexes.count());
			assertEquals(1L, e_indexes.count());
		}
		// rerun to check it all works second+ time round
		{
			CompletableFuture<Collection<BasicMessageBean>> ret_val =
					_mock_graph_db_service.onPublishOrUpdate(bucket, Optional.empty(), false, ImmutableSet.of(GraphSchemaBean.name), Collections.emptySet());
			
			assertEquals("Should return no errors: " + ret_val.join().stream().map(b -> b.message()).collect(Collectors.joining(";")), Collections.emptyList(), ret_val.join());
			
			// But also now check the Titan indexes have all been created:
			
			final TitanManagement mgmt = _titan.openManagement();
			Stream<TitanGraphIndex> v_indexes = StreamUtils.stream(mgmt.getGraphIndexes(Vertex.class));
			Stream<TitanGraphIndex> e_indexes = StreamUtils.stream(mgmt.getGraphIndexes(Edge.class));
			assertEquals(4L, v_indexes.count());
			assertEquals(1L, e_indexes.count());
		}
		// Error if specifying deduplication fields
		{
			final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class)
					.with(GraphSchemaBean::custom_decomposition_configs, Arrays.asList())
					.with(GraphSchemaBean::deduplication_fields, Arrays.asList("nonempty"))
					.done().get();
			
			final DataBucketBean dedup_fields_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/on/publish")
					.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class)
									.with(DataSchemaBean::graph_schema, graph_schema)
									.done().get()
							)
				.done().get();			
			
			CompletableFuture<Collection<BasicMessageBean>> ret_val =
					_mock_graph_db_service.onPublishOrUpdate(dedup_fields_bucket, Optional.empty(), false, ImmutableSet.of(GraphSchemaBean.name), Collections.emptySet());
			
			assertEquals(1, ret_val.join().size());
			assertEquals(1, ret_val.join().stream().filter(b -> !b.success()).count());
		}
		
		//(See also test_handleBucketDeletionRequest, for some coverage testing of onPublishOrUpdate)
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test_handleBucketDeletionRequest() throws InterruptedException {
		final org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper titan_mapper = _titan.io(IoCore.graphson()).mapper().create().createMapper();
		
		// Ensure indexes exist
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/bucket/delete")
			.done().get();			
		
		_mock_graph_db_service.onPublishOrUpdate(bucket, Optional.empty(), false, ImmutableSet.of(GraphSchemaBean.name), Collections.emptySet()).join();		
		
		// Add a bunch of edges to delete
		
		final int N_OBJECTS = 300;
		{
			final TitanTransaction tx = _titan.buildTransaction().start();
			
			for (int ii = 0; ii < N_OBJECTS; ++ii) { // needs to be x6
				
				final Vertex v1 = tx.addVertex("test_del_A_" + ii);
				final Vertex v2 = tx.addVertex("test_del_B_" + ii);
				final Edge e1 = v1.addEdge("test_edge_" + ii, v2);
				if (0 == (ii % 3)) {
					v1.property(GraphAnnotationBean.a2_p, "/test/bucket/delete");
					v2.property(GraphAnnotationBean.a2_p, "/test/bucket/delete");
					e1.property(GraphAnnotationBean.a2_p, "/test/bucket/delete");
				}
				else if (1 == (ii % 3)) {
					v1.property(GraphAnnotationBean.a2_p, "/test/bucket/no_delete");
					v2.property(GraphAnnotationBean.a2_p, "/test/bucket/no_delete");				
					e1.property(GraphAnnotationBean.a2_p, "/test/bucket/no_delete");
				}
				else if (2 == (ii % 3)) {
					v1.property(GraphAnnotationBean.a2_p, "/test/bucket/delete");
					v2.property(GraphAnnotationBean.a2_p, "/test/bucket/delete");
					v1.property(org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.set, GraphAnnotationBean.a2_p, "/test/bucket/delete_2");
					v2.property(org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.set, GraphAnnotationBean.a2_p, "/test/bucket/delete_2");
					
					// Alternate edges
					if (2 == (ii % 6)) {
						e1.property(GraphAnnotationBean.a2_p, "/test/bucket/delete");					
					}
					else {
						e1.property(GraphAnnotationBean.a2_p, "/test/bucket/delete_2");										
					}
				}
				
			}
			System.out.println("Building test graph: " + new Date().getTime());
			tx.commit();
			System.out.println("Built test graph: " + new Date().getTime());
		}
		//(wait for the data to appear)
		System.out.println("Waiting 2s for ES to refresh...");
		Thread.sleep(2000L);
		
		// Quick test that the setup seems sensible
		{
			final TitanTransaction tx = _titan.buildTransaction().start();
			
			//(have to use an index, full scans are disabled)
			assertEquals(2*N_OBJECTS, StreamUtils.stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").vertices()).count()); 
			assertEquals(N_OBJECTS, StreamUtils.stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").edges()).count());
			tx.commit();
		}
		
		// First phase of deletes (do this via onPublishOrUpdate for coverage)
		{
			final TitanTransaction tx = _titan.buildTransaction().start();
			
			CompletableFuture<Collection<BasicMessageBean>> ret_val =
					_mock_graph_db_service.onPublishOrUpdate(bucket, Optional.empty(), false, Collections.emptySet(), ImmutableSet.of(GraphSchemaBean.name));
			
			
			// Got a "success" reply
			assertEquals(1, ret_val.join().size());
			assertEquals(1, ret_val.join().stream().filter(b -> b.success()).count());

			System.out.println("Waiting 2s for ES to refresh... " + new Date().getTime());
			Thread.sleep(2000L);
			
			//(have to use an index, full scans are disabled)
			assertEquals("Vertices", 2*(2*N_OBJECTS)/3, StreamUtils.stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").vertices()).count()); 
			assertEquals("Edges", N_OBJECTS/2, StreamUtils.stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").edges()).count());
			assertEquals("Vertices", 2*(2*N_OBJECTS)/3, StreamUtils.stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "/test/bucket/delete").vertices()).count()); 
			assertEquals("Edges", N_OBJECTS/2, StreamUtils.stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "/test/bucket/delete").edges()).count());
			
			// (Check that all the vertex references to the bucket have gone) 
			StreamUtils.<TitanVertex>stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").vertices()).forEach((Vertex v) -> {
				if (Optionals.streamOf(v.properties(GraphAnnotationBean.a2_p), false).anyMatch(p -> p.value().equals("/test/bucket/delete"))) {
					fail("All refs to /test/bucket/delete should be gone: " + titan_mapper.convertValue(v, JsonNode.class));
				}
				v.edges(Direction.BOTH).forEachRemaining(e -> { //(should be implied by the edge query, but just to be on the safe side...)
					if (Optionals.streamOf(v.properties(GraphAnnotationBean.a2_p), false).anyMatch(p -> p.value().equals("/test/bucket/delete"))) {
						fail("All refs to /test/bucket/delete should be gone: " + titan_mapper.convertValue(e, JsonNode.class));
					}
				});
			});
			
			tx.commit();			
		}
		
		// Second phase of deletes (first: make sure fails if specifiy a secondary buffer)
		{
			CompletableFuture<BasicMessageBean> ret_val =
					_mock_graph_db_service.handleBucketDeletionRequest(bucket, Optional.of("alex"), true);
			
			assertFalse(ret_val.join().success());
		}	
		// Let's try that again...
		{
			final DataBucketBean other_bucket = BeanTemplateUtils.build(DataBucketBean.class)
					.with(DataBucketBean::full_name, "/test/bucket/delete_2")
				.done().get();			
			
			final TitanTransaction tx = _titan.buildTransaction().start();
			
			CompletableFuture<BasicMessageBean> ret_val =
					_mock_graph_db_service.handleBucketDeletionRequest(other_bucket, Optional.empty(), true);
			
			assertTrue(ret_val.join().success());
			
			System.out.println("Waiting 2s for ES to refresh... " + new Date().getTime());
			Thread.sleep(2000L);
			
			//(have to use an index, full scans are disabled)
			assertEquals("Vertices", (2*N_OBJECTS)/3, StreamUtils.stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").vertices()).count()); 
			assertEquals("Edges", N_OBJECTS/3, StreamUtils.stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").edges()).count());
			assertEquals("Vertices", (2*N_OBJECTS)/3, StreamUtils.stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "/test/bucket/delete_2").vertices()).count()); 
			assertEquals("Edges", N_OBJECTS/3, StreamUtils.stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "/test/bucket/delete_2").edges()).count());
			
			tx.commit();			
		}
	}
	
	@Test
	public void test_miscCoverage() {
		
		final Optional<IGenericDataService> data_service = _mock_graph_db_service.getDataService();
		assertEquals(_mock_graph_db_service, data_service.get());
		
		assertEquals(Optional.empty(), data_service.get().getReadableCrudService(null, null, null));
		assertEquals(Optional.empty(), data_service.get().getUpdatableCrudService(null, null, null));
		assertEquals(Collections.emptySet(), data_service.get().getSecondaryBuffers(null, null));
		assertEquals(Optional.empty(), data_service.get().getPrimaryBufferName(null, null));
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test").done().get();
		assertFalse(data_service.get().switchCrudServiceToPrimaryBuffer(bucket, null, null, null).join().success());
		assertTrue(data_service.get().handleAgeOutRequest(null).join().success());
	}

	@Test
	public void test_createRemoteConfig() {
		
		final Config cfg = _mock_graph_db_service.createRemoteConfig(null);
		
		System.out.println(cfg.root().toString());
		
		assertEquals("inmemory", cfg.getString("storage.backend"));
		assertEquals("elasticsearch", cfg.getString("index.search.backend"));		
	}
	
	@Test
	public void test_setup() {
		
		//TODO (ALEPH-15): test file location
		
		final TitanGraphConfigBean config = 
				BeanTemplateUtils.build(TitanGraphConfigBean.class)
					.with(TitanGraphConfigBean::config_override, 
							ImmutableMap.<String, Object>of(
									"storage.backend", "inmemory",
									"query.force-index", true
									))
				.done().get();
		
		final TitanGraph g = _mock_graph_db_service.setup(config);
		
		assertEquals("inmemory", g.configuration().getString("storage.backend"));
		assertEquals(true, g.configuration().getBoolean("query.force-index"));
	}
	
}
