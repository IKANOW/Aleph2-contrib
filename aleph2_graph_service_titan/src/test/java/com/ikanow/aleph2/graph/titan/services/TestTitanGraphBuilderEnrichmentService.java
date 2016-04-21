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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import scala.Tuple2;

import com.codepoetics.protonpack.StreamUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.core.shared.utils.BatchRecordUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockSecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.GraphAnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.graph.titan.data_model.SimpleDecompConfigBean;
import com.ikanow.aleph2.graph.titan.data_model.SimpleDecompConfigBean.SimpleDecompElementBean;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.TemporaryBackendException;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;

/**
 * @author Alex
 *
 */
public class TestTitanGraphBuilderEnrichmentService extends TestTitanCommon {
	final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	@SuppressWarnings("unchecked")
	@Before
	public void setup() throws InterruptedException {
		super.setup();
		
		// wipe anything existing in the graph
		final TitanTransaction tx = _titan.buildTransaction().start();
		Optionals.<TitanVertex>streamOf(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").vertices(), false).forEach(v -> v.remove());
		Optionals.<TitanEdge>streamOf(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").edges(), false).forEach(v -> v.remove());
		tx.commit();
		System.out.println("Sleeping while waiting to cleanse ES");
		Thread.sleep(2000L);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test_TitanGraphBuilderEnrichmentService() throws InterruptedException {
		
		final TitanGraphBuilderEnrichmentService graph_enrich_service = new TitanGraphBuilderEnrichmentService();
		
		final MockServiceContext service_context = new MockServiceContext();
		final MockSecurityService mock_security = new MockSecurityService();
		mock_security.setGlobalMockRole("nobody:DataBucketBean:read,write:test:end:2:end:*", true);
		service_context.addService(ISecurityService.class, Optional.empty(), mock_security);
		service_context.addService(IGraphService.class, Optional.empty(), _mock_graph_db_service);
		final IEnrichmentModuleContext context = Mockito.mock(IEnrichmentModuleContext.class);
		Mockito.when(context.getServiceContext()).thenReturn(service_context);
		Mockito.when(context.getNextUnusedId()).thenReturn(0L);
		
		final EnrichmentControlMetadataBean control_merge = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
				.with(EnrichmentControlMetadataBean::entry_point, SimpleGraphMergeService.class.getName())				
				.done().get();
				
		final EnrichmentControlMetadataBean control_decomp = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
				.with(EnrichmentControlMetadataBean::entry_point, SimpleGraphDecompService.class.getName())				
				.with(EnrichmentControlMetadataBean::config,
						BeanTemplateUtils.toMap(
						BeanTemplateUtils.build(SimpleDecompConfigBean.class)
							.with(SimpleDecompConfigBean::elements, Arrays.asList(
									BeanTemplateUtils.build(SimpleDecompElementBean.class)
										.with(SimpleDecompElementBean::edge_name, "test_edge_1")
										.with(SimpleDecompElementBean::from_fields, Arrays.asList("int_ip1", "int_ip2"))
										.with(SimpleDecompElementBean::from_type, "ip")
										.with(SimpleDecompElementBean::to_fields, Arrays.asList("host1", "host2"))
										.with(SimpleDecompElementBean::to_type, "host")
									.done().get()
									,
									BeanTemplateUtils.build(SimpleDecompElementBean.class)
										.with(SimpleDecompElementBean::edge_name, "test_edge_2")
										.with(SimpleDecompElementBean::from_fields, Arrays.asList("missing"))
										.with(SimpleDecompElementBean::from_type, "ip")
										.with(SimpleDecompElementBean::to_fields, Arrays.asList("host1", "host2"))
										.with(SimpleDecompElementBean::to_type, "host")
									.done().get()
									)
							)
						.done().get())
				)
			.done().get();
		
		final GraphSchemaBean graph_schema = BeanTemplateUtils.build(GraphSchemaBean.class)
				.with(GraphSchemaBean::custom_decomposition_configs, Arrays.asList(control_decomp))
				.with(GraphSchemaBean::custom_merge_configs, Arrays.asList(control_merge))
				.done().get();
		
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/end/2/end")
				.with(DataBucketBean::owner_id, "nobody")
				.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class)
								.with(DataSchemaBean::graph_schema, graph_schema)
								.done().get()
				)
				.done().get();

		_mock_graph_db_service.onPublishOrUpdate(bucket, Optional.empty(), false, ImmutableSet.of(GraphSchemaBean.name), Collections.emptySet());
		
		final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
				.done().get();
		
		// Initialize
		{
			graph_enrich_service.onStageInitialize(context, bucket, control, null, Optional.empty());
		}
		
		// First batch vs an empty graph
		{
			final Stream<Tuple2<Long, IBatchRecord>> batch = 
					Stream.<ObjectNode>of(
						_mapper.createObjectNode().put("int_ip1", "ipA").put("host1", "dY")
						)
					.map(o -> Tuples._2T(0L, new BatchRecordUtils.JsonBatchRecord(o)))
					;		
		
			graph_enrich_service.onObjectBatch(batch, Optional.empty(), Optional.empty());
			System.out.println("Sleeping 2s to wait for ES to refresh");
			Thread.sleep(2000L);
			
			// Check graph
			final TitanTransaction tx = _titan.buildTransaction().start();
			assertEquals(2, StreamUtils.stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").vertices()).count());
			assertEquals(1, StreamUtils.stream(tx.query().has(GraphAnnotationBean.type, "ip").vertices()).count());
			assertEquals(1, StreamUtils.stream(tx.query().has(GraphAnnotationBean.type, "host").vertices()).count());
			assertEquals(1, StreamUtils.stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").edges()).count());
			tx.commit();
			
			// Check stats:
			assertEquals(2L, graph_enrich_service._mutable_stats.get().vertices_created);
			assertEquals(0L, graph_enrich_service._mutable_stats.get().vertices_updated);
			assertEquals(2L, graph_enrich_service._mutable_stats.get().vertices_emitted);
			assertEquals(0L, graph_enrich_service._mutable_stats.get().vertex_matches_found);
			assertEquals(0L, graph_enrich_service._mutable_stats.get().vertex_errors);
			assertEquals(1L, graph_enrich_service._mutable_stats.get().edges_created);
			assertEquals(0L, graph_enrich_service._mutable_stats.get().edges_updated);
			assertEquals(1L, graph_enrich_service._mutable_stats.get().edges_emitted); 
			assertEquals(0L, graph_enrich_service._mutable_stats.get().edge_matches_found);
			assertEquals(0L, graph_enrich_service._mutable_stats.get().edge_errors);
		}
		
		// Second batch vs the results of the previous batch
		{
			// Create some recoverable errors:
			{
				final PermanentLockingException outer = Mockito.mock(PermanentLockingException.class);
				graph_enrich_service._MUTABLE_TEST_ERRORS.push(new TitanException("test", outer));
			}
			{
				final TemporaryBackendException inner = Mockito.mock(TemporaryBackendException.class);
				final TitanException outer = Mockito.mock(TitanException.class);
				Mockito.when(outer.getCause()).thenReturn(inner);
				graph_enrich_service._MUTABLE_TEST_ERRORS.push(new TitanException("test", outer));
			}			
			
			final Stream<Tuple2<Long, IBatchRecord>> batch = 
					Stream.<ObjectNode>of(
						_mapper.createObjectNode().put("int_ip1", "ipA").put("int_ip2", "ipB").put("host1", "dX").put("host2", "dY")
						,
						_mapper.createObjectNode().put("int_ip1", "ipA").put("host1", "dZ").put("host2", "dY")
					)
					.map(o -> Tuples._2T(0L, new BatchRecordUtils.JsonBatchRecord(o)))
					;		
			
			graph_enrich_service.onObjectBatch(batch, Optional.empty(), Optional.empty());
			System.out.println("Sleeping 2s to wait for ES to refresh");
			Thread.sleep(2000L);
			
			// Check graph
			final TitanTransaction tx = _titan.buildTransaction().start();
			assertEquals(5, StreamUtils.stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").vertices()).count());
			assertEquals(2, StreamUtils.stream(tx.query().has(GraphAnnotationBean.type, "ip").vertices()).count());
			assertEquals(3, StreamUtils.stream(tx.query().has(GraphAnnotationBean.type, "host").vertices()).count());
			assertEquals(5, StreamUtils.stream(tx.query().hasNot(GraphAnnotationBean.a2_p, "get_everything").edges()).count());
			tx.commit();
			
			// Check stats:
			assertEquals(5L, graph_enrich_service._mutable_stats.get().vertices_created);
			assertEquals(2L, graph_enrich_service._mutable_stats.get().vertices_updated);
			assertEquals(7L, graph_enrich_service._mutable_stats.get().vertices_emitted);
			assertEquals(2L, graph_enrich_service._mutable_stats.get().vertex_matches_found);
			assertEquals(0L, graph_enrich_service._mutable_stats.get().vertex_errors);
			assertEquals(5L, graph_enrich_service._mutable_stats.get().edges_created);
			assertEquals(1L, graph_enrich_service._mutable_stats.get().edges_updated);
			assertEquals(7L, graph_enrich_service._mutable_stats.get().edges_emitted);
			assertEquals(1L, graph_enrich_service._mutable_stats.get().edge_matches_found);
			assertEquals(0L, graph_enrich_service._mutable_stats.get().edge_errors);
		}				
		// Check error case:
		{
			graph_enrich_service._MUTABLE_TEST_ERRORS.push(new TitanException("test"));
			try {
				graph_enrich_service.onObjectBatch(Stream.empty(), Optional.empty(), Optional.empty());
				fail("Should have errored");
			}
			catch (Exception e) {}		
		}
		
		// (coverage)
		graph_enrich_service.onStageComplete(true);
	}
}
