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

import java.io.File;
import java.util.Arrays;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockSecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.graph.titan.data_model.SimpleDecompConfigBean;
import com.ikanow.aleph2.graph.titan.data_model.SimpleDecompConfigBean.SimpleDecompElementBean;
import com.thinkaurelius.titan.core.TitanGraph;

/**
 * @author Alex
 *
 */
public class TestTitanGraphBuilderEnrichmentService {

	TitanGraph _titan = null;
	MockTitanGraphService _mock_graph_db_service = null;
	
	@Before
	public void setup() {
		
		//(delete old ES files)
		try {
			new File(TitanGraphService.UUID).delete();
		}
		catch (Exception e) {}
		
		_mock_graph_db_service = new MockTitanGraphService();
		_titan = _mock_graph_db_service.getUnderlyingPlatformDriver(TitanGraph.class, Optional.empty()).get();
	}
	
	@Test
	public void test_TitanGraphBuilderEnrichmentService() {
		
		final TitanGraphBuilderEnrichmentService graph_enrich_service = new TitanGraphBuilderEnrichmentService();
		
		final MockServiceContext service_context = new MockServiceContext();
		final MockSecurityService mock_security = new MockSecurityService();
		mock_security.setGlobalMockRole("nobody:DataBucketBean:read,write:test:end:2:end:*", true);
		service_context.addService(ISecurityService.class, Optional.empty(), mock_security);
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

		final EnrichmentControlMetadataBean control = BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
				.done().get();
		
		// Initialize
		{
			graph_enrich_service.onStageInitialize(context, bucket, control, null, Optional.empty());
		}
		
		// First batch vs an empty graph
		{
			//TODO: phase 1 data
		
			//TODO: check graph
			
			//TODO: check stats
		}
		
		// Second batch vs the results of the previous batch
		{
			//TODO: phase 2 data
			
			//TODO: check graph
			
			//TODO: check stats
		}		
		
		// (coverage)
		graph_enrich_service.onStageComplete(true);
	}
}
