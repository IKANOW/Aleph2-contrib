/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
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
package com.ikanow.aleph2.analytics.storm.assets;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.analytics.storm.services.MockStormTestingService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;

public class TestPassthroughTopology extends TestPassthroughBase {
	static final Logger _logger = LogManager.getLogger(); 

	@Test
	public void test_passthroughTopology() throws InterruptedException, ExecutionException {
		//////////////////////////////////////////////////////
		// PHASE 1: GET AN IN-TECHNOLOGY CONTEXT
		// Bucket
		final AnalyticThreadJobBean.AnalyticThreadJobInputBean analytic_input = 
				BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobInputBean.class)
					.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::data_service, "stream")
					.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::resource_name_or_id, "")
				.done().get();
		
		final AnalyticThreadJobBean.AnalyticThreadJobOutputBean analytic_output =
				BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobOutputBean.class)
					.with(AnalyticThreadJobBean.AnalyticThreadJobOutputBean::is_transient, false)
				.done().get();
		
		final AnalyticThreadJobBean analytic_job1 = BeanTemplateUtils.build(AnalyticThreadJobBean.class)
				.with(AnalyticThreadJobBean::name, "analytic_job1")
				.with(AnalyticThreadJobBean::inputs, Arrays.asList(analytic_input))
				.with(AnalyticThreadJobBean::output, analytic_output)
				.done().get();		
		
		final AnalyticThreadBean analytic_thread = 	BeanTemplateUtils.build(AnalyticThreadBean.class)
				.with(AnalyticThreadBean::jobs, Arrays.asList(analytic_job1))
				.done().get();		
		
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::_id, "test_passthroughtopology")
				.with(DataBucketBean::modified, new Date())
				.with(DataBucketBean::full_name, "/test/passthrough")
				.with(DataBucketBean::analytic_thread, analytic_thread)
				.with("data_schema", BeanTemplateUtils.build(DataSchemaBean.class)
						.with("search_index_schema", BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
								.done().get())
						.done().get())
				.done().get();
		
		//////////////////////////////////////////////////////
		// PHASE 2: SPECIFICALLY FOR THIS TEST
		//(Also: register a listener on the output to generate a secondary queue)
		final ICoreDistributedServices cds = _service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		final String end_queue_topic = cds.generateTopicName(test_bucket.full_name(), ICoreDistributedServices.QUEUE_END_NAME);
		cds.createTopic(end_queue_topic, Optional.of(Collections.emptyMap()));
		
		//////////////////////////////////////////////////////
		// PHASE 3: SUBMIT TO TESTING SERVICE
		final BasicMessageBean res = new MockStormTestingService(_service_context).testAnalyticModule(test_bucket).get();		
		assertTrue("Storm starts", res.success());
		
		_logger.info("******** Submitted storm cluster: " + res.message());
		Thread.sleep(5000L);
		
		//////////////////////////////////////////////////////
		//PHASE 4: PREPARE INPUT DATA
		
		// 4a: cleanse
		
		final ISearchIndexService index_service = _service_context.getService(ISearchIndexService.class, Optional.empty()).get();
		final ICrudService<JsonNode> crud_service = 
				index_service.getDataService()
					.flatMap(s -> s.getWritableDataService(JsonNode.class, test_bucket, Optional.empty(), Optional.empty()))
					.flatMap(IDataWriteService::getCrudService)
					.get();
		crud_service.deleteDatastore().get();
		_logger.info("******** Cleansed existing datastore");
		Thread.sleep(2000L);
		assertEquals(0L, crud_service.countObjects().get().intValue());
		
		// 4b: write to kafka
		
		final String topic_name = cds.generateTopicName(test_bucket.full_name(), Optional.empty());
		cds.produce(topic_name, "{\"test\":\"test1\"}");
		_logger.info("******** Written to CDS: " + topic_name);
		
		//////////////////////////////////////////////////////
		//PHASE 5: CHECK OUTPUT DATA		
		
		// 5a: check ES index
		
		for (int i = 0; i < 60; ++i) {
			Thread.sleep(1000L);
			if (crud_service.countObjects().get() > 0) { 
				_logger.info("******** Waited for ES object to populate: " + i);
				break;
			}
		}		
		assertEquals("Should be 1 object in the repo", 1L, crud_service.countObjects().get().intValue());		
		assertEquals("Object should be test:test1", 1L, crud_service.countObjectsBySpec(CrudUtils.allOf().when("test", "test1")).get().intValue());		
		
		// 5b: check kafka queue
		
		Iterator<String> consumer = cds.consumeAs(end_queue_topic, Optional.empty());
		int message_count = 0;
		//read the item off the queue
		while ( consumer.hasNext() ) {
			consumer.next();
        	message_count++;
		}
		assertEquals(1, message_count);
	}
	
}
