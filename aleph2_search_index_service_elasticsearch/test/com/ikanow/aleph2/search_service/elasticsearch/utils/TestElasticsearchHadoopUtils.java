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
package com.ikanow.aleph2.search_service.elasticsearch.utils;

import static org.junit.Assert.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultimap;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputConfigBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.search_service.elasticsearch.hadoop.assets.Aleph2EsInputFormat;
import com.ikanow.aleph2.search_service.elasticsearch.services.ElasticsearchIndexService;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext;
import com.ikanow.aleph2.shared.crud.elasticsearch.services.MockElasticsearchCrudServiceFactory;
import com.ikanow.aleph2.shared.crud.elasticsearch.services.ElasticsearchCrudService.CreationPolicy;

import fj.data.Either;

public class TestElasticsearchHadoopUtils {

	protected ICrudService<JsonNode> _dummy_crud = null;
	protected MockElasticsearchCrudServiceFactory _crud_factory = null;
	
	@Before
	public void setupDummyCrudService() {
		
		_crud_factory = new MockElasticsearchCrudServiceFactory();
		
		_dummy_crud = _crud_factory.getElasticsearchCrudService(
				JsonNode.class, 
				new ElasticsearchContext.ReadWriteContext(_crud_factory.getClient(),
						new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("r__" + BucketUtils.getUniqueSignature("/test", Optional.empty()), Optional.empty(), Either.left(true)),
						new ElasticsearchContext.TypeContext.ReadWriteTypeContext.FixedRwTypeContext("data_object_test")
						), 
				Optional.empty(), 
				CreationPolicy.OPTIMIZED, 
				Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
		
		_dummy_crud.deleteDatastore().join();
		try { Thread.sleep(1100L); } catch (Exception e) {}		
	}
	
	@SuppressWarnings("rawtypes")
	public static interface InputFormatAccessTest extends IAnalyticsAccessContext<InputFormat> {}
	public static interface StringAccessTest extends IAnalyticsAccessContext<String> {}
	
	@Test
	public void test_getAccessService() {

		new ElasticsearchHadoopUtils(); //code coverage!
		
		@SuppressWarnings("rawtypes")
		final IAnalyticsAccessContext<InputFormat> access_context =
				ElasticsearchHadoopUtils.getInputFormat(null, null); // (doesn't matter what the input is here)
		
		assertEquals(Either.right(Aleph2EsInputFormat.class), access_context.getAccessService());
		
		final ElasticsearchIndexService test_service = new ElasticsearchIndexService(null, _crud_factory, null);
		
		assertEquals(Optional.empty(), test_service.getUnderlyingPlatformDriver(StringAccessTest.class, Optional.empty()));
		// (add more functional tests below)
		assertEquals(Optional.empty(), test_service.getUnderlyingPlatformDriver(InputFormatAccessTest.class, Optional.empty()));
		assertEquals(Optional.empty(), test_service.getUnderlyingPlatformDriver(InputFormatAccessTest.class, Optional.of("badly_formatted")));
		assertTrue("Should return input format access test", test_service.getUnderlyingPlatformDriver(InputFormatAccessTest.class, Optional.of("::{}")).isPresent());
	}
	
	@Test 
	public void test_getAccessConfig() {
		
		// No filter (no type), max records
		{
			final AnalyticThreadJobBean.AnalyticThreadJobInputBean job_input =
					BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobInputBean.class)
						.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::resource_name_or_id, "/test")
						.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::config,
								BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobInputConfigBean.class)
									.with(AnalyticThreadJobBean.AnalyticThreadJobInputConfigBean::test_record_limit_request, 10L)
								.done().get()
								)
					.done().get()
					;
			
			final ElasticsearchIndexService test_service = new ElasticsearchIndexService(null, _crud_factory, null);
			
			@SuppressWarnings("rawtypes")
			final IAnalyticsAccessContext<InputFormat> access_context = test_service.getUnderlyingPlatformDriver(InputFormatAccessTest.class, 					
					Optional.of("misc_user:/bucket_path:" + BeanTemplateUtils.toJson(job_input).toString())).get();
					
			final Map<String, Object> res = access_context.getAccessConfig().get();
			assertEquals(Arrays.asList("aleph2.batch.debugMaxSize", "es.index.read.missing.as.empty", "es.query", "es.resource"), res.keySet().stream().sorted().collect(Collectors.toList()));
			
			assertEquals("r__" + BucketUtils.getUniqueSignature("/test", Optional.empty()) + "*/", res.get("es.resource"));
			assertEquals("?q=*", res.get("es.query"));
			assertEquals("yes", res.get("es.index.read.missing.as.empty"));
			assertEquals("service_name=Aleph2EsInputFormat options={aleph2.batch.debugMaxSize=10, es.resource=r__test__f911f6d77ac9*/, es.index.read.missing.as.empty=yes, es.query=?q=*}", access_context.describe());
			assertEquals("10", res.get("aleph2.batch.debugMaxSize"));
		}
		
		// No filter (added type)
		{
			_dummy_crud.storeObject(BeanTemplateUtils.configureMapper(Optional.empty()).createObjectNode()).join();
			try { Thread.sleep(1100L); } catch (Exception e) {}
			
			final AnalyticThreadJobBean.AnalyticThreadJobInputBean job_input =
					BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobInputBean.class)
						.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::resource_name_or_id, "/test")					
					.done().get()
					;
			
			@SuppressWarnings("rawtypes")
			final IAnalyticsAccessContext<InputFormat> access_context =
					ElasticsearchHadoopUtils.getInputFormat(_crud_factory.getClient(), job_input); // (doesn't matter what the input is here)
			
			final Map<String, Object> res = access_context.getAccessConfig().get();
			assertEquals(Arrays.asList("es.index.read.missing.as.empty", "es.query", "es.resource"), res.keySet().stream().sorted().collect(Collectors.toList()));
			
			assertEquals("r__" + BucketUtils.getUniqueSignature("/test", Optional.empty()) + "*/data_object_test", res.get("es.resource"));
			assertEquals("?q=*", res.get("es.query"));
			assertEquals("yes", res.get("es.index.read.missing.as.empty"));
			assertEquals(null, res.get("aleph2.batch.debugMaxSize"));
		}
		
		// More complex filter ("URL query")
		{
			final AnalyticThreadJobBean.AnalyticThreadJobInputBean job_input =
					BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobInputBean.class)
						.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::resource_name_or_id, "/test2")			
						.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::filter,
									new LinkedHashMap<String, Object>(ImmutableMap.<String, Object>builder()
										.put("technology_override", "?q=test")
									.build())								
								)
					.done().get()
					;
			
			@SuppressWarnings("rawtypes")
			final IAnalyticsAccessContext<InputFormat> access_context =
					ElasticsearchHadoopUtils.getInputFormat(_crud_factory.getClient(), job_input); // (doesn't matter what the input is here)
			
			final Map<String, Object> res = access_context.getAccessConfig().get();
			assertEquals(Arrays.asList("es.index.read.missing.as.empty", "es.query", "es.resource"), res.keySet().stream().sorted().collect(Collectors.toList()));
			
			assertEquals("r__" + BucketUtils.getUniqueSignature("/test2", Optional.empty()) + "*/", res.get("es.resource"));
			assertEquals("?q=test", res.get("es.query"));
			assertEquals("yes", res.get("es.index.read.missing.as.empty"));			
		}
		
		// More complex filter (JSON)
		{
			final AnalyticThreadJobBean.AnalyticThreadJobInputBean job_input =
					BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobInputBean.class)
						.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::resource_name_or_id, "/test3")			
						.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::filter,
									new LinkedHashMap<String, Object>(ImmutableMap.<String, Object>builder()
										.put("technology_override", 
												ImmutableMap.<String, Object>builder()
													.put("test", "test2")
												.build()
												)
									.build())								
								)
					.done().get()
					;
			
			@SuppressWarnings("rawtypes")
			final IAnalyticsAccessContext<InputFormat> access_context =
					ElasticsearchHadoopUtils.getInputFormat(_crud_factory.getClient(), job_input); // (doesn't matter what the input is here)
			
			final Map<String, Object> res = access_context.getAccessConfig().get();
			assertEquals(Arrays.asList("es.index.read.missing.as.empty", "es.query", "es.resource"), res.keySet().stream().sorted().collect(Collectors.toList()));
			
			assertEquals("r__" + BucketUtils.getUniqueSignature("/test3", Optional.empty()) + "*/", res.get("es.resource"));
			assertEquals("{\"test\":\"test2\"}", res.get("es.query"));
			assertEquals("yes", res.get("es.index.read.missing.as.empty"));						
		}
		
	}
	
	@Test
	public void test_getTimedIndexes() {
		final LinkedHashMultimap<String, String> test_in = LinkedHashMultimap.create();
		test_in.put("test_time_sliced_index__4633d78cd58b_2015.04.01", "test");
		test_in.put("test_time_sliced_index__4633d78cd58b_2015.06.01", "test");
		test_in.put("r__test_time_sliced_index__4633d78cd58b_2015.06.03", "test");
		test_in.put("r__test_time_sliced_index__4633d78cd58b", "test");
		
		// min + max
		{
			final Date now = Date.from(LocalDateTime.of(2015, 6, 12, 2, 0).atZone(ZoneOffset.systemDefault()).toInstant());
			
			AnalyticThreadJobInputConfigBean test =
					BeanTemplateUtils.build(AnalyticThreadJobInputConfigBean.class)
						.with(AnalyticThreadJobInputConfigBean::time_min, "12 days")
						.with(AnalyticThreadJobInputConfigBean::time_max, "8 days")
					.done().get();
			
			AnalyticThreadJobInputBean input =
					BeanTemplateUtils.build(AnalyticThreadJobInputBean.class)
					.with(AnalyticThreadJobInputBean::config, test)
					.done().get();
			
			final Optional<Stream<String>> res = 
					ElasticsearchHadoopUtils.getTimedIndexes(input, test_in, now);
			
			assertEquals(
					Arrays.asList(
							"test_time_sliced_index__4633d78cd58b_2015.06.01",
							"r__test_time_sliced_index__4633d78cd58b_2015.06.03"
							)
					, 
					res.map(s -> s.collect(Collectors.toList())).get());
		}
		// neither
		{
			AnalyticThreadJobInputConfigBean test =
					BeanTemplateUtils.build(AnalyticThreadJobInputConfigBean.class)
					.done().get();
			
			AnalyticThreadJobInputBean input =
					BeanTemplateUtils.build(AnalyticThreadJobInputBean.class)
					.with(AnalyticThreadJobInputBean::config, test)
					.done().get();
			
			final Optional<Stream<String>> res = 
					ElasticsearchHadoopUtils.getTimedIndexes(input, test_in, new Date());
			
			assertEquals(Optional.empty(), res);
		}
		
	}
}
