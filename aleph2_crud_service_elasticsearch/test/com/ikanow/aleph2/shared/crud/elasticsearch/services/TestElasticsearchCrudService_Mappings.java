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
 ******************************************************************************/
package com.ikanow.aleph2.shared.crud.elasticsearch.services;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.IBatchSubservice;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.BeanTemplate;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext;
import com.ikanow.aleph2.shared.crud.elasticsearch.services.ElasticsearchCrudService.CreationPolicy;
import com.ikanow.aleph2.shared.crud.elasticsearch.services.ElasticsearchCrudService.ElasticsearchBatchSubsystem;

public class TestElasticsearchCrudService_Mappings {

	////////////////////////////////////////////////

	// UTILS
	
	// Set this string to connect vs a real D
	
	@Before
	public void setupCrudServiceFactory() throws UnknownHostException {
		if (null == _factory) {
			_factory = new MockElasticsearchCrudServiceFactory();
		}
	}
	
	protected IElasticsearchCrudServiceFactory _factory = null; 
	
	public <O> ElasticsearchCrudService<O> getTestService(String test_name_case, Class<O> bean_clazz, ElasticsearchContext es_context
			) throws InterruptedException, ExecutionException
	{
		
		final String test_name = test_name_case.toLowerCase();
		
		final ElasticsearchCrudService<O> service = _factory.getElasticsearchCrudService(bean_clazz, es_context, 
				Optional.of(false), CreationPolicy.AVAILABLE_IMMEDIATELY,
				Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

		try {
			service.deleteDatastore().get();
		}
		catch (Exception e) {
			// It's OK probably just doens't exist yet
		}
		
		// Create an empty index
		final CreateIndexRequest cir = new CreateIndexRequest(test_name);
		_factory.getClient().admin().indices().create(cir).actionGet();		
		//(Wait for above operation to be completed)
		_factory.getClient().admin().cluster().health(new ClusterHealthRequest(test_name).waitForYellowStatus()).actionGet();
		
		return service;		
	}
		
	public <O> ElasticsearchCrudService<O> getTestService(String test_name_case, Class<O> bean_clazz) throws InterruptedException, ExecutionException {
		final String test_name = test_name_case.toLowerCase();
				
		return getTestService(test_name_case, bean_clazz,
				new ElasticsearchContext.ReadWriteContext(_factory.getClient(), 
						new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext(test_name, Optional.empty()),
						new ElasticsearchContext.TypeContext.ReadWriteTypeContext.FixedRwTypeContext("test"))
				);
	}
	
	/////////////////////////////////////////////////////
	
	// Test handling of mapping conflicts
	
	public static class TestBean {
		String test_string1;
		Map<String, Object> test_map = new HashMap<String, Object>();
	}
	
	@Test
	public void test_MultipleMappingsPerIndex_singleStore() throws InterruptedException, ExecutionException {
		
		// Using normal type system: should fail
		
		{
			ElasticsearchCrudService<TestBean> service = getTestService("testMultipleMappingsPerIndex_1", TestBean.class,				
					new ElasticsearchContext.ReadWriteContext(_factory.getClient(), 
							new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("testMultipleMappingsPerIndex_1".toLowerCase(), Optional.empty()),
							new ElasticsearchContext.TypeContext.ReadWriteTypeContext.FixedRwTypeContext("type1")));
	
			TestBean test_long = new TestBean();
			test_long.test_string1 = "test1a.1";
			test_long.test_map.put("test_map", 1L);
			
			
			TestBean test_string = new TestBean();
			test_string.test_string1 = "test1a.2";
			test_string.test_map.put("test_map", "test1b");
	
			assertEquals(0L, service.countObjects().get().longValue());
			
			service.storeObject(test_long).get();
			
			assertEquals(1L, service.countObjects().get().longValue());
			
			CompletableFuture<Supplier<Object>> ret_val = service.storeObject(test_string);
			
			try {
				ret_val.get();
				fail("Should have throw mapping exception");
			}
			catch (Exception mpe) {
				assertTrue("Correct type: " + mpe.getClass(), org.elasticsearch.index.mapper.MapperParsingException.class.isAssignableFrom(mpe.getCause().getClass()));
			}
			
			assertEquals(1L, service.countObjects().get().longValue());
		}		

		// Using auto type: should create a second type
		{
			ElasticsearchCrudService<TestBean> service = getTestService("testMultipleMappingsPerIndex_2", TestBean.class,				
					new ElasticsearchContext.ReadWriteContext(_factory.getClient(), 
							new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("testMultipleMappingsPerIndex_2".toLowerCase(), Optional.empty()),
							new ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext(Optional.empty(), Optional.empty(), new HashSet<String>(Arrays.asList("fixed_field_ignore")))));
	
			TestBean test_long = new TestBean();
			test_long.test_string1 = "test1a.1";
			test_long.test_map.put("test_map", 1L);
			
			
			TestBean test_string = new TestBean();
			test_string.test_string1 = "test1a.2";
			test_string.test_map.put("test_map", "test1b");
	
			assertEquals(0L, service.countObjects().get().longValue());
			
			service.storeObject(test_long).get();
			
			assertEquals(1L, service.countObjects().get().longValue());
			
			CompletableFuture<Supplier<Object>> ret_val = service.storeObject(test_string);
			
			ret_val.get();
			
			assertEquals(2L, service.countObjects().get().longValue());

			// Reinsert and check it still works (includes fail-then-recurse-then-succeed test)
			
			service.storeObject(test_long).get();
			
			assertEquals(3L, service.countObjects().get().longValue());
			
			CompletableFuture<Supplier<Object>> ret_val_2 = service.storeObject(test_string);

			ret_val_2.get();
			
			assertEquals(4L, service.countObjects().get().longValue());
			
			// And another level of recursion
			
			TestBean test_object = new TestBean();
			test_object.test_string1 = "test1a.3";
			test_object.test_map.put("test_map", test_string);
	
			CompletableFuture<Supplier<Object>> ret_val_3 = service.storeObject(test_object);

			ret_val_3.get();
			
			assertEquals(5L, service.countObjects().get().longValue());
		}
		
		//////////////////////
		
		// FINALLY: handle the case where the field is a designated "fixed field"
		
		{
			ElasticsearchCrudService<TestBean> service = getTestService("testMultipleMappingsPerIndex_3", TestBean.class,				
					new ElasticsearchContext.ReadWriteContext(_factory.getClient(), 
							new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("testMultipleMappingsPerIndex_3".toLowerCase(), Optional.empty()),
							new ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext(Optional.empty(), Optional.empty(), new HashSet<String>(Arrays.asList("test_map.test_map")))));
	
			TestBean test_long = new TestBean();
			test_long.test_string1 = "test1a.1";
			test_long.test_map.put("test_map", 1L);
			
			
			TestBean test_string = new TestBean();
			test_string.test_string1 = "test1a.2";
			test_string.test_map.put("test_map", "test1b");
	
			assertEquals(0L, service.countObjects().get().longValue());
			
			service.storeObject(test_long).get();
			
			assertEquals(1L, service.countObjects().get().longValue());
			
			CompletableFuture<Supplier<Object>> ret_val = service.storeObject(test_string);
			
			try {
				ret_val.get();
				fail("Should have errored since the failing field is fixed");
			}
			catch (Exception e) {}
			
			assertEquals(1L, service.countObjects().get().longValue());
		}
		
	}
	
	@Test
	public void test_MultipleMappingsPerIndex_multiStore() throws InterruptedException, ExecutionException {
		
		// 1) Check fails with mixed mapping
		
		{
			ElasticsearchCrudService<TestBean> service = getTestService("testMultipleMappingsPerIndex_multi1", TestBean.class,				
					new ElasticsearchContext.ReadWriteContext(_factory.getClient(), 
							new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("testMultipleMappingsPerIndex_multi1".toLowerCase(), Optional.empty()),
							new ElasticsearchContext.TypeContext.ReadWriteTypeContext.FixedRwTypeContext("type1")));
	
			// Set up the mapping
			
			final TestBean test_long1 = new TestBean();
			test_long1.test_string1 = "test1a.1";
			test_long1.test_map.put("test_map", 1L);
			
			final TestBean test_long2 = new TestBean();
			test_long2.test_string1 = "test1a.2";
			test_long2.test_map.put("test_map", 2L);

			service.storeObjects(Arrays.asList(test_long1, test_long2)).get();
			
			assertEquals(2L, service.countObjects().get().longValue());
			
			// Submit 2 docs, one that will work with "type1", one that won't work
			
			final TestBean test_long3 = new TestBean();
			test_long3.test_string1 = "test1a.3";
			test_long3.test_map.put("test_map", 1L);
			
			
			final TestBean test_string1 = new TestBean();
			test_string1.test_string1 = "test1a.4";
			test_string1.test_map.put("test_map", "test1b");

			final CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> cf = service.storeObjects(Arrays.asList(test_long3, test_string1));
			cf.get();
			
			assertEquals(3L, service.countObjects().get().longValue());
			assertEquals(1, cf.get()._2().get().intValue());

			final BeanTemplate<TestBean> for_queries = BeanTemplateUtils.build(TestBean.class).with("test_map", null).done();
			assertEquals(3L, service.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type1")).get().intValue());
			assertEquals(0L, service.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type2")).get().intValue());
			
		}
		
		// 2) Check works with mixed mapping when using auto type
		
		ElasticsearchCrudService<TestBean> service = getTestService("testMultipleMappingsPerIndex_multi2", TestBean.class,				
				new ElasticsearchContext.ReadWriteContext(_factory.getClient(), 
						new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("testMultipleMappingsPerIndex_multi2".toLowerCase(), Optional.empty()),
						new ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext(Optional.of(Arrays.asList("type_1", "type_2", "type_3")), Optional.of("type_"), new HashSet<String>(Arrays.asList("test_random_field")))));		
		{
	
			// Set up the mapping
			
			final TestBean test_long1 = new TestBean();
			test_long1.test_string1 = "test1a.1";
			test_long1.test_map.put("test_map", 1L);
			
			final TestBean test_long2 = new TestBean();
			test_long2.test_string1 = "test1a.2";
			test_long2.test_map.put("test_map", 2L);

			service.storeObjects(Arrays.asList(test_long1, test_long2)).get();
			
			assertEquals(2L, service.countObjects().get().longValue());
			
			// Submit 2 docs, one that will work with "type1", one that won't work
			
			final TestBean test_long3 = new TestBean();
			test_long3.test_string1 = "test1a.3";
			test_long3.test_map.put("test_map", 1L);
			
			
			final TestBean test_string1 = new TestBean();
			test_string1.test_string1 = "test1a.4";
			test_string1.test_map.put("test_map", "test1b");

			final CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> cf = service.storeObjects(Arrays.asList(test_long3, test_string1));
			cf.get();
			
			assertEquals(4L, service.countObjects().get().longValue());
			assertEquals(2, cf.get()._2().get().intValue());

			final BeanTemplate<TestBean> for_queries = BeanTemplateUtils.build(TestBean.class).with("test_map", null).done();
			assertEquals(3L, service.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type_1")).get().intValue());
			assertEquals(1L, service.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type_2")).get().intValue());
			
		}	
		
		// 3) 3rd level of recursion and list in which all messages initially fail
		
		{
			final TestBean test_string1 = new TestBean();
			test_string1.test_string1 = "test1a.4";
			test_string1.test_map.put("test_map", "test1b");

			TestBean test_object1 = new TestBean();
			test_object1.test_string1 = "test1a.5";
			test_object1.test_map.put("test_map", test_string1);

			TestBean test_object2 = new TestBean();
			test_object2.test_string1 = "test1a.6";
			test_object2.test_map.put("test_map", test_string1);
			
			
			final CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> cf = service.storeObjects(Arrays.asList(test_string1, test_object1, test_object2));
			cf.get();
			
			assertEquals(7L, service.countObjects().get().longValue());
			assertEquals(3, cf.get()._2().get().intValue());

			final BeanTemplate<TestBean> for_queries = BeanTemplateUtils.build(TestBean.class).with("test_map", null).done();
			assertEquals(3L, service.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type_1")).get().intValue());
			assertEquals(2L, service.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type_2")).get().intValue());
			assertEquals(2L, service.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type_3")).get().intValue());
		}
		
		// 4) Check reverts to failing if the failing type is designated fix
		
		{
			ElasticsearchCrudService<TestBean> service_2 = getTestService("testMultipleMappingsPerIndex_multi3", TestBean.class,				
				new ElasticsearchContext.ReadWriteContext(_factory.getClient(), 
						new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("testMultipleMappingsPerIndex_multi3".toLowerCase(), Optional.empty()),
						new ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext(Optional.of(Arrays.asList("type_1", "type_2", "type_3")), Optional.of("type_"), new HashSet<>(Arrays.asList("test_map.test_map")))));		
	
			// Set up the mapping
			
			final TestBean test_long1 = new TestBean();
			test_long1.test_string1 = "test1a.1";
			test_long1.test_map.put("test_map", 1L);
			
			final TestBean test_long2 = new TestBean();
			test_long2.test_string1 = "test1a.2";
			test_long2.test_map.put("test_map", 2L);

			service_2.storeObjects(Arrays.asList(test_long1, test_long2)).get();
			
			assertEquals(2L, service_2.countObjects().get().longValue());
			
			// Submit 2 docs, one that will work with "type1", one that won't work
			
			final TestBean test_long3 = new TestBean();
			test_long3.test_string1 = "test1a.3";
			test_long3.test_map.put("test_map", 1L);
			
			
			final TestBean test_string1 = new TestBean();
			test_string1.test_string1 = "test1a.4";
			test_string1.test_map.put("test_map", "test1b");

			final CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> cf = service_2.storeObjects(Arrays.asList(test_long3, test_string1));
			cf.get();
			
			assertEquals(3L, service_2.countObjects().get().longValue());
			assertEquals(1, cf.get()._2().get().intValue());

			final BeanTemplate<TestBean> for_queries = BeanTemplateUtils.build(TestBean.class).with("test_map", null).done();
			assertEquals(3L, service_2.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type_1")).get().intValue());
			assertEquals(0L, service_2.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type_2")).get().intValue());
		}
	}
	
	/////////////////////////////////////////////////////

	@Test
	public void test_MultipleMappingsPerIndex_multiStore_batch() throws InterruptedException, ExecutionException {
		
		// 1) Check fails with mixed mapping
		
		{
			ElasticsearchCrudService<TestBean> service = getTestService("testMultipleMappingsPerIndex_multi1", TestBean.class,				
					new ElasticsearchContext.ReadWriteContext(_factory.getClient(), 
							new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("testMultipleMappingsPerIndex_multi1".toLowerCase(), Optional.empty()),
							new ElasticsearchContext.TypeContext.ReadWriteTypeContext.FixedRwTypeContext("type1")));
	
			@SuppressWarnings("unchecked")
			final ElasticsearchCrudService<TestBean>.ElasticsearchBatchSubsystem batch_service = service.getUnderlyingPlatformDriver(ElasticsearchBatchSubsystem.class, Optional.empty()).get();

			batch_service.setBatchProperties(Optional.empty(), Optional.empty(), Optional.of(Duration.of(1, ChronoUnit.SECONDS)), Optional.empty());			
			
			// Set up the mapping
			
			final TestBean test_long1 = new TestBean();
			test_long1.test_string1 = "test1a.1";
			test_long1.test_map.put("test_map", 1L);
			
			final TestBean test_long2 = new TestBean();
			test_long2.test_string1 = "test1a.2";
			test_long2.test_map.put("test_map", 2L);

			batch_service.storeObjects(Arrays.asList(test_long1, test_long2), true);
			try { Thread.sleep(2200L); } catch (Exception e) {}
			
			assertEquals(2L, service.countObjects().get().longValue());
			
			// Submit 2 docs, one that will work with "type1", one that won't work
			
			final TestBean test_long3 = new TestBean();
			test_long3.test_string1 = "test1a.3";
			test_long3.test_map.put("test_map", 1L);
			
			
			final TestBean test_string1 = new TestBean();
			test_string1.test_string1 = "test1a.4";
			test_string1.test_map.put("test_map", "test1b");

			batch_service.storeObjects(Arrays.asList(test_long3, test_string1), false);			
			try { Thread.sleep(3250L); } catch (Exception e) {}			
			
			assertEquals(3L, service.countObjects().get().longValue());

			final BeanTemplate<TestBean> for_queries = BeanTemplateUtils.build(TestBean.class).with("test_map", null).done();
			assertEquals(3L, service.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type1")).get().intValue());
			assertEquals(0L, service.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type2")).get().intValue());
			
		}
		
		// 2) Check works with mixed mapping when using auto type
		
		ElasticsearchCrudService<TestBean> service = getTestService("testMultipleMappingsPerIndex_multi2", TestBean.class,				
				new ElasticsearchContext.ReadWriteContext(_factory.getClient(), 
						new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("testMultipleMappingsPerIndex_multi2".toLowerCase(), Optional.empty()),
						new ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext(Optional.of(Arrays.asList("type_1", "type_2", "type_3")), Optional.of("type_"), new HashSet<String>(Arrays.asList("test_map")))));
		//(not test_map.test_map)
		
		@SuppressWarnings("unchecked")
		final ElasticsearchCrudService<TestBean>.ElasticsearchBatchSubsystem batch_service = service.getUnderlyingPlatformDriver(ElasticsearchBatchSubsystem.class, Optional.empty()).get();

		batch_service.setBatchProperties(Optional.empty(), Optional.empty(), Optional.of(Duration.of(1, ChronoUnit.SECONDS)), Optional.of(5));			
		
		{

			// Set up the mapping
			
			final TestBean test_long1 = new TestBean();
			test_long1.test_string1 = "test1a.1";
			test_long1.test_map.put("test_map", 1L);
			
			final TestBean test_long2 = new TestBean();
			test_long2.test_string1 = "test1a.2";
			test_long2.test_map.put("test_map", 2L);

			batch_service.storeObjects(Arrays.asList(test_long1, test_long2), true);			
			try { Thread.sleep(4250L); } catch (Exception e) {}
			
			assertEquals(2L, service.countObjects().get().longValue());
			
			// Submit 2 docs, one that will work with "type1", one that won't work
			
			final TestBean test_long3 = new TestBean();
			test_long3.test_string1 = "test1a.3";
			test_long3.test_map.put("test_map", 1L);
			
			
			final TestBean test_string1 = new TestBean();
			test_string1.test_string1 = "test1a.4";
			test_string1.test_map.put("test_map", "test1b");

			batch_service.storeObjects(Arrays.asList(test_long3, test_string1), false);			
			try { Thread.sleep(4250L); } catch (Exception e) {}			
			
			assertEquals(4L, service.countObjects().get().longValue());

			final BeanTemplate<TestBean> for_queries = BeanTemplateUtils.build(TestBean.class).with("test_map", null).done();
			assertEquals(3L, service.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type_1")).get().intValue());
			assertEquals(1L, service.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type_2")).get().intValue());
			
		}	
		
		// 3) 3rd level of recursion and list in which all messages initially fail
		
		{
			final TestBean test_string1 = new TestBean();
			test_string1.test_string1 = "test1a.4";
			test_string1.test_map.put("test_map", "test1b");

			TestBean test_object1 = new TestBean();
			test_object1.test_string1 = "test1a.5";
			test_object1.test_map.put("test_map", test_string1);

			TestBean test_object2 = new TestBean();
			test_object2.test_string1 = "test1a.6";
			test_object2.test_map.put("test_map", test_string1);
			

			batch_service.storeObjects(Arrays.asList(test_string1, test_object1, test_object2), false);			
			try { Thread.sleep(5250L); } catch (Exception e) {}			
						
			assertEquals(7L, service.countObjects().get().longValue());

			final BeanTemplate<TestBean> for_queries = BeanTemplateUtils.build(TestBean.class).with("test_map", null).done();
			assertEquals(3L, service.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type_1")).get().intValue());
			assertEquals(2L, service.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type_2")).get().intValue());
			assertEquals(2L, service.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type_3")).get().intValue());
		}
		
		// 4) Check reverts to failing if the failing type is designated fix
		
		{
			ElasticsearchCrudService<TestBean> service_2 = getTestService("testMultipleMappingsPerIndex_multi3", TestBean.class,				
				new ElasticsearchContext.ReadWriteContext(_factory.getClient(), 
						new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("testMultipleMappingsPerIndex_multi3".toLowerCase(), Optional.empty()),
						new ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext(Optional.of(Arrays.asList("type_1", "type_2", "type_3")), Optional.of("type_"), new HashSet<>(Arrays.asList("test_map.test_map")))));		
	
			final IBatchSubservice<TestBean> batch_service_2 = service_2.getBatchCrudSubservice().get();			
			
			// Set up the mapping
			
			final TestBean test_long1 = new TestBean();
			test_long1.test_string1 = "test1a.1";
			test_long1.test_map.put("test_map", 1L);
			
			final TestBean test_long2 = new TestBean();
			test_long2.test_string1 = "test1a.2";
			test_long2.test_map.put("test_map", 2L);

			batch_service_2.storeObjects(Arrays.asList(test_long1, test_long2));
			try { Thread.sleep(4250L); } catch (Exception e) {}
			
			assertEquals(2L, service_2.countObjects().get().longValue());
			
			// Submit 2 docs, one that will work with "type1", one that won't work
			
			final TestBean test_long3 = new TestBean();
			test_long3.test_string1 = "test1a.3";
			test_long3.test_map.put("test_map", 1L);
			
			
			final TestBean test_string1 = new TestBean();
			test_string1.test_string1 = "test1a.4";
			test_string1.test_map.put("test_map", "test1b");

			batch_service_2.storeObjects(Arrays.asList(test_long3, test_string1));
			try { Thread.sleep(5250L); } catch (Exception e) {}			
			
			assertEquals(3L, service_2.countObjects().get().longValue());

			final BeanTemplate<TestBean> for_queries = BeanTemplateUtils.build(TestBean.class).with("test_map", null).done();
			assertEquals(3L, service_2.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type_1")).get().intValue());
			assertEquals(0L, service_2.countObjectsBySpec(CrudUtils.allOf(for_queries).when("_type", "type_2")).get().intValue());
		}
		
	}
	
}
