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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.junit.Before;
import org.junit.Test;

import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext;
import com.ikanow.aleph2.shared.crud.elasticsearch.services.ElasticsearchCrudService.CreationPolicy;

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
				Optional.empty(), Optional.empty(), Optional.empty());

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
						new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext(test_name),
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
	public void testMultipleMappingsPerIndex() throws InterruptedException, ExecutionException {
		
		// Using normal type system: should fail
		
		{
			ElasticsearchCrudService<TestBean> service = getTestService("testMultipleMappingsPerIndex_1", TestBean.class,				
					new ElasticsearchContext.ReadWriteContext(_factory.getClient(), 
							new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("testMultipleMappingsPerIndex_1".toLowerCase()),
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
							new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("testMultipleMappingsPerIndex_2".toLowerCase()),
							new ElasticsearchContext.TypeContext.ReadWriteTypeContext.AutoRwTypeContext(Optional.empty(), Optional.empty())));
	
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
	}
	
	
	/////////////////////////////////////////////////////
	
	//TODO when testing store objects, check the case where all the writes fail, to make sure that this still results in full success
	
	
	//TODO: all sorts of things to test...
	
	//TODO: test id ranges with mapping
}
