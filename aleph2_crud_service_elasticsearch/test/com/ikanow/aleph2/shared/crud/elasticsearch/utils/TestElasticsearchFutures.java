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
package com.ikanow.aleph2.shared.crud.elasticsearch.utils;

import static org.junit.Assert.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

import com.ikanow.aleph2.shared.crud.elasticsearch.services.MockElasticsearchCrudServiceFactory;

public class TestElasticsearchFutures {

	// Mostly this is adequately tested by TestElasticsearchIndexService
	
	// The exception is CompletableFuture<T> wrap(final ListenableActionFuture<ES> es_future, final Function<ES, T> value_generator)
	// which is only called from the search_index_service so needs to be tested here
	// (longer term would be nice to test the other calls explicitly here)
	
	
	@Test
	public void test_basicWrap() throws InterruptedException, ExecutionException {
		
		final MockElasticsearchCrudServiceFactory factory = new MockElasticsearchCrudServiceFactory();		

		// Create an index just so we can delete it (will error if already exists, so just ignore result):

		CompletableFuture<String> s0 = 
				ElasticsearchFutureUtils.wrap(factory.getClient().admin().indices().prepareCreate("index_for_wrap_test").execute(), __ -> "created");
		
		try { s0.get(); } catch (Exception e) {}
		
		// Delete it, check that the wrapper is applied: 

		CompletableFuture<String> s1 = 
				ElasticsearchFutureUtils.wrap(factory.getClient().admin().indices().prepareDelete("index_for_wrap_test").execute(), __ -> "deleted");
		
		assertEquals("deleted", s1.get());
		
		// Now delete it again, check that is fails this time
		
		CompletableFuture<String> s2 = 
				ElasticsearchFutureUtils.wrap(factory.getClient().admin().indices().prepareDelete("index_for_wrap_test").execute(), __ -> "test2");
		
		try {
			s2.get();
			fail("Should have thrown exception here");
		}
		catch (Exception e) {
			// we're done!
		}
		
	}
	
}
