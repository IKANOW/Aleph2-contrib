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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;

/**Collection of utilities for converting from ES futures to CompletableFutures
 * @author alex
 */
public class ElasticsearchFutureUtils {
	
    /** Wraps an elasticsearch future into a completable one
     * @param es_future - the ES future
     * @param value_generator - a user provided function to generate the output of a successful future
     * @return
     */
    public static <ES, T> CompletableFuture<T> wrap(final ListenableActionFuture<ES> es_future, final Function<ES, T> value_generator)
    {
        final CompletableFuture<T> completableFuture = new CompletableFuture<>();
 
        es_future.addListener(new ActionListener<ES>() {

			@Override
			public void onFailure(Throwable e) {
				completableFuture.completeExceptionally(e);
			}

			@Override
			public void onResponse(ES response) {
				completableFuture.complete(value_generator.apply(response));
			}
			
		});        
        return completableFuture;
    }
	
	
}
