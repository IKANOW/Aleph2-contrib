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
import java.util.function.BiConsumer;
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
	
    /** Wraps an elasticsearch future into a completable one, supporting user-defined action on failure
     * @param es_future - the ES future
     * @param value_generator - a user provided function to generate the output of a successful future
     * @param error_handler - a user provided function that takes the error and the returned future and can eg resubmit the attempt with different params
     * @return - the ES future converted to a Java8 CF and "decorated" with user defined success/failure actions
     */
    public static <ES, T> CompletableFuture<T> wrap(final ListenableActionFuture<ES> es_future, 
    												final Function<ES, T> value_generator,
    												final BiConsumer<Throwable, CompletableFuture<T>> error_handler
    												)
    {
        final CompletableFuture<T> completableFuture = new CompletableFuture<>();
 
        es_future.addListener(new ActionListener<ES>() {

			@Override
			public void onFailure(Throwable e) {				
				error_handler.accept(e, completableFuture);
			}

			@Override
			public void onResponse(ES response) {
				completableFuture.complete(value_generator.apply(response));
			}
			
		});        
        return completableFuture;
    }
	
    /** Wraps an elasticsearch future into a completable one, supporting user-defined action on success and failure
     * @param es_future - the ES future
     * @param value_generator - a user provided function that takes the success response and the returned future and can eg resubmit the attempt with different params (eg on partial failure)
     * @param error_handler - a user provided function that takes the error and the returned future and can eg resubmit the attempt with different params
     * @return - the ES future converted to a Java8 CF and "decorated" with user defined success/failure actions
     */
    public static <ES, T> CompletableFuture<T> wrap(final ListenableActionFuture<ES> es_future, 
    												final CompletableFuture<T> original_future,
    												final BiConsumer<ES, CompletableFuture<T>> success_handler,
    												final BiConsumer<Throwable, CompletableFuture<T>> error_handler
    												)
    {
        es_future.addListener(new ActionListener<ES>() {

			@Override
			public void onFailure(Throwable e) {
				error_handler.accept(e, original_future);
			}

			@Override
			public void onResponse(ES response) {
				success_handler.accept(response, original_future);
			}
			
		});        
		return original_future;
    }
}
