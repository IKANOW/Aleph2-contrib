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

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

/** Titan implementation of the graph service
 * @author Alex
 *
 */
public class MockTitanGraphService extends TitanGraphService {

	@Inject
	public MockTitanGraphService() {
		super(true);
	}
	
	//////////////////////////////////////////////////////////
	
	// DATA SERVICE PROVIDER / GENERIC DATA SERVICE
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider#onPublishOrUpdate(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, boolean, java.util.Set, java.util.Set)
	 */
	@Override
	public CompletableFuture<Collection<BasicMessageBean>> onPublishOrUpdate(
			DataBucketBean bucket, Optional<DataBucketBean> old_bucket,
			boolean suspended, Set<String> data_services,
			Set<String> previous_data_services)
	{
		// (Mock Titan doesn't support indices - so nothing to do here)
		
		//TODO: if previously was graph and now isn't then call handleBucketDeletionRequest
		
		return CompletableFuture.completedFuture(Collections.emptyList()); 
	}
}
