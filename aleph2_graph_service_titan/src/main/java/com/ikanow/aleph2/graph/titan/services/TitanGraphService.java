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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import scala.Tuple2;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.IReadOnlyCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.graph.titan.utils.ErrorUtils;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;

/** Titan implementation of the graph service
 * @author Alex
 *
 */
public class TitanGraphService implements IGraphService, IGenericDataService {

	protected final TitanGraph _titan;
	
	/** Guice injector
=	 */
	@Inject
	public TitanGraphService() {
		//TODO: instead of this line, set up properly from the properties file (or config bean overrides)
		this(true);
		
		//TODO: Ensure that the _b indices are present (and also name/type?)
	}
	
	/** Mock titan c'tor to allow it to use the protected _titan property
	 * @param mock
	 */
	protected TitanGraphService(boolean mock) {
		_titan = TitanFactory.build()
						.set("storage.backend", "inmemory")
						// (inmemory storage back end does not support indices)
					.open();
	}
	
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Arrays.asList(this);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> maybe_driver_options) {
		
		return Patterns.match(driver_class).<Optional<T>>andReturn()
			.when(IEnrichmentBatchModule.class, 
					__ -> maybe_driver_options.map(driver_opts -> driver_opts.equals("com.ikanow.aleph2.analytics.services.GraphBuilderEnrichmentService")).orElse(false),
					__ -> Optional.<T>of((T) new TitanGraphBuilderEnrichmentService()))
			.when(TitanGraph.class, __ -> Optional.<T>of((T) _titan))
			.otherwise(__ -> Optional.empty())
			;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public Tuple2<String, List<BasicMessageBean>> validateSchema(
			GraphSchemaBean schema, DataBucketBean bucket) {
		
		final LinkedList<BasicMessageBean> errors = new LinkedList<>();
		
		if (Optionals.ofNullable(schema.custom_decomposition_configs()).isEmpty()) {
			errors.add(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "validateSchema", ErrorUtils.DECOMPOSITION_ENRICHMENT_NEEDED, bucket.full_name()));
		}
		if (Optionals.ofNullable(schema.custom_merge_configs()).isEmpty()) {
			errors.add(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "validateSchema", ErrorUtils.MERGE_ENRICHMENT_NEEDED, bucket.full_name()));
		}		
		return Tuples._2T("",  errors);
	}

	//////////////////////////////////////////////////////////
	
	// DATA SERVICE PROVIDER / GENERIC DATA SERVICE
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider#getDataService()
	 */
	@Override
	public Optional<IGenericDataService> getDataService() {
		return Optional.of(this);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider#onPublishOrUpdate(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, boolean, java.util.Set, java.util.Set)
	 */
	@Override
	public CompletableFuture<Collection<BasicMessageBean>> onPublishOrUpdate(
			DataBucketBean bucket, Optional<DataBucketBean> old_bucket,
			boolean suspended, Set<String> data_services,
			Set<String> previous_data_services)
	{
		//TODO set up indices
		
		//TODO: if previously was graph and now isn't then call handleBucketDeletionRequest
		
		return CompletableFuture.completedFuture(Collections.emptyList()); 
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getWritableDataService(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, java.util.Optional)
	 */
	@Override
	public <O> Optional<IDataWriteService<O>> getWritableDataService(
			Class<O> clazz, DataBucketBean bucket, Optional<String> options,
			Optional<String> secondary_buffer) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getReadableCrudService(java.lang.Class, java.util.Collection, java.util.Optional)
	 */
	@Override
	public <O> Optional<IReadOnlyCrudService<O>> getReadableCrudService(
			Class<O> clazz, Collection<DataBucketBean> buckets,
			Optional<String> options) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getUpdatableCrudService(java.lang.Class, java.util.Collection, java.util.Optional)
	 */
	@Override
	public <O> Optional<ICrudService<O>> getUpdatableCrudService(
			Class<O> clazz, Collection<DataBucketBean> buckets,
			Optional<String> options) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getSecondaryBuffers(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
	 */
	@Override
	public Set<String> getSecondaryBuffers(DataBucketBean bucket,
			Optional<String> intermediate_step) {
		return Collections.emptySet();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getPrimaryBufferName(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
	 */
	@Override
	public Optional<String> getPrimaryBufferName(DataBucketBean bucket,
			Optional<String> intermediate_step) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#switchCrudServiceToPrimaryBuffer(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> switchCrudServiceToPrimaryBuffer(
			DataBucketBean bucket, Optional<String> secondary_buffer,
			Optional<String> new_name_for_ex_primary,
			Optional<String> intermediate_step) {
		return CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "switchCrudServiceToPrimaryBuffer", ErrorUtils.BUFFERS_NOT_SUPPORTED, bucket.full_name()));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#handleAgeOutRequest(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> handleAgeOutRequest(
			DataBucketBean bucket) {
		// TODO Auto-generated method stub
		return CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "handleAgeOutRequest", ErrorUtils.NOT_YET_IMPLEMENTED, "handleAgeOutRequest"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#handleBucketDeletionRequest(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, boolean)
	 */
	@Override
	public CompletableFuture<BasicMessageBean> handleBucketDeletionRequest(
			DataBucketBean bucket, Optional<String> secondary_buffer,
			boolean bucket_or_buffer_getting_deleted) {
		// TODO Auto-generated method stub
		return CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage(this.getClass().getSimpleName(), "handleBucketDeletionRequest", ErrorUtils.NOT_YET_IMPLEMENTED, "handleBucketDeletionRequest"));
	}

}
