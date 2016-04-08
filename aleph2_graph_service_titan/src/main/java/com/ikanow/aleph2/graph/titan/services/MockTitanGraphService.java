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
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import scala.Tuple2;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
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
public class MockTitanGraphService implements IGraphService {

	protected final TitanGraph _titan;
	
	@Inject
	public MockTitanGraphService() {
		_titan = TitanFactory.build()
						.set("storage.backend", "inmemory")
						.set("query.force-index", true)
					.open();
		//TODO: set up indices somewhere .. need to write one of those handleBucketUpdate things?
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

}
