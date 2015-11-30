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
package com.ikanow.aleph2.v1.document_db.services;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.mapreduce.InputFormat;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.DocumentSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.AnalyticsUtils;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.v1.document_db.utils.V1DocumentDbErrorUtils;
import com.ikanow.aleph2.v1.document_db.utils.V1DocumentDbHadoopUtils;

/** An implementation of the V1 document service
 *  Currently only usable to generate inputs to hadoop processing
 * @author Alex
 */
public class V1DocumentService implements IDocumentService {

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		// Just return myself, everything I need will be shaded into me
		return Arrays.asList(this);
	}

 	/* (non-Javadoc)
 	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
 	 */
 	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {

		if (IAnalyticsAccessContext.class.isAssignableFrom(driver_class)) {
			if (InputFormat.class.isAssignableFrom(AnalyticsUtils.getTypeName((Class<? extends IAnalyticsAccessContext>)driver_class))) { // INPUT FORMAT
				return (Optional<T>) driver_options.map(json -> BeanTemplateUtils.from(json, AnalyticThreadJobBean.AnalyticThreadJobInputBean.class))
						.map(job_input -> V1DocumentDbHadoopUtils.getInputFormat(job_input.get()))
						.map(access_context -> AnalyticsUtils.injectImplementation((Class<? extends IAnalyticsAccessContext>)driver_class, access_context))
						;
			}			
		}
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.DocumentSchemaBean, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public Tuple2<String, List<BasicMessageBean>> validateSchema(
			DocumentSchemaBean schema, DataBucketBean bucket) {
		
		// Can't use v1 in this way
		return Tuples._2T("", 
				Arrays.asList(
						ErrorUtils.buildErrorMessage(this.getClass(), "validateSchema", V1DocumentDbErrorUtils.V1_DOCUMENT_DB_READ_ONLY),
						ErrorUtils.buildErrorMessage(this.getClass(), "validateSchema", V1DocumentDbErrorUtils.V1_DOCUMENT_DB_ANALYTICS_ONLY)
						));
	}
	
	// (leave getDataService to return Optional.empty())
}
