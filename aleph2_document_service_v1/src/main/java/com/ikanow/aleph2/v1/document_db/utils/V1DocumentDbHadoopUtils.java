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
package com.ikanow.aleph2.v1.document_db.utils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.mapreduce.InputFormat;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.v1.document_db.hadoop.assets.Aleph2V1InputFormat;

import fj.data.Either;

/** Utilities to bridge the legacy V1 mongodb service and V2
 * @author Alex
 */
public class V1DocumentDbHadoopUtils {

	/** 
	 * @param input_config - the input settings
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static IAnalyticsAccessContext<InputFormat> getInputFormat(
			final AnalyticThreadJobBean.AnalyticThreadJobInputBean job_input)
	{
		return new IAnalyticsAccessContext<InputFormat>() {
			private LinkedHashMap<String, Object> _mutable_output = null;
			
			@Override
			public String describe() {
				//(return the entire thing)
				return ErrorUtils.get("service_name={0} options={1}", 
						this.getAccessService().right().value().getSimpleName(),
						this.getAccessConfig().get()
						);				
			}
			
			/* (non-Javadoc)
			 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext#getAccessService()
			 */
			@Override
			public Either<InputFormat, Class<InputFormat>> getAccessService() {
				return Either.right((Class<InputFormat>)(Class<?>)Aleph2V1InputFormat.class);
			}

			/* (non-Javadoc)
			 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext#getAccessConfig()
			 */
			@Override
			public Optional<Map<String, Object>> getAccessConfig() {
				if (null != _mutable_output) {
					return Optional.of(_mutable_output);
				}				
				_mutable_output = new LinkedHashMap<>();
											
				// Check for input record limit:
				//TODO:
//				Optional.ofNullable(job_input.config()).map(cfg -> cfg.record_limit_request())
//						.ifPresent(max -> _mutable_output.put(Aleph2V1InputFormat.BE_DEBUG_MAX_SIZE, Long.toString(max)));				

				//TODO: lots of other stuff - this is the bulk of the logic here
				
				
				return Optional.of(Collections.unmodifiableMap(_mutable_output));
			}			
		};
	}
}
