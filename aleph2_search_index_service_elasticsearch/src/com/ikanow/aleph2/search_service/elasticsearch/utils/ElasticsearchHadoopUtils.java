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
package com.ikanow.aleph2.search_service.elasticsearch.utils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.mapreduce.InputFormat;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.search_service.elasticsearch.hadoop.assets.Aleph2EsInputFormat;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext;

import fj.data.Either;

/** Utilities to build an ES input format for Aleph2
 * @author Alex
 */
public class ElasticsearchHadoopUtils {

	/** 
	 * @param input_config - the input settings
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static IAnalyticsAccessContext<InputFormat> getInputFormat(
			final AnalyticThreadJobBean.AnalyticThreadJobInputBean job_input)
	{
		return new IAnalyticsAccessContext<InputFormat>() {

				/* (non-Javadoc)
				 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext#getAccessService()
				 */
				@Override
				public Either<InputFormat, Class<InputFormat>> getAccessService() {
					return Either.right((Class<InputFormat>)(Class<?>)Aleph2EsInputFormat.class);
				}

				/* (non-Javadoc)
				 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext#getAccessConfig()
				 */
				@Override
				public Optional<Map<String, Object>> getAccessConfig() {

					final LinkedHashMap<String, Object> mutable_output = new LinkedHashMap<>();
												
					//TODO (XXX): going to start off with a simple version of this:

					mutable_output.put("es.resource", ElasticsearchContext.READ_PREFIX + ElasticsearchIndexUtils.getBaseIndexName(
							BeanTemplateUtils.build(DataBucketBean.class)
								.with(DataBucketBean::full_name, job_input.resource_name_or_id())
							.done().get()
							, 
							Optional.empty()) + "*");
					//TODO (XXX): support multi-buckets / buckets with non-standard indexes ... also use the tmin/tmax
					// (needs MDB to pull out)
					
					mutable_output.put("es.index.read.missing.as.empty", "yes");
					
					mutable_output.put("es.query",
							Optional.ofNullable(job_input.filter())
										.map(f -> f.get("technology_override"))
										.map(o -> {
											return (o instanceof String)
												? o.toString()
												: BeanTemplateUtils.from(o).toString()
												;
										})
										.orElse("?q=*"));
					// (incorporate tmin/tmax and also add a JSON mapping for the Aleph2 crud utils)
					
					// Here are the parameters that can be set:
					// es.query ... can be stringified JSON or a q=string .... eg conf.set("es.query", "?q=me*");  
					//config.set("es.resource", overallIndexNames.toString()); .. .this was in the format X,Y,Z[/type],,etc which then got copied to 
					// create a simple multi-input format .. looks like i didn't do anything if no type was set, unclear if that was an optimization
					// or if it doesn't work... (if it doesn't work then what you have to do is get the mappings for each index and
					// get the types and insert them all)
					//config.set("es.index.read.missing.as.empty", "yes");

					// (not sure if need to set just normal http port/host?)
					//config.set("es.net.proxy.http.host", "localhost");
					//config.set("es.net.proxy.http.port", "8888");


					// TODO Auto-generated method stub
					return Optional.of(Collections.unmodifiableMap(mutable_output));
				}			
			};
	}
}
