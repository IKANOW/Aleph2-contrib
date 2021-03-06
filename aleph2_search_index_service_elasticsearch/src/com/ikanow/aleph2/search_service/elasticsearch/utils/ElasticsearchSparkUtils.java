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

package com.ikanow.aleph2.search_service.elasticsearch.utils;

import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import com.google.common.collect.Multimap;
import com.ikanow.aleph2.core.shared.utils.TimeSliceDirUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.search_service.elasticsearch.hadoop.assets.Aleph2EsInputFormat;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext;

import fj.data.Either;

/** Utilities to build an ES spark SQL for Aleph2
 * @author Alex
 */
public class ElasticsearchSparkUtils {

	/** 
	 * @param input_config - the input settings
	 * @return
	 */
	public static IAnalyticsAccessContext<DataFrame> getDataFrame(
			final Client client,
			final AnalyticThreadJobBean.AnalyticThreadJobInputBean job_input)
	{
		final SetOnce<Map<String, String>> _es_options = new SetOnce<>();
		
		return new IAnalyticsAccessContext<DataFrame>() {
			
			@Override
			public String describe() {
				//(return the entire thing)
				return ErrorUtils.get("service_name={0} options={1}", 
						this.getAccessService().right().value().getSimpleName(),
						_es_options.optional()
						);				
			}
			
			/* (non-Javadoc)
			 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext#getAccessService()
			 */
			@SuppressWarnings("unchecked")
			@Override
			public Either<DataFrame, Class<DataFrame>> getAccessService() {
				return Either.right((Class<DataFrame>)(Class<?>)DataFrame.class);
			}

			@Override
			public Optional<Map<String, Object>> getAccessConfig() {
				
				// OK this is horrible but we're going to return a map of lambdas SparkContext -> SchemaRDD
				
				//TODO (XXX): going to start off with a simple version of this:

				final String index_resource = 
						ElasticsearchContext.READ_PREFIX + ElasticsearchIndexUtils.getBaseIndexName(
							BeanTemplateUtils.build(DataBucketBean.class)
								.with(DataBucketBean::full_name, job_input.resource_name_or_id())
							.done().get()
							, 
							Optional.empty()) 
						+ "*";
				
				//TODO (ALEPH-72): support multi-buckets / buckets with non-standard indexes ... also use the tmin/tmax
				// (needs MDB to pull out - because need to get the full bucket ugh)
				
				// Currently need to add types: 
				//TODO (ALEPH-72): from elasticsearch-hadoop 2.2.0.m2 this will no longer be necessary (currently at 2.2.0.m1)
				final Multimap<String, String> index_type_mapping = ElasticsearchIndexUtils.getTypesForIndex(client, index_resource);				
				final String type_resource = index_type_mapping.values().stream().collect(Collectors.toSet()).stream().collect(Collectors.joining(","));
				final String final_index = ElasticsearchHadoopUtils.getTimedIndexes(job_input, index_type_mapping, new Date())
						.map(s -> Stream.concat(s, TimeSliceDirUtils.getUntimedDirectories(index_type_mapping.keySet().stream()))
									.collect(Collectors.joining(",")))
					.orElse(index_resource);						
				
				//TODO (ALEPH-72): handle single/multiple types
				
				final Map<String, String> es_options = 
						ImmutableMap.<String, String>of(
								"es.index.read.missing.as.empty", "yes"
								,
								"es.read.metadata", "true",
								"es.read.metadata.field", Aleph2EsInputFormat.ALEPH2_META_FIELD								
								,
								"es.resource", final_index + "/" + type_resource								
								);
				
				_es_options.set(es_options);
				final String table_name = Optional.ofNullable(job_input.name()).orElse(BucketUtils.getUniqueSignature(job_input.resource_name_or_id(), Optional.empty()));

				Function<SQLContext, DataFrame> f = sql_context -> {					
					final DataFrame df = JavaEsSparkSQL.esDF(sql_context, es_options);
					df.registerTempTable(table_name);
					return df;
				}
				;
				return Optional.of(ImmutableMap.of(table_name, (Object)f));
			}
		};
	}
}
