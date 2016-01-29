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
package com.ikanow.aleph2.analytics.spark.assets;

import java.util.Collections;
import java.util.Optional;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import com.ikanow.aleph2.analytics.spark.data_model.SparkTopologyConfigBean;
import com.ikanow.aleph2.analytics.spark.utils.SparkTechnologyUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;

import fj.data.Either;

/** Very simple spark topology, writes out all received objects
 * @author Alex
 */
public class SparkSqlTopology {

	// Params:
	public final static String SUBSAMPLE_NORMAL = "spark.aleph2_subsample";
	public final static String SUBSAMPLE_TEST = "spark.aleph2_subsample_test_override";	
	
	//(not needed)
	final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {

		try {			
			final Tuple2<IAnalyticsContext, Optional<ProcessingTestSpecBean>> aleph2_tuple = SparkTechnologyUtils.initializeAleph2(args);
			final IAnalyticsContext context = aleph2_tuple._1();
			final Optional<ProcessingTestSpecBean> test_spec = aleph2_tuple._2();

			// Optional: make really really sure it exists after the specified timeout
			SparkTechnologyUtils.registerTestTimeout(test_spec, () -> {
				System.exit(0);
			});
			
			final SparkTopologyConfigBean config = BeanTemplateUtils.from(context.getJob().map(job -> job.config()).orElse(Collections.emptyMap()), SparkTopologyConfigBean.class).get();
			final String sql_string = Optional.ofNullable(config.script()).orElse("");
			
			//INFO:
			System.out.println("Starting SparkPassthroughTopology");
			
			SparkConf spark_context = new SparkConf().setAppName("SparkPassthroughTopology");

			final Optional<Double> sub_sample = test_spec
												.map(__ -> Optional.ofNullable(spark_context.getDouble(SUBSAMPLE_TEST, -1)))
												.orElseGet(() -> Optional.ofNullable(spark_context.getDouble(SUBSAMPLE_NORMAL, -1)))
												.filter(d -> d > 0)
												;
			
			//INFO:
			sub_sample.ifPresent(d -> System.out.println("OPTIONS: sub_sample = " + d));
			test_spec.ifPresent(spec -> System.out.println("OPTIONS: test_spec = " + BeanTemplateUtils.toJson(spec).toString()));
			System.out.println("OPTIONS: sql = " + sql_string);
			
			//DEBUG
			//final boolean test_mode = test_spec.isPresent(); // (serializable thing i can pass into the map)
			
			try (final JavaSparkContext jsc = new JavaSparkContext(spark_context)) {
	
				JavaSQLContext sql_context = new JavaSQLContext(jsc);
				
				final Multimap<String, JavaSchemaRDD> inputs = SparkTechnologyUtils.buildBatchSparkSqlInputs(context, test_spec, sql_context, Collections.emptySet());
				
				//INFO
				System.out.println("Registered tables = " + inputs.keySet().toString());
				
				final long written = sql_context.sql(sql_string).map(row -> {
					final JsonNode j = _mapper.createObjectNode().put("message", row.toString());
					return context.emitObject(Optional.empty(), context.getJob().get(), Either.left(j), Optional.empty());
				})
				.count();
				
				//INFO:
				System.out.println("Wrote: data_objects=" + written);
			}
		}
		catch (Throwable t) {
			System.out.println(ErrorUtils.getLongForm("ERROR: {0}", t));
		}
	}
}
