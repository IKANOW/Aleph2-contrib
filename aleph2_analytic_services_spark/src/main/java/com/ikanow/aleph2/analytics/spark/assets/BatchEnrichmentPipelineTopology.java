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

package com.ikanow.aleph2.analytics.spark.assets;

import java.util.Collections;
import java.util.Optional;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Multimap;
import com.ikanow.aleph2.analytics.spark.utils.SparkTechnologyUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;

import fj.data.Either;
import fj.data.Validation;

/** A topology that builds an acyclic graph from the specified pipeline
 * @author Alex
 *
 */
public class BatchEnrichmentPipelineTopology {

	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {

		//TODO: actually implement the batch pipeline logic
		
		try {			
			final Tuple2<IAnalyticsContext, Optional<ProcessingTestSpecBean>> aleph2_tuple = SparkTechnologyUtils.initializeAleph2(args);
			final IAnalyticsContext context = aleph2_tuple._1();
			final Optional<ProcessingTestSpecBean> test_spec = aleph2_tuple._2();

			// Optional: make really really sure it exists after the specified timeout
			SparkTechnologyUtils.registerTestTimeout(test_spec, () -> {
				System.exit(0);
			});
			
			//INFO:
			System.out.println("Starting BatchEnrichmentPipelineTopology");
			
			SparkConf spark_context = new SparkConf().setAppName("BatchEnrichmentPipelineTopology");

			//INFO:
			test_spec.ifPresent(spec -> System.out.println("OPTIONS: test_spec = " + BeanTemplateUtils.toJson(spec).toString()));
			
			//DEBUG
			//final boolean test_mode = test_spec.isPresent(); // (serializable thing i can pass into the map)
			
			try (final JavaSparkContext jsc = new JavaSparkContext(spark_context)) {
	
				final Multimap<String, JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>> inputs = SparkTechnologyUtils.buildBatchSparkInputs(context, test_spec, jsc, Collections.emptySet());
				
				final Optional<JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>> input = inputs.values().stream().reduce((acc1, acc2) -> acc1.union(acc2));
				
				long written = input.map(in -> in.values())
						.map(rdd -> {
							return rdd
								.map(t2 -> {
									final Validation<BasicMessageBean, JsonNode> ret_val = context.emitObject(Optional.empty(), context.getJob().get(), Either.left(t2._2().getJson()), Optional.empty());
									return ret_val; // (doesn't matter what I return, just want to count it up)
								})
								//DEBUG: (print the output JSON on success and the error message on fail)
								//.map(val -> test_mode ? val.f().bind(f -> Validation.fail("FAIL: " + f.message())) : val)
								.count();
						})
						.orElse(-1L)
						;
				
				jsc.stop();
				
				//INFO:
				System.out.println("Wrote: data_objects=" + written);
			}
		}
		catch (Throwable t) {
			System.out.println(ErrorUtils.getLongForm("ERROR: {0}", t));
		}
	}
}
