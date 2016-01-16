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

import java.util.Base64;
import java.util.Optional;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.google.common.collect.Multimap;
import com.ikanow.aleph2.analytics.spark.utils.SparkTechnologyUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;

import fj.data.Either;

/** Very simple spark topology, writes out all received objects
 * @author Alex
 */
public class SparkPassthroughTopology {

	//(not needed)
	//final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {

		try {			
			final IAnalyticsContext context = ContextUtils.getAnalyticsContext(new String(Base64.getDecoder().decode(args[0].getBytes())));

			//INFO:
			System.out.println("Starting SparkPassthroughTopology");
			
			SparkConf spark_context = new SparkConf().setAppName("SparkPassthroughTopology");
			
			try (JavaSparkContext jsc = new JavaSparkContext(spark_context)) {
	
				final Multimap<String, JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>> inputs = SparkTechnologyUtils.buildBatchSparkInputs(context, jsc);
				
				final Optional<JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>> input = inputs.values().stream().reduce((acc1, acc2) -> acc1.union(acc2));
				
				long written = 
					input.map(in -> in
						.values()
						.sample(true, 0.01) //TODO: add this as an option for passthrough module
						.map(t2 -> context.emitObject(Optional.empty(), context.getJob().get(), Either.left(t2._2().getJson()), Optional.empty()))
						.count())
					.orElse(-1L)
					;
				
				jsc.stop();
				
				System.out.println("Wrote: data_objects=" + written);
			}
		}
		catch (Throwable t) {
			System.out.println(ErrorUtils.getLongForm("ERROR: {0}", t));
		}
	}
}
