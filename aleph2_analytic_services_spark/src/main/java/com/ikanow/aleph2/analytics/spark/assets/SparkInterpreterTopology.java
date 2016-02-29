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

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.repl.SparkIMain;

import scala.Tuple2;







//import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Multimap;
import com.ikanow.aleph2.analytics.spark.data_model.SparkTopologyConfigBean;
import com.ikanow.aleph2.analytics.spark.utils.SparkTechnologyUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
//import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
//import com.ikanow.aleph2.data_model.utils.Lambdas;

//import fj.data.Either;
//import fj.data.Validation;

/** Very simple spark topology, runs the compiled script
 * @author Alex
 */
@SuppressWarnings("unused")
public class SparkInterpreterTopology {

	// Params:
	
	//(not needed)
	//final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
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
			
			final String scala_script = Optional.ofNullable(config.script()).orElse("");
			
			//INFO:
			System.out.println("Starting SparkInterpreterTopology");
			
			SparkConf spark_context = new SparkConf().setAppName("SparkInterpreterTopology");

			test_spec.ifPresent(spec -> System.out.println("OPTIONS: test_spec = " + BeanTemplateUtils.toJson(spec).toString()));
			
			//DEBUG
			//final boolean test_mode = test_spec.isPresent(); // (serializable thing i can pass into the map)
			
			try (final JavaSparkContext jsc = new JavaSparkContext(spark_context)) {
	
				final Multimap<String, JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>> inputs = SparkTechnologyUtils.buildBatchSparkInputs(context, test_spec, jsc, Collections.emptySet());
				
				//TODO move this into a separate module since it's quite big
				/*
				final SparkIMain interpreter = new SparkIMain();
				
				interpreter.bind(
						"inputs",
						"com.google.common.collect.Multimap[String, org.apache.spark.api.java.JavaPairRDD[Object, Tuple2[Long, com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord]]]",
						inputs,
						scala.collection.JavaConversions.asScalaBuffer(Arrays.<String>asList()).toList()
						);
				//TODO aleph2 context etc
				
				//TODO Need to compile user code into a object with a fn taking inputs, contexts, etc and then interpret calling that function 
				//interpreter.compile("scala_script");
				//interpreter.compileString("object Test { def main( args:Array[String] ): Unit = println(inputs.entries().iterator().next().getValue().count()) }");
				//interpreter.interpret("println(inputs.entries().iterator().next().getValue().count())");
				 */				 
				jsc.stop();
				
				//INFO:
				System.out.println("Finished interpreter");
			}
		}
		catch (Throwable t) {
			System.out.println(ErrorUtils.getLongForm("ERROR: {0}", t));
		}
	}
}
