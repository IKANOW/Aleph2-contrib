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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.google.common.collect.Multimap;
import com.ikanow.aleph2.analytics.spark.data_model.SparkTopologyConfigBean;
import com.ikanow.aleph2.analytics.spark.utils.RddDependencyUtils;
import com.ikanow.aleph2.analytics.spark.utils.SparkTechnologyUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.SetOnce;

import fj.data.Either;
import fj.data.Validation;

/** A topology that builds an acyclic graph from the specified pipeline
 * @author Alex
 *
 */
public class BatchEnrichmentPipelineTopology {

	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {

		final SetOnce<IBucketLogger> logger = new SetOnce<>();
		
		try {			
			final Tuple2<IAnalyticsContext, Optional<ProcessingTestSpecBean>> aleph2_tuple = SparkTechnologyUtils.initializeAleph2(args);
			final IAnalyticsContext context = aleph2_tuple._1();
			final Optional<ProcessingTestSpecBean> test_spec = aleph2_tuple._2();

			logger.set(context.getLogger(context.getBucket()));
			
			// Optional: make really really sure it exists after the specified timeout
			SparkTechnologyUtils.registerTestTimeout(test_spec, () -> {
				System.exit(0);
			});
			
			//INFO:
			System.out.println("Starting BatchEnrichmentPipelineTopology");
			
			logger.optional().ifPresent(l -> {
				l.inefficientLog(Level.INFO, ErrorUtils.buildSuccessMessage("BatchEnrichmentPipelineTopology", 
						"main", "Starting BatchEnrichmentPipelineTopology.{0}", Optionals.of(() -> context.getJob().get().name()).orElse("no_name")));
			});			
			
			SparkConf spark_context = new SparkConf().setAppName("BatchEnrichmentPipelineTopology");

			//INFO:
			test_spec.ifPresent(spec -> System.out.println("OPTIONS: test_spec = " + BeanTemplateUtils.toJson(spec).toString()));
			
			final SparkTopologyConfigBean config = BeanTemplateUtils.from(context.getJob().map(job -> job.config()).orElse(Collections.emptyMap()), SparkTopologyConfigBean.class).get();
			
			//DEBUG
			//final boolean test_mode = test_spec.isPresent(); // (serializable thing i can pass into the map)
			
			try (final JavaSparkContext jsc = new JavaSparkContext(spark_context)) {
	
				final Multimap<String, JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>> inputs = SparkTechnologyUtils.buildBatchSparkInputs(context, test_spec, jsc, Collections.emptySet());

				final Validation<String, //(error)
				Tuple2<
				Map<String, Either<JavaRDD<Tuple2<Long, IBatchRecord>>, JavaRDD<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>>>>, //(list of all RDDs)
				Map<String, JavaRDD<Tuple2<Long, IBatchRecord>>> //(just outputs
				>>
				enrichment_pipeline = RddDependencyUtils.buildEnrichmentPipeline(context, jsc, inputs, Optionals.ofNullable(config.enrich_pipeline()));
				
				if (enrichment_pipeline.isFail()) {
					throw new RuntimeException("ERROR: BatchEnrichmentPipelineTopology: " + enrichment_pipeline.fail());
				}
				
				logger.optional().ifPresent(l -> {
					l.inefficientLog(Level.INFO, ErrorUtils.buildSuccessMessage("BatchEnrichmentPipelineTopology", 
							"main", "BatchEnrichmentPipelineTopology.{0}: created pipeline: all={1} outputs={2}", 
							Optionals.of(() -> context.getJob().get().name()).orElse("no_name"),
							enrichment_pipeline.success()._1().keySet(), enrichment_pipeline.success()._2().keySet() 
							));
				});
				
				final Optional<JavaRDD<Tuple2<Long, IBatchRecord>>> all_outputs = 
						enrichment_pipeline.success()._2().values().stream().reduce((acc1, acc2) -> acc1.union(acc2))
						;
				
				long written = all_outputs.map(rdd -> rdd.count()).orElse(-1L);
				
				jsc.stop();
				
				logger.optional().ifPresent(l -> {
					l.inefficientLog(Level.INFO, ErrorUtils.buildSuccessMessage("BatchEnrichmentPipelineTopology", 
							"main", "Stopping BatchEnrichmentPipelineTopology.{0}", Optionals.of(() -> context.getJob().get().name()).orElse("no_name")));
				});
				
				//INFO:
				System.out.println("Wrote: data_objects=" + written);
			}
		}
		catch (Throwable t) {
			logger.optional().ifPresent(l -> {
				l.inefficientLog(Level.ERROR, ErrorUtils.buildSuccessMessage("BatchEnrichmentPipelineTopology", 
						"main", ErrorUtils.getLongForm("Error executing BatchEnrichmentPipelineTopology.unknown: {0}", t)));
			});
			
			System.out.println(ErrorUtils.getLongForm("ERROR: {0}", t));
			logger.optional().ifPresent(Lambdas.wrap_consumer_u(l -> l.flush().get(10, TimeUnit.SECONDS)));
			System.exit(-1);
		}
	}
}
