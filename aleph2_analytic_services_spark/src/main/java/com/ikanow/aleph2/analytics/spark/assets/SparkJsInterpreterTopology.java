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
import java.util.stream.Stream;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;








import scala.Tuple2;














import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.ikanow.aleph2.analytics.spark.data_model.SparkTopologyConfigBean;
import com.ikanow.aleph2.analytics.spark.utils.SparkTechnologyUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.SetOnce;

/** Very simple spark topology, runs the provided JS script
 * @author Alex
 */
@SuppressWarnings("unused")
public class SparkJsInterpreterTopology {

	// Params:
	
	//(not needed)
	//final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {

		final SetOnce<IBucketLogger> bucket_logger = new SetOnce<>();
		final SetOnce<String> job_name = new SetOnce<>(); // (the string we'll use in logging activities)
		try {			
			final Tuple2<IAnalyticsContext, Optional<ProcessingTestSpecBean>> aleph2_tuple = SparkTechnologyUtils.initializeAleph2(args);
			final IAnalyticsContext context = aleph2_tuple._1();
			final Optional<ProcessingTestSpecBean> test_spec = aleph2_tuple._2();

			bucket_logger.set(context.getLogger(context.getBucket()));
			job_name.set(context.getJob().map(j -> j.name()).orElse("no_name"));
			
			// Optional: make really really sure it exists after the specified timeout
			SparkTechnologyUtils.registerTestTimeout(test_spec, () -> {
				System.exit(0);
			});
			
			
			final SparkTopologyConfigBean config = BeanTemplateUtils.from(context.getJob().map(job -> job.config()).orElse(Collections.emptyMap()), SparkTopologyConfigBean.class).get();
			
			final String js_script = Optional.ofNullable(config.script()).orElse("");
			
			//INFO:
			System.out.println("Starting " + job_name.get());
			
			SparkConf spark_context = new SparkConf().setAppName(job_name.get());

			test_spec.ifPresent(spec -> System.out.println("OPTIONS: test_spec = " + BeanTemplateUtils.toJson(spec).toString()));
			
			try (final JavaSparkContext jsc = new JavaSparkContext(spark_context)) {
	
				final Multimap<String, JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>> inputs = SparkTechnologyUtils.buildBatchSparkInputs(context, test_spec, jsc, Collections.emptySet());
				final JavaPairRDD<Object, Tuple2<Long, IBatchRecord>> all_inputs = inputs.values().stream().reduce((acc1, acc2) -> acc1.union(acc2)).orElse(null);				

				// Load globals:
				ScriptEngineManager manager = new ScriptEngineManager();
				ScriptEngine engine = manager.getEngineByName("JavaScript");
				engine.put("_a2_global_context", context);
				engine.put("_a2_global_bucket", context.getBucket().get());
				engine.put("_a2_global_job", context.getJob().get());
				engine.put("_a2_global_config", BeanTemplateUtils.configureMapper(Optional.empty()).convertValue(config, JsonNode.class));
				engine.put("_a2_global_mapper", BeanTemplateUtils.configureMapper(Optional.empty()));
				//TODO (until bucket logger is serializable, don't allow anywhere)
				//engine.put("_a2_bucket_logger", bucket_logger.optional().orElse(null));
				engine.put("_a2_enrichment_name", job_name.get());
				engine.put("_a2_spark_inputs", inputs);
				engine.put("_a2_spark_inputs_all", all_inputs);
				engine.put("_a2_spark_context", jsc);
				
				Stream.concat(config.uploaded_lang_files().stream(), Stream.of("aleph2_js_globals_before.js", ""))
					.flatMap(Lambdas.flatWrap_i(import_path -> {
						try {
							if (import_path.equals("")) { // also import the user script just before here
								return js_script;
							}
							else return IOUtils.toString(SparkJsInterpreterTopology.class.getClassLoader().getResourceAsStream(import_path), "UTF-8");
						}
						catch (Throwable e) {
							bucket_logger.optional().ifPresent(l -> l.log(Level.ERROR, 
									ErrorUtils.lazyBuildMessage(false, () -> SparkJsInterpreterTopology.class.getSimpleName(), 
											() -> job_name.get() + ".main", 
											() -> null, 
											() -> ErrorUtils.get("Error initializing stage {0} (script {1}): {2}", job_name.get(), import_path, e.getMessage()), 
											() -> ImmutableMap.<String, Object>of("full_error", ErrorUtils.getLongForm("{0}", e)))
									));
							
							System.out.println(ErrorUtils.getLongForm("onStageInitialize: {0}", e));
							throw e; // ignored
						}
					}))
					.forEach(Lambdas.wrap_consumer_i(script -> {
						try {
							engine.eval(script);
						}
						catch (Throwable e) {
							bucket_logger.optional().ifPresent(l -> l.log(Level.ERROR, 
									ErrorUtils.lazyBuildMessage(false, () -> SparkJsInterpreterTopology.class.getSimpleName(), 
											() -> job_name.get() + ".main", 
											() -> null, 
											() -> ErrorUtils.get("Error initializing stage {0} (main script): {1}", job_name.get(), e.getMessage()), 
											() -> ImmutableMap.<String, Object>of("full_error", ErrorUtils.getLongForm("{0}", e)))
									));
							
							System.out.println(ErrorUtils.getLongForm("onStageInitialize: {0}", e));		
							throw e; // ignored
						}
					}));
					;
				
				jsc.stop();
				
				//INFO:
				System.out.println("Finished " + job_name.get());
			}
		}
		catch (Throwable t) {
			System.out.println(ErrorUtils.getLongForm("ERROR: {0}", t));
			
			bucket_logger.optional().ifPresent(l -> l.log(Level.ERROR, 
					ErrorUtils.lazyBuildMessage(false, () -> SparkJsInterpreterTopology.class.getSimpleName() + job_name.optional().map(j -> "." + j).orElse(""), 
							() -> job_name.optional().orElse("global") + ".main", 
							() -> null, 
							() -> ErrorUtils.get("Error on batch in job {0}: {1}", job_name.optional().orElse("global") + ".main", t.getMessage()), 
							() -> ImmutableMap.<String, Object>of("full_error", ErrorUtils.getLongForm("{0}", t)))
					));
		}
	}
}
