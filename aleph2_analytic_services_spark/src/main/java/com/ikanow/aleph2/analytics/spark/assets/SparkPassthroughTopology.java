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
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.analytics.hadoop.assets.BeFileInputReader;
import com.ikanow.aleph2.analytics.hadoop.utils.HadoopTechnologyUtils;
import com.ikanow.aleph2.analytics.spark.utils.SparkTechnologyUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Tuples;

import fj.data.Either;

/** Very simple spark topology, writes out all received objects
 * @author Alex
 */
public class SparkPassthroughTopology {

	/**/
	final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {

		try {			
			final IAnalyticsContext context = ContextUtils.getAnalyticsContext(new String(Base64.getDecoder().decode(args[0].getBytes())));
	
			/**/
			System.out.println("(here 3)");
			
			SparkConf spark_context = new SparkConf().setAppName("SparkPassthroughTopology");
			try (JavaSparkContext jsc = new JavaSparkContext(spark_context)) {
	
				/**/
				final List<String> paths = context.getInputPaths(Optional.empty(), context.getJob().get(), context.getJob().get().inputs().stream().findAny().get());
				/**/
//				JavaRDD<String> input = jsc.textFile(paths.stream().findFirst().get());
				//JavaPairRDD<String, String> input = jsc.newAPIHadoopFile(paths.stream().findAny().get(), CombineFileInputFormat.class, String.class, String.class, HadoopTechnologyUtils.getHadoopConfig(context.getServiceContext().getGlobalProperties()));
				final Configuration config = HadoopTechnologyUtils.getHadoopConfig(context.getServiceContext().getGlobalProperties());
			    final Job inputJob = Job.getInstance(config);
			    inputJob.setInputFormatClass(BeFileInputFormat_Pure.class);
			    //mapreduce.input.fileinputformat.inputdir
				paths.stream().forEach(Lambdas.wrap_consumer_u(path -> FileInputFormat.addInputPath(inputJob, new Path(path))));
				
				/**/
				paths.stream().forEach(Lambdas.wrap_consumer_u(path -> System.out.println("??? " + path)));
				
//				@SuppressWarnings("unchecked")
//				JavaPairRDD<Object, Tuple2<Long, IBatchRecord>> input = jsc.newAPIHadoopFile(paths.stream().findAny().get(), BeFileInputFormat_Pure.class, Object.class, (Class<Tuple2<Long, IBatchRecord>>)(Class<?>)Tuple2.class, config);
				@SuppressWarnings("unchecked")
				JavaPairRDD<String, Tuple2<Long, IBatchRecord>> input = jsc.newAPIHadoopRDD(inputJob.getConfiguration(), BeFileInputFormat_Pure.class, String.class, (Class<Tuple2<Long, IBatchRecord>>)(Class<?>)Tuple2.class);
//				@SuppressWarnings("unchecked")
//				JavaRDD<Tuple2<Long, IBatchRecord>> input = 
//					jsc.newAPIHadoopFile(paths.stream().findAny().get(), BeFileInputFormat_Pure.class, String.class, (Class<Tuple2<Long, IBatchRecord>>)(Class<?>)Tuple2.class, config)
//						.values()
//						//.map(t2 -> t2._2())
//						;

				//TODO: note in BeFileInput Reader I handled the case where the signature isn't set in the config (also may not be an enrichment context... really shouldn't use the context at all)
				
				/**/
				//OK this doesn't work and understand why...
				// prolbme is https://github.com/apache/spark/blob/branch-1.2/core/src/main/scala/org/apache/spark/SerializableWritable.scala
				//JavaPairRDD<Object, Tuple2<Long, IBatchRecord>> input = SparkTechnologyUtils.getCombinedInput(context, jsc);
				
				long written = input
						.values()
/**/						
						.sample(true, 0.01) //TODO: add this as an option for passthrough module
/**/					//.map(t2 -> Tuples._2T(0L, new BeFileInputReader.BatchRecord(_mapper.createObjectNode().put("test", t2), null)))
//					.map(t2 -> Tuples._2T(0L, new BeFileInputReader.BatchRecord(_mapper.createObjectNode().put("test", t2._2()), null)))
					
//					.map(t2 -> context.emitObject(Optional.empty(), context.getJob().get(), Either.left(t2._2()._2().getJson()), Optional.empty()))					
					.map(t2 -> context.emitObject(Optional.empty(), context.getJob().get(), Either.left(t2._2().getJson()), Optional.empty()))
						//_mapper.convertValue(arg0, arg1)						
/**/						
//.map(t2 -> {
////	final HashMap<String, Object> val = new HashMap<String, Object>();
////	val.put("val", "test");	
////	context.emitObject(Optional.empty(), context.getJob().get(), Either.right(val), Optional.empty());
//	context.emitObject(Optional.empty(), context.getJob().get(), Either.left(_mapper.createObjectNode().put("test", t2)), Optional.empty());
//	return "test";
//})						
					.count()
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
