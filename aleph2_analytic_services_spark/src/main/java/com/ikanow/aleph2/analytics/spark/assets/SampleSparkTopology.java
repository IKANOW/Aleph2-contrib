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

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;

/** Very simple spark topology
 * @author Alex
 */
public class SampleSparkTopology {

	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {

		System.out.println("STARTED SPARK JOB: " + args.length);
		if (args.length > 0) {
			System.out.println("ALEPH2 SIGNATURE: " + args[0]);			
		}
		
		try {			
			//TODO: pass this into method so we can check it's serializable...
			//@SuppressWarnings("unused")
			//final IAnalyticsContext context = ContextUtils.getAnalyticsContext(new String(Base64.getDecoder().decode(args[0].getBytes())));
	
			System.out.println("RETRIEVED ALEPH2 CONTEXT");
	
			SparkConf sparkConf = new SparkConf().setAppName("JavaSparkPi");
			try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
	
				int slices = (args.length == 2) ? Integer.parseInt(args[1]) : 2;
				int n = 100000 * slices;
				List<Integer> l = new ArrayList<Integer>(n);
				for (int i = 0; i < n; i++) {
					l.add(i);
				}
		
				JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);
		
				int count = dataSet.map((Integer integer) -> { 
						double x = Math.random() * 2 - 1;
						double y = Math.random() * 2 - 1;
						return (x * x + y * y < 1) ? 1 : 0;
					})
					.reduce((Integer integer, Integer integer2) -> {
						return integer + integer2;
					})
					;
		
				System.out.println("Pi is roughly " + 4.0 * count / n);
		
				jsc.stop();
			}
		}
		catch (Throwable t) {
			System.out.println(ErrorUtils.getLongForm("ERROR: {0}", t));
		}
	}
}
