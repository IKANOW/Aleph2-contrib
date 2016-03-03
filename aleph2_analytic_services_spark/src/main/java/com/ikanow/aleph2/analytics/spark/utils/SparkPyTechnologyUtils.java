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

package com.ikanow.aleph2.analytics.spark.utils;

import org.apache.spark.api.java.JavaSparkContext;

import com.ikanow.aleph2.analytics.spark.services.SparkPyWrapperService;

/** This class acts as a wrapper for Spark/Python related functionality
 * @author Alex
 *
 */
public class SparkPyTechnologyUtils {
	
	/** Returns a spark wrapper for use by python
	 * @param spark_context
	 * @param signature
	 * @param test_signature
	 * @return
	 */
	public static SparkPyWrapperService getAleph2(JavaSparkContext spark_context, String signature, String test_signature) {
		return new SparkPyWrapperService(spark_context, signature, test_signature);
	}
	
	/** Returns a spark wrapper for use by python
	 * @param spark_context
	 * @param signature
	 * @return
	 */
	public static SparkPyWrapperService getAleph2(JavaSparkContext spark_context, String signature) {
		return new SparkPyWrapperService(spark_context, signature);
	}
	
}
