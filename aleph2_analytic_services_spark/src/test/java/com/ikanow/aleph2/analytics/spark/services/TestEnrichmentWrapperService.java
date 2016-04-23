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

package com.ikanow.aleph2.analytics.spark.services;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Tuples;

/**
 * @author Alex
 *
 */
public class TestEnrichmentWrapperService {

	static JavaSparkContext _spark;
	
	@Before
	public void setup() {
		if (null != _spark) {
			return;
		}
		_spark = new JavaSparkContext("local", "TestEnrichmentWrapperService");
	}
	
	@Test
	public void test_groupingBehavior() {
		
		//(quickly test teh whole thing works!)
		{
			JavaRDD<String> test = _spark.parallelize(Arrays.asList("a", "b", "c"));		
			assertEquals(3L, test.map(s -> s + "X").count());		
		}		
		
		// (sampel group)
		{
			JavaRDD<Tuple2<String, String>> test = _spark.parallelize(Arrays.asList(Tuples._2T("a", "resa1"), Tuples._2T("b", "resb1"), Tuples._2T("a", "resa2")));
			assertEquals(2L, test
				.groupBy(t2 -> t2._1())
				.map(key_lt2 -> {
					System.out.println("key=" + key_lt2._1() + ".. vals = " + Optionals.streamOf(key_lt2._2(), false).map(t2 -> t2.toString()).collect(Collectors.joining(";")));
					return null;
				})
				.count()
			);
		}
		
	}
}
