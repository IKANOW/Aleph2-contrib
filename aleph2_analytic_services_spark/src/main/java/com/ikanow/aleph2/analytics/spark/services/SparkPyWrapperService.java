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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import com.ikanow.aleph2.analytics.spark.utils.SparkTechnologyUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;

import fj.data.Either;

/** This class contains simple easy-to-invoke-from-py4j wrappers for the spark functionality 
 * @author Alex
 *
 */
public class SparkPyWrapperService {
	protected final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	final JavaSparkContext _spark_context;
	final IAnalyticsContext _aleph2_context;
	final Optional<ProcessingTestSpecBean> _test_spec_bean;
	
	final SetOnce<Multimap<String, JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>>> _rdd_set = new SetOnce<>();
	
	/** Creates a spark wrapper object (with test spec)
	 * @param spark_context
	 * @param signature
	 */
	public SparkPyWrapperService(JavaSparkContext spark_context, String signature) {
		this(spark_context, signature, null);
	}
	
	/** Creates a spark wrapper object (with test spec)
	 * @param spark_context
	 * @param signature
	 * @param test_signature
	 */
	public SparkPyWrapperService(JavaSparkContext spark_context, String signature, String test_signature) {
		_spark_context = spark_context;
		try {
			final Tuple2<IAnalyticsContext, Optional<ProcessingTestSpecBean>> t2 = 
					SparkTechnologyUtils.initializeAleph2(((null == test_signature) ? Stream.of(signature) : Stream.of(signature, test_signature)).toArray(String[]::new));
			
			_aleph2_context = t2._1();
			_test_spec_bean = t2._2();
		}
		catch (Exception e) {
			throw new RuntimeException();
		}
	}
	
	///////////////////////////////////////////////////////
	
	// INPUTS - NORMAL RDDs
	
	/** Get a list of available RDDs
	 * @return
	 */
	public Set<String> getRddInputNames() {
		initializeRdd(Collections.emptySet());
		
		return _rdd_set.get().keySet();
	}
	
	/** Get a named RDD
	 *  (NOTE: will join RDDs of the same name together)
	 * @param input_name
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public JavaRDD<Map<String, Object>> getRddInput(final String input_name) {
		initializeRdd(Collections.emptySet());
		
		return _rdd_set.get().get(input_name).stream().reduce((acc1, acc2) -> acc1.union(acc2))
						.map(rdd -> rdd.<Map<String, Object>>map(t2 -> _mapper.convertValue(t2._2()._2().getJson(), Map.class))).orElse(null);
	}
	
	/** Returns a union of all the types (for convenience)
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public JavaRDD<Map<String, Object>> getAllRddInputs() {
		initializeRdd(Collections.emptySet());
		
		return _rdd_set.get().values().stream().reduce((acc1, acc2) -> acc1.union(acc2))
				.map(rdd -> rdd.<Map<String, Object>>map(t2 -> _mapper.convertValue(t2._2()._2().getJson(), Map.class))).orElse(null);
	}

	///////////////////////////////////////////////////////
	
	// INPUTS - SPARK-SQL
	
	//TODO
	
	///////////////////////////////////////////////////////
	
	// OUTPUTS
	
	//TODO: need an emitRdd that works on a pair and thus lets me emit vs external emit as i please...
	
	public long emitRdd(final JavaRDD<Map<String, Object>> output) {
		return output.map(map -> _aleph2_context.emitObject(Optional.empty(), _aleph2_context.getJob().get(), Either.right(map), Optional.empty())).count();
	}
	
	public long externalEmitRdd(final String path, final JavaRDD<Map<String, Object>> output) {
		final DataBucketBean emit_bucket = BeanTemplateUtils.build(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.class)
				.with("full_name", path)
			.done().get();
		
		return output.map(map -> _aleph2_context.emitObject(Optional.of(emit_bucket), _aleph2_context.getJob().get(), Either.right(map), Optional.empty())).count();
	}
	
	public boolean emitObject(final Map<String, Object> map) {
		return _aleph2_context.emitObject(Optional.empty(), _aleph2_context.getJob().get(), Either.right(map), Optional.empty()).isSuccess();
	}
	
	public boolean externalEmitObject(final String path, final Map<String, Object> map) {
		final DataBucketBean emit_bucket = BeanTemplateUtils.build(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.class)
				.with("full_name", path)
			.done().get();
		
		return _aleph2_context.emitObject(Optional.of(emit_bucket), _aleph2_context.getJob().get(), Either.right(map), Optional.empty()).isSuccess();
	}
	
	///////////////////////////////////////////////////////
	
	// UTILS
	
	/** Initialize the RDDs
	 * @param exclude
	 */
	private void initializeRdd(final Set<String> exclude) {
		if (_rdd_set.isSet()) {
			_rdd_set.set(SparkTechnologyUtils.buildBatchSparkInputs(_aleph2_context, _test_spec_bean, _spark_context, exclude));			
		}
	}
	
}
