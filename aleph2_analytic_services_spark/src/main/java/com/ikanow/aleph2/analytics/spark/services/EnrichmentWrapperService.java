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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;




import java.util.Optional;

import org.apache.spark.api.java.JavaRDD;














import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;
















import java.util.stream.Stream;

import com.codepoetics.protonpack.StreamUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Iterators;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Tuples;

/** A utility service that runs an Aleph2 enrichment service inside Spark
 * TODO also make something that can run in a map/reduce/etc (make serializable, have inMap method)
 * @author Alex
 *
 */
public class EnrichmentWrapperService implements Serializable {
	private static final long serialVersionUID = 5696574452500014975L;

	//TODO: handle streaming case, different input to pipeline etc
	
	/** A transform/enrichment function that can be used in mapPartitions
	 * @return
	 */
	public FlatMapFunction<Iterator<Tuple2<Long, IBatchRecord>>, Tuple2<Long, IBatchRecord>> inMapPartitions() {
		return it -> {			
			final Wrapper chain_start = new Wrapper();
			
			return StreamUtils.unfold(it, itit -> {
				return itit.hasNext() ? Optional.of(itit) : Optional.empty();
			})
			.map(itit -> Tuples._2T(itit.next(), itit.hasNext()))
			.<Tuple2<Long, IBatchRecord>>flatMap(id_record__islast -> {

				//TODO: set grouping key for initial element
				final Optional<JsonNode> grouping_key = Optional.empty();
				
				return chain_start.process(Arrays.asList(id_record__islast._1()), id_record__islast._2(), grouping_key);				
			})
			::iterator
			;
		};	
	}

	//TODO: map/inMapPartitions(GroupingKey) takes a JavaPairRDD, does all the spawning of new enrichments etc
	
	/** Creates a wrapper for a single enrichment job
	 * @param aleph2_context - the Aleph2 analytic context
	 * @param name - the name of the enrichment pipeline from the job
	 */
	public EnrichmentWrapperService(final IAnalyticsContext aleph2_context, final String name) {
		//TODO: 
	}
	
	/** Creates a wrapper for a pipeline of enrichment control (no grouping/fan in/fan out allowed)
	 * @param aleph2_context - the Aleph2 analytic context
	 * @param name - the name of the enrichment pipeline from the job
	 */
	public EnrichmentWrapperService(final IAnalyticsContext aleph2_context, final List<EnrichmentControlMetadataBean> pipeline_elements) {		
		//TODO: 
	}
	
	/** Maps from one JavaRDD to another via the pipeline of enrichment elements
	 * @param rdd
	 * @return
	 */
	public JavaRDD<Tuple2<Long, IBatchRecord>> map(JavaRDD<Tuple2<Long, IBatchRecord>> rdd) {
		return rdd.mapPartitions(this.inMapPartitions());
	}
	
	/** Utility class to handle a chained list of pipeline elements
	 * @author Alex
	 *
	 */
	protected static class Wrapper {
		protected Wrapper _next;
		protected int _batch_size;
		final protected List<Tuple2<Long, IBatchRecord>> mutable_list = new LinkedList<>();
		
		protected IEnrichmentBatchModule _batch_module;
		
		/** User c'tor
		 * @param - list of enrichment objects
		 */
		protected Wrapper() {
			//TODO
		}
		
		/** Processes the next stage of a chain of pipelines
		 * @param list
		 * @param last - note only called immediately before the cleanup call
		 * @return
		 */
		public Stream<Tuple2<Long, IBatchRecord>> process(final List<Tuple2<Long, IBatchRecord>> list, boolean last, Optional<JsonNode> grouping_key) {
			mutable_list.addAll(list);
			if (last || (mutable_list.size() > _batch_size)) {
				return Optionals.streamOf(Iterators.partition(mutable_list.iterator(), _batch_size), false).<Tuple2<Long, IBatchRecord>>flatMap(l -> {
					
					_batch_module.onObjectBatch(l.stream(), Optional.of(l.size()), grouping_key);
					
					//TODO: handle reduce cases?
					_batch_module.onStageComplete(true); //(TODOD also need to add support for this in batch enrichment)
					//TODO: get list, replacing this:
					final List<Tuple2<Long, IBatchRecord>> stage_output = new LinkedList<>();
					
					final Stream<Tuple2<Long, IBatchRecord>> ret_vals_int = _next.process(stage_output, true, Optional.empty());
					return ret_vals_int;
				})
				;
			}
			else return Stream.empty();
		}
	}
	
}
