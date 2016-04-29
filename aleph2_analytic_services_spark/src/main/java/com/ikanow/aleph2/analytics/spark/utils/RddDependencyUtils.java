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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.collect.Multimap;
import com.ikanow.aleph2.analytics.spark.services.EnrichmentPipelineService;
import com.ikanow.aleph2.core.shared.utils.DependencyUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Tuples;

import fj.data.Either;
import fj.data.Validation;

/** Encapsulates some of the unpleasant logic that converts the enricher contains into a connected set of RDDs
 * @author Alex
 *
 */
public class RddDependencyUtils {

	/** Builds an RDD pipeline
	 * @param inputs
	 * @param enrichment_pipeline_config
	 * @return a validation, if successful containing (all generated rdds, output rdds only) - normally only the second tuple is needed
	 */
	public static Validation<String, //(error)
				Tuple2<
				Map<String, Either<JavaRDD<Tuple2<Long, IBatchRecord>>, JavaRDD<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>>>>, //(list of all RDDs)
				Map<String, JavaRDD<Tuple2<Long, IBatchRecord>>> //(just outputs
				>
	>
	buildEnrichmentPipeline(
			final IAnalyticsContext context,
			final JavaSparkContext jsc,
			final Multimap<String, JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>> inputs,
			final Collection<EnrichmentControlMetadataBean> enrichment_pipeline_config
			)
	{
		// Build the pipeline
		final Validation<String, LinkedHashMap<String, Tuple2<Set<String>, List<EnrichmentControlMetadataBean>>>> maybe_enrichment_pipeline =
				DependencyUtils.buildPipelineOfContainers(inputs.keySet(), enrichment_pipeline_config);
		
		return maybe_enrichment_pipeline.bind(enrichment_pipeline -> {
			
			// (2 types of RDD - before and after...)
			final HashMap<String, Either<JavaRDD<Tuple2<Long, IBatchRecord>>, JavaRDD<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>>>> mutable_rdds = new HashMap<>();
			
			// Insert all the inputs:
			inputs.asMap().entrySet().stream().forEach(kv ->				
				mutable_rdds.put(kv.getKey(), 
						Either.left(kv.getValue().stream().reduce((acc1, acc2) -> acc1.union(acc2)).get().map(t2 -> t2._2())))
			);
			
			// First pass, find all the groupings:
			// (if _any_ immediately downstream element needs grouping then treat as all do and map the extra element away)
			final Map<String, Collection<String>> jobs_that_need_to_group =
				enrichment_pipeline.values().stream().distinct()
					.<Tuple2<String, Collection<String>>>flatMap(t2 -> {
						return t2._2().stream().findFirst()
								.map(e -> Optionals.ofNullable(e.grouping_fields()))
								.<Stream<Tuple2<String, Collection<String>>>>map(groupings -> t2._1().stream().map(input -> Tuples._2T(input, groupings)))
								.orElseGet(Stream::empty);
					})
					.collect(Collectors.toMap(t2 -> t2._1(), t2 -> t2._2()))
					;
			
			// Second pass, do we need $inputs:
			if (enrichment_pipeline.values().stream().distinct().anyMatch(t2 -> t2._1().contains(EnrichmentControlMetadataBean.PREVIOUS_STEP_ALL_INPUTS))) {
				inputs.put(EnrichmentControlMetadataBean.PREVIOUS_STEP_ALL_INPUTS, 
						inputs.values().stream().reduce((acc1, acc2) -> acc1.union(acc2)).orElse(jsc.emptyRDD().flatMapToPair(__->null))
						);
			}
			
			// Third/forth pass, create another mutable state that tells us which enrichers are the furthest downstream
			final Set<String> mutable_enricher_set = new HashSet<>(enrichment_pipeline.values().stream().distinct()
				.<EnrichmentControlMetadataBean>flatMap(t2 -> StreamUtils.stream(t2._2().stream().findFirst())) // (discard inputs)
				.map(e -> e.name())
				.collect(Collectors.toSet()))
				;
			enrichment_pipeline.values().stream().distinct()
				.forEach(t2 -> {
					final EnrichmentControlMetadataBean control = t2._2().stream().findFirst().get();
					mutable_enricher_set.removeAll(control.dependencies());					
				});
			
			
			// Fifth (!) pass actually does all the work:
			
			enrichment_pipeline.values().stream().distinct()
				.filter(t2 -> t2._2().stream().findFirst().isPresent()) // (discard inputs)
				.forEach(t2 -> {
					final EnrichmentControlMetadataBean control = t2._2().stream().findFirst().get();
					final boolean upstream_is_grouped = !Optionals.ofNullable(control.grouping_fields()).isEmpty();
					final Collection<String> downstream_grouping = jobs_that_need_to_group.get(control.name());
					final boolean downstream_is_grouped = null != downstream_grouping;
							
					final boolean to_emit = mutable_enricher_set.contains(control.name());
					
					// Get all the inputs:
					// 4 cases depending on whether upstream/downstream are grouped
	
					if (upstream_is_grouped) {
						// (ignore any inputs that haven't been grouped)
						final JavaRDD<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>> rdd_inputs = 
								t2._1().stream().map(dep -> mutable_rdds.get(dep))
								.filter(rdd_choice -> rdd_choice.isRight())
								.map(rdd_choice -> rdd_choice.right().value())
								.reduce((acc1, acc2) -> acc1.union(acc2)).orElseGet(() -> jsc.emptyRDD())
								;
						
						if (!downstream_is_grouped) {
							mutable_rdds.put(control.name(), Either.left(
									EnrichmentPipelineService.javaGroupOf(rdd_inputs)
											.mapPartitions(EnrichmentPipelineService.create(context, to_emit, t2._2()).javaInMapPartitionsPostGroup())
							))
							;
						}
						else {
							mutable_rdds.put(control.name(), Either.right(
									EnrichmentPipelineService.javaGroupOf(rdd_inputs)
											.mapPartitions(EnrichmentPipelineService.create(context, to_emit, t2._2()).javaInMapPartitionsPrePostGroup(new ArrayList<>(downstream_grouping)))
							))
							;								
						}
					}
					else {
						// (convert any grouped inputs to ungrouped)
						final JavaRDD<Tuple2<Long, IBatchRecord>> rdd_inputs = 
								t2._1().stream().map(dep -> mutable_rdds.get(dep))
									.map(rdd_choice -> 
										rdd_choice.<JavaRDD<Tuple2<Long, IBatchRecord>>>either(
												ungrouped -> ungrouped,
												grouped -> grouped.map(tt2 -> tt2._2())
												)
									)
									.reduce((acc1, acc2) -> acc1.union(acc2)).orElseGet(() -> jsc.emptyRDD())
									;
						
						if (!downstream_is_grouped) {
							mutable_rdds.put(control.name(), Either.left(
									rdd_inputs.mapPartitions(EnrichmentPipelineService.create(context, to_emit, t2._2()).javaInMapPartitions())));
						}
						else {
							mutable_rdds.put(control.name(), Either.right(
									rdd_inputs.mapPartitions(EnrichmentPipelineService.create(context, to_emit, t2._2()).javaInMapPartitionsPreGroup(new ArrayList<>(downstream_grouping)))
							))
							;																
						}
					}
				});
			
			return Validation.success(
					Tuples._2T(
					mutable_rdds,
					mutable_enricher_set.stream()
						.map(e_name -> Tuples._2T(e_name, mutable_rdds.get(e_name)))
						.filter(name_rdd -> null != name_rdd._2())
						.<Tuple2<String, JavaRDD<Tuple2<Long, IBatchRecord>>>>map(name__rdd_choice -> 
							Tuples._2T(name__rdd_choice._1(), name__rdd_choice._2().either(ungrouped -> ungrouped, grouped -> grouped.map(t2 -> t2._2))))
						.collect(Collectors
								.<Tuple2<String, JavaRDD<Tuple2<Long, IBatchRecord>>>, String, JavaRDD<Tuple2<Long, IBatchRecord>>>
								toMap(t2 -> t2._1(), t2 -> t2._2()))));
		});
	}
}
