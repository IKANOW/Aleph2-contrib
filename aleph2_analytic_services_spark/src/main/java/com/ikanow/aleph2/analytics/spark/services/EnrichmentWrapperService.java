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

import scala.compat.java8.JFunction;
import scala.compat.java8.JFunction1;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;




import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.spark.api.java.JavaRDD;














import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;


































import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.codepoetics.protonpack.StreamUtils;
import com.codepoetics.protonpack.Streamable;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.ikanow.aleph2.analytics.spark.utils.SparkErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule.ProcessingStage;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;

/** A utility service that runs an Aleph2 enrichment service inside Spark
 * @author Alex
 *
 */
public class EnrichmentWrapperService implements Serializable {
	private static final long serialVersionUID = 5696574452500014975L;
	protected static final int _DEFAULT_BATCH_SIZE = 100;
	
	final protected List<EnrichmentControlMetadataBean> _pipeline_elements;
	final protected IAnalyticsContext _analytics_context;
		
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//
	// C'TOR	
	
	/** Creates a wrapper for a single enrichment job
	 * @param aleph2_context - the Aleph2 analytic context
	 * @param name - the name of the enrichment pipeline from the job
	 */
	@SuppressWarnings("unchecked")
	public EnrichmentWrapperService(final IAnalyticsContext aleph2_context, final Set<String> names) {
		this(aleph2_context
				,
				aleph2_context.getJob()
					.map(j -> j.config())
					.map(cfg -> cfg.get(EnrichmentControlMetadataBean.ENRICHMENT_PIPELINE))
					.<List<Object>>map(pipe -> Patterns.match().<List<Object>>andReturn()
										.when(List.class, l -> l)
										.otherwise(() -> null)
						)
					.<List<EnrichmentControlMetadataBean>>map(l -> l.stream()
							.<EnrichmentControlMetadataBean>map(o -> BeanTemplateUtils.from((Map<String, Object>)o, EnrichmentControlMetadataBean.class).get())
							.filter(cfg -> names.contains(cfg.name()))
							.collect(Collectors.toList())
							)
					.orElse(Collections.emptyList())
			);
	}
	
	/** Creates a wrapper for a pipeline of enrichment control (no grouping/fan in/fan out allowed)
	 * @param aleph2_context - the Aleph2 analytic context
	 * @param name - the name of the enrichment pipeline from the job
	 */
	public EnrichmentWrapperService(final IAnalyticsContext aleph2_context, final List<EnrichmentControlMetadataBean> pipeline_elements) {		
		_analytics_context = aleph2_context;
		_pipeline_elements = pipeline_elements;
		
		if (_pipeline_elements.isEmpty()) {
			throw new RuntimeException(ErrorUtils.get(SparkErrorUtils.NO_PIPELINE_ELEMENTS_SPECIFIED, 
							aleph2_context.getBucket().map(b -> b.full_name()).orElse("(unknown bucket)"),
							aleph2_context.getJob().map(j -> j.name()).orElse("(unknown job)")));
							
		}
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//
	// FUNCTIONAL API
	
	protected Wrapper _chain_start = null;
	
	//TODO: handle streaming case (includes map), different input to pipeline etc

	//TODO: scala version of map/groupBy
	
	/** A transform/enrichment function that can be used in mapPartitions (scala version)
	 * @return
	 */
	public scala.Function1<scala.collection.Iterator<Tuple2<Long, IBatchRecord>>, scala.collection.Iterator<Tuple2<Long, IBatchRecord>>> inMapPartitions() {
		return JFunction.func(
				it -> 
				// (Alternative to above double function, might be faster, not sure if the cast will work)
				//(JFunction1<scala.collection.Iterator<Tuple2<Long, IBatchRecord>>, scala.collection.Iterator<Tuple2<Long, IBatchRecord>>>)
					Lambdas.<scala.collection.Iterator<Tuple2<Long, IBatchRecord>>, scala.collection.Iterator<Tuple2<Long, IBatchRecord>>>wrap_u(
							it1 -> JavaConverters.asScalaIteratorConverter(this.javaInMapPartitions().call(JavaConverters.asJavaIteratorConverter(it1).asJava()).iterator()).asScala()
					).apply(it));
	}	
	
//	private Function<scala.collection.Iterator<Tuple2<Long, IBatchRecord>>, scala.collection.Iterator<Tuple2<Long, IBatchRecord>>> semiScalaInMapPartitions() {
//		return ;
//	}
	
	/** A transform/enrichment function that can be used in mapPartitions (java version)
	 * @return
	 */
	public FlatMapFunction<Iterator<Tuple2<Long, IBatchRecord>>, Tuple2<Long, IBatchRecord>> javaInMapPartitions() {
		return it -> {			
			_chain_start = new Wrapper(Streamable.of(_pipeline_elements), _DEFAULT_BATCH_SIZE, 
									Tuples._2T(ProcessingStage.unknown, ProcessingStage.batch)); //(unknown: could be input or previous stage in chain)
			
			return StreamUtils.unfold(it, itit -> {
				return itit.hasNext() ? Optional.of(itit) : Optional.empty();
			})
			.map(itit -> Tuples._2T(itit.next(), itit.hasNext()))
			.<Tuple2<Long, IBatchRecord>>flatMap(id_record__islast -> {
				
				return _chain_start.process(Arrays.asList(id_record__islast._1()), id_record__islast._2(), Optional.empty());
			})
			::iterator
			;
		};	
	}

	/** A transform/enrichment function that can be used in mapPartitions immediately following a grouping key
	 * @return
	 */
	public FlatMapFunction<Iterator<Tuple2<JsonNode, Iterable<Tuple2<Long, IBatchRecord>>>>, Tuple2<Long, IBatchRecord>> inMapPartitionsAfterGroup(final JsonNode groupingKey) {
		return it -> {			
			final Tuple2<JsonNode, Iterable<Tuple2<Long, IBatchRecord>>> key_objects = it.next();
			if (null == _chain_start) {
				_chain_start = new Wrapper(Streamable.of(_pipeline_elements), _DEFAULT_BATCH_SIZE, 
						Tuples._2T(ProcessingStage.unknown, ProcessingStage.batch)); //(unknown: could be input or previous stage in chain)
			}
			final Wrapper chain_start = _chain_start.clone();
			
			final Iterable<Tuple2<Long, IBatchRecord>> ret_val = StreamUtils.unfold(key_objects._2().iterator(), itit -> {
				return itit.hasNext() ? Optional.of(itit) : Optional.empty();
			})
			.map(itit -> Tuples._2T(itit.next(), itit.hasNext()))
			.<Tuple2<Long, IBatchRecord>>flatMap(id_record__islast -> {

				return chain_start.process(Arrays.asList(id_record__islast._1()), id_record__islast._2(), Optional.of(key_objects._1()));				
			})
			::iterator
			;
			return ret_val;
		};	
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//
	// PROCEDURAL API
	
	/** Maps from one JavaRDD to another via the pipeline of enrichment elements
	 * @param rdd
	 * @return
	 */
	public JavaRDD<Tuple2<Long, IBatchRecord>> map(JavaRDD<Tuple2<Long, IBatchRecord>> rdd) {
		return rdd.mapPartitions(this.javaInMapPartitions());
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//
	// UTILS
	
	//TODO: log stats and record when batch completes 
	
	/** Utility class to handle a chained list of pipeline elements
	 * @author Alex
	 *
	 */
	protected class Wrapper {
		final protected Wrapper _next;
		final protected int _batch_size;
		final protected List<Tuple2<Long, IBatchRecord>> mutable_list = new LinkedList<>();		
		final protected IEnrichmentBatchModule _batch_module;
		final IEnrichmentModuleContext _enrichment_context;		
		final protected Optional<IBucketLogger> _logger;
		
		/** User c'tor
		 * @param - list of enrichment objects
		 */
		protected Wrapper(final Streamable<EnrichmentControlMetadataBean> remaining_elements, final int default_batch_size, 
							final Tuple2<ProcessingStage, ProcessingStage> previous_next)
		{
			final EnrichmentControlMetadataBean control = remaining_elements.stream().findFirst().get();			
			_enrichment_context = _analytics_context.getUnderlyingPlatformDriver(IEnrichmentModuleContext.class, Optional.empty()).get();
			_logger = Optional.ofNullable(_analytics_context.getLogger(Optional.empty()));
			
			_batch_size = Optional.ofNullable(control.technology_override().get(EnrichmentControlMetadataBean.BATCH_SIZE_OVERRIDE))
									.map(o -> Patterns.match(o).<Integer>andReturn()
												.when(Integer.class, __->__)
												.when(Long.class, l->l.intValue())
												.when(String.class, s -> Integer.parseInt(s))
												.otherwise(() -> null)
										)
									.orElse(default_batch_size)
									;
			
			final Map<String, SharedLibraryBean> library_beans = _analytics_context.getLibraryConfigs();
			final Optional<String> entryPoint = BucketUtils.getBatchEntryPoint(library_beans, control);
			_batch_module =  entryPoint.map(Optional::of).orElseGet(() -> Optional.of("com.ikanow.aleph2.analytics.services.PassthroughService")).map(Lambdas.wrap_u(ep -> {
				try {
					// (need the context classloader bit in order to the JCL classpath with the user JARs when called as part of validation from within the service)
					return (IEnrichmentBatchModule)Class.forName(ep, true, Thread.currentThread().getContextClassLoader()).newInstance();
				}
				catch (Throwable t) {
					
					_logger.ifPresent(l -> l.log(Level.ERROR, 						
							ErrorUtils.lazyBuildMessage(false, () -> "EnrichmentWrapperService", 
									() -> Optional.ofNullable(control.name()).orElse("no_name") + ".onStageInitialize", 
									() -> null, 
									() -> ErrorUtils.get("Error initializing {1}:{2}: {0}", Optional.ofNullable(control.name()).orElse("(no name)"), entryPoint.orElse("(unknown entry"), t.getMessage()), 
									() -> ImmutableMap.<String, Object>of("full_error", ErrorUtils.getLongForm("{0}", t)))
							));
					
					throw t; // (will trigger a mass failure of the job, which is I think what we want...)
				}
			})).get()
			;

			final boolean is_final_stage = !remaining_elements.skip(1).stream().findFirst().isPresent();
			
			_batch_module.onStageInitialize(_enrichment_context, _analytics_context.getBucket().get(), control,  
											Tuples._2T(previous_next._1(), is_final_stage ? ProcessingStage.unknown : previous_next._2()),
											Optional.empty() //(not sure what to do about next_grouping_fields?)
											);
			
			if (!is_final_stage) {
				final Tuple2<ProcessingStage, ProcessingStage> next__previous_next = 
						Tuples._2T(previous_next._2(), is_final_stage ? ProcessingStage.unknown : previous_next._2());
						
				_next = new Wrapper(remaining_elements.skip(1), _batch_size, next__previous_next);
			}
			else _next = null;
		}
		
		//TODO: clone function + state
		public Wrapper clone() {
			return this;
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
					
					_batch_module.onStageComplete(true); //(TODOD also need to add support for this in batch enrichment)
					@SuppressWarnings("unchecked")
					final List<Tuple2<Long, IBatchRecord>> stage_output = (List<Tuple2<Long, IBatchRecord>>)
							_enrichment_context.getUnderlyingPlatformDriver(List.class, Optional.empty()).get();
										
					final Stream<Tuple2<Long, IBatchRecord>> ret_vals_int =  (null != _next)
							? _next.process(stage_output, true, Optional.empty())
							: stage_output.stream()
							;
							
					return ret_vals_int;
				})
				;
			}
			else return Stream.empty();
		}
	}
	
}
