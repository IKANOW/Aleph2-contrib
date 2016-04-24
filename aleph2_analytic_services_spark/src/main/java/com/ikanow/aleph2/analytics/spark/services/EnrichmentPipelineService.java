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
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;




import java.util.Map;
import java.util.Optional;

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
public class EnrichmentPipelineService implements Serializable {
	private static final long serialVersionUID = 5696574452500014975L;
	protected static final int _DEFAULT_BATCH_SIZE = 100;
	
	final protected List<EnrichmentControlMetadataBean> _pipeline_elements;
	final protected IAnalyticsContext _analytics_context;
		
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//
	// C'TOR	
	
	/** Creates an enrichment/transform pipeline service with the specified elements
	 * @param aleph2_context
	 * @param pipeline_elements
	 * @return
	 */
	public EnrichmentPipelineService create(final IAnalyticsContext aleph2_context, final List<EnrichmentControlMetadataBean> pipeline_elements) {
		return new EnrichmentPipelineService(aleph2_context, pipeline_elements);
	}
	/** Selects a set of enrichment/transform pipeline elements from the current job, and creates a service out of them
	 * @param aleph2_context
	 * @param names
	 * @return
	 */
	public EnrichmentPipelineService select(final IAnalyticsContext aleph2_context, String... names) {
		return new EnrichmentPipelineService(aleph2_context, names);
	}
	/** Selects a set of enrichment/transform pipeline elements from the current job, and creates a service out of them
	 * @param aleph2_context
	 * @param names
	 * @return
	 */
	public EnrichmentPipelineService select(final IAnalyticsContext aleph2_context, final Collection<String> names) {
		return new EnrichmentPipelineService(aleph2_context, names.stream().toArray(String[]::new));
	}

	//////////////////////////////////////////////////////////////////
	
	/** Creates a wrapper for a single enrichment job
	 * @param aleph2_context - the Aleph2 analytic context
	 * @param name - the name of the enrichment pipeline from the job
	 * @param dummy - just to differentatiate
	 */
	@SuppressWarnings("unchecked")
	protected EnrichmentPipelineService(final IAnalyticsContext aleph2_context, final String... names) {
		final Map<String, EnrichmentControlMetadataBean> pipeline_elements_map =		
			aleph2_context.getJob()
				.map(j -> j.config())
				.map(cfg -> cfg.get(EnrichmentControlMetadataBean.ENRICHMENT_PIPELINE))
				.<List<Object>>map(pipe -> Patterns.match().<List<Object>>andReturn()
									.when(List.class, l -> (List<Object>)l)
									.otherwise(() -> (List<Object>)null)
					)
				.<Map<String, EnrichmentControlMetadataBean>>map(l -> l.stream()
						.<EnrichmentControlMetadataBean>map(o -> BeanTemplateUtils.from((Map<String, Object>)o, EnrichmentControlMetadataBean.class).get())
						.collect(Collectors.toMap((EnrichmentControlMetadataBean cfg) -> cfg.name(), cfg -> cfg))
						)
				.orElse(Collections.emptyMap())
				;

		final List<EnrichmentControlMetadataBean> pipeline_elements = Arrays.stream(names).map(n -> pipeline_elements_map.get(n)).filter(cfg -> null != cfg).collect(Collectors.toList());
		
		_analytics_context = aleph2_context;
		_pipeline_elements = pipeline_elements;
		
		if (_pipeline_elements.isEmpty()) {
			throw new RuntimeException(ErrorUtils.get(SparkErrorUtils.NO_PIPELINE_ELEMENTS_SPECIFIED, 
							aleph2_context.getBucket().map(b -> b.full_name()).orElse("(unknown bucket)"),
							aleph2_context.getJob().map(j -> j.name()).orElse("(unknown job)")));
							
		}
		
	}
	
	/** Creates a wrapper for a pipeline of enrichment control (no grouping/fan in/fan out allowed)
	 * @param aleph2_context - the Aleph2 analytic context
	 * @param name - the name of the enrichment pipeline from the job
	 */
	protected EnrichmentPipelineService(final IAnalyticsContext aleph2_context, final List<EnrichmentControlMetadataBean> pipeline_elements) {		
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
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// SCALA VERSIONS
	
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
	
	//TODO: other scala versions
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// JAVA VERSIONS
	
	//TODO: other scala versions
	
	/** A transform/enrichment function that can be used in mapPartitions (java version)
	 * @return
	 */
	public FlatMapFunction<Iterator<Tuple2<Long, IBatchRecord>>, Tuple2<Long, IBatchRecord>> javaInMapPartitions() {
		return it -> {			
			_chain_start = new Wrapper(Streamable.of(_pipeline_elements), _DEFAULT_BATCH_SIZE, 
									Tuples._2T(ProcessingStage.unknown, ProcessingStage.batch), Optional.empty()); //(unknown: could be input or previous stage in chain)
			
			return StreamUtils.unfold(it, itit -> {
				return itit.hasNext() ? Optional.of(itit) : Optional.empty();
			})
			.map(itit -> Tuples._2T(itit.next(), itit.hasNext()))
			.<Tuple2<Long, IBatchRecord>>flatMap(id_record__islast -> {
				
				return _chain_start.process(addIncomingRecord(id_record__islast._1()), id_record__islast._2(), Optional.empty()).map(t2 -> t2._1());
			})
			::iterator
			;
		};	
	}

	/** A transform/enrichment function that can be used in mapPartitions (java version) immediately preceding a grouping operation
	 * @param names - an ordered list of field names (dot notation), is used to create a grouping key (["?"] means the enrichment engine can pick whatever it wants)
	 * @return
	 */
	public FlatMapFunction<Iterator<Tuple2<Long, IBatchRecord>>, Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> javaInMapPartitionsPreGroup(final String... names) {
		return javaInMapPartitionsPreGroup(Arrays.stream(names).collect(Collectors.toList()));
	}
	
	/** A transform/enrichment function that can be used in mapPartitions (java version) immediately preceding a grouping operation
	 * @param names - an ordered list of field names (dot notation), is used to create a grouping key (["?"] means the enrichment engine can pick whatever it wants)
	 * @return
	 */
	public FlatMapFunction<Iterator<Tuple2<Long, IBatchRecord>>, Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> javaInMapPartitionsPreGroup(final List<String> grouping_fields) {
		return it -> {			
			_chain_start = new Wrapper(Streamable.of(_pipeline_elements), _DEFAULT_BATCH_SIZE, 
									Tuples._2T(ProcessingStage.unknown, ProcessingStage.batch), Optional.of(grouping_fields)); //(unknown: could be input or previous stage in chain)
			
			return StreamUtils.unfold(it, itit -> {
				return itit.hasNext() ? Optional.of(itit) : Optional.empty();
			})
			.map(itit -> Tuples._2T(itit.next(), itit.hasNext()))
			.<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>>flatMap(id_record__islast -> {
				
				return _chain_start.process(addIncomingRecord(id_record__islast._1()), id_record__islast._2(), Optional.empty());
			})
			::iterator
			;
		};	
	}


	/** A transform/enrichment function that can be used in mapPartitions (java version) immediately following a grouping key operation
	 * @return
	 */
	public FlatMapFunction<Iterator<Tuple2<JsonNode, Iterable<Tuple2<Long, IBatchRecord>>>>, Tuple2<Long, IBatchRecord>> inMapPartitionsPostGroup() {
		return it -> {			
			final Tuple2<JsonNode, Iterable<Tuple2<Long, IBatchRecord>>> key_objects = it.next();
			if (null == _chain_start) {
				_chain_start = new Wrapper(Streamable.of(_pipeline_elements), _DEFAULT_BATCH_SIZE, 
						Tuples._2T(ProcessingStage.unknown, ProcessingStage.batch), Optional.empty()); //(unknown: could be input or previous stage in chain)
			}
			final Wrapper chain_start = _chain_start.clone();
			
			final Iterable<Tuple2<Long, IBatchRecord>> ret_val = StreamUtils.unfold(key_objects._2().iterator(), itit -> {
				return itit.hasNext() ? Optional.of(itit) : Optional.empty();
			})
			.map(itit -> Tuples._2T(itit.next(), itit.hasNext()))
			.<Tuple2<Long, IBatchRecord>>flatMap(id_record__islast -> {

				return chain_start.process(addIncomingRecord(id_record__islast._1()), id_record__islast._2(), Optional.of(key_objects._1())).map(t2 -> t2._1());				
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
	
	//TODO: validation in calling stack
	
	/** Utility class to handle a chained list of pipeline elements
	 * @author Alex
	 *
	 */
	protected class Wrapper {
		final protected Wrapper _next;
		final protected int _batch_size;
		final protected List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> mutable_list = new LinkedList<>();		
		final protected IEnrichmentBatchModule _batch_module;
		final IEnrichmentModuleContext _enrichment_context;		
		final protected Optional<IBucketLogger> _logger;
		protected Wrapper _variable_clone_cache = null;
		
		/** User c'tor
		 * @param - list of enrichment objects
		 */
		protected Wrapper(final Streamable<EnrichmentControlMetadataBean> remaining_elements, final int default_batch_size, 
							final Tuple2<ProcessingStage, ProcessingStage> previous_next, final Optional<List<String>> next_grouping_fields)
		{
			final EnrichmentControlMetadataBean control = remaining_elements.stream().findFirst().get();			
			_batch_size = Optional.ofNullable(control.technology_override().get(EnrichmentControlMetadataBean.BATCH_SIZE_OVERRIDE))
					.map(o -> Patterns.match(o).<Integer>andReturn()
								.when(Integer.class, __->__)
								.when(Long.class, l->l.intValue())
								.when(String.class, s -> Integer.parseInt(s))
								.otherwise(() -> null)
						)
					.orElse(default_batch_size)
					;

			_enrichment_context = _analytics_context.getUnderlyingPlatformDriver(IEnrichmentModuleContext.class, Optional.of(Integer.toString(_batch_size))).get();
			_logger = Optional.ofNullable(_analytics_context.getLogger(Optional.empty()));
			
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
											next_grouping_fields.filter(__ -> is_final_stage)
											);
			
			if (!is_final_stage) {
				final Tuple2<ProcessingStage, ProcessingStage> next__previous_next = 
						Tuples._2T(previous_next._2(), is_final_stage ? ProcessingStage.unknown : previous_next._2());
						
				_next = new Wrapper(remaining_elements.skip(1), _batch_size, next__previous_next, next_grouping_fields);
			}
			else _next = null;
		}
		
		//TODO: clone function + state (then cache)
		public Wrapper clone() {
			if (null != _variable_clone_cache) return _variable_clone_cache;
			else return this;
		}
		
		/** Processes the next stage of a chain of pipelines
		 * @param list
		 * @param last - note only called immediately before the cleanup call
		 * @return
		 */
		public Stream<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> process(final List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> list, boolean last, Optional<JsonNode> grouping_key) {
			mutable_list.addAll(list);
			if (last || (mutable_list.size() > _batch_size)) {
				return Optionals.streamOf(Iterators.partition(mutable_list.iterator(), _batch_size), false).<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>>flatMap(l -> {
					
					@SuppressWarnings("unchecked")
					final List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> stage_output = (List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>>)
							_enrichment_context.getUnderlyingPlatformDriver(List.class, Optional.empty()).get();
					
					stage_output.clear();
					_batch_module.onObjectBatch(l.stream().map(t2 -> t2._1()), Optional.of(l.size()), grouping_key);
					
					_batch_module.onStageComplete(true); //TODO (ALEPH-62): (also need to add support for this in batch enrichment, though it's a fair bit of work)
										
					final Stream<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> ret_vals_int =  (null != _next)
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
	
	/** Adds the incoming element from the previous stage to the batch
	 * @param el
	 * @return
	 */
	protected static List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> addIncomingRecord(final Tuple2<Long, IBatchRecord> el) {
		return Arrays.asList(Tuples._2T(el, Optional.empty()));
	}
	
}
