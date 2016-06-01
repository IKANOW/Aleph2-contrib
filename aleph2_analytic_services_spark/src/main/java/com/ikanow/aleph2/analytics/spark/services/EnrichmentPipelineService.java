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

import scala.collection.JavaConverters;

import java.util.ArrayList;
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
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

































































import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.codepoetics.protonpack.StreamUtils;
import com.codepoetics.protonpack.Streamable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.ikanow.aleph2.analytics.spark.utils.SparkErrorUtils;
import com.ikanow.aleph2.core.shared.utils.BatchRecordUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule.ProcessingStage;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.logging.utils.LoggingMergeFunctions;
import com.ikanow.aleph2.logging.utils.LoggingRules;

import fj.Unit;
import fj.data.Either;
import fj.data.Validation;

/** A utility service that runs an Aleph2 enrichment service inside Spark
 * @author Alex
 *
 */
public class EnrichmentPipelineService implements Serializable {
	public final static String UUID = UuidUtils.get().getRandomUuid().split("-")[4];
	private static final long serialVersionUID = 5696574452500014975L;
	protected static final int _DEFAULT_BATCH_SIZE = 100;
	private static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	final protected List<EnrichmentControlMetadataBean> _pipeline_elements;
	final protected IAnalyticsContext _analytics_context;
	final protected boolean _emit_when_done;
	protected transient Wrapper _chain_start = null;
			
	final protected boolean _is_streaming;
	protected final static Cache<EnrichmentPipelineService, Wrapper> _instance_pool = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).build();
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//
	// INITIALIZATION API
	
	/** Creates an enrichment/transform pipeline service with the specified elements
	 * @param aleph2_context
	 * @param pipeline_elements
	 * @return
	 */
	public static EnrichmentPipelineService create(final IAnalyticsContext aleph2_context, boolean emit_when_done, final List<EnrichmentControlMetadataBean> pipeline_elements) {
		return new EnrichmentPipelineService(aleph2_context, emit_when_done, pipeline_elements);
	}
	/** Selects a set of enrichment/transform pipeline elements from the current job, and creates a service out of them
	 * @param aleph2_context
	 * @param names
	 * @return
	 */
	public static EnrichmentPipelineService select(final IAnalyticsContext aleph2_context, boolean emit_when_done, String... names) {
		return new EnrichmentPipelineService(aleph2_context, emit_when_done, names);
	}
	/** Selects a set of enrichment/transform pipeline elements from the current job, and creates a service out of them
	 * @param aleph2_context
	 * @param names
	 * @return
	 */
	public static EnrichmentPipelineService select(final IAnalyticsContext aleph2_context, boolean emit_when_done, final Collection<String> names) {
		return new EnrichmentPipelineService(aleph2_context, emit_when_done, names.stream().toArray(String[]::new));
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//
	// FUNCTIONAL API
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// SCALA VERSIONS
	
	public abstract class SerializableAbstractFunction1<A, B> extends scala.runtime.AbstractFunction1<A, B> implements Serializable {
		private static final long serialVersionUID = 421692222804663556L;		
	}
	
	/** A transform/enrichment function that can be used in mapPartitions (scala version)
	 * @return
	 */
	public scala.Function1<scala.collection.Iterator<Tuple2<Long, IBatchRecord>>, scala.collection.Iterator<Tuple2<Long, IBatchRecord>>> inMapPartitions() {
		return new SerializableAbstractFunction1<scala.collection.Iterator<Tuple2<Long, IBatchRecord>>, scala.collection.Iterator<Tuple2<Long, IBatchRecord>>>() {
			private static final long serialVersionUID = 6619928568102664447L;

			@Override
			public scala.collection.Iterator<Tuple2<Long, IBatchRecord>> apply(scala.collection.Iterator<Tuple2<Long, IBatchRecord>> it1) {
				try {
					return JavaConverters.asScalaIteratorConverter(javaInMapPartitions().call(JavaConverters.asJavaIteratorConverter(it1).asJava()).iterator()).asScala();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		};
	}
	
	/** A transform/enrichment function that can be used in mapPartitions (scala version) immediately preceding a grouping operation
	 * @param names - an ordered list of field names (dot notation), is used to create a grouping key (["?"] means the enrichment engine can pick whatever it wants)
	 * @return
	 */
	public scala.Function1<scala.collection.Iterator<Tuple2<Long, IBatchRecord>>, scala.collection.Iterator<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>>> inMapPartitionsPreGroup(final String... names) {
		return inMapPartitionsPreGroup(Arrays.stream(names).collect(Collectors.toList()));
	}	
	
	/** A transform/enrichment function that can be used in mapPartitions (scala version) immediately preceding a grouping operation
	 * @param grouping_fields - an ordered list of field names (dot notation), is used to create a grouping key (["?"] means the enrichment engine can pick whatever it wants)
	 * @return
	 */
	public scala.Function1<scala.collection.Iterator<Tuple2<Long, IBatchRecord>>, scala.collection.Iterator<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>>> inMapPartitionsPreGroup(final List<String> grouping_fields) {
		return new SerializableAbstractFunction1<scala.collection.Iterator<Tuple2<Long, IBatchRecord>>, scala.collection.Iterator<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>>>() {
			private static final long serialVersionUID = 2563609106687060337L;

			@Override
			public scala.collection.Iterator<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>> apply(scala.collection.Iterator<Tuple2<Long, IBatchRecord>> it1) {
				try {
					return JavaConverters.asScalaIteratorConverter(javaInMapPartitionsPreGroup(grouping_fields).call(JavaConverters.asJavaIteratorConverter(it1).asJava()).iterator()).asScala();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}			
		};		
	}	
	
	/** A transform/enrichment function that can be used in mapPartitions (java version) immediately following a grouping key operation and that itself groups
	 *  (NOTE operates on *java* iterables - you will need to convert in order to operate on your own groupby calls)
	 * @param names - an ordered list of field names (dot notation), is used to create a grouping key (["?"] means the enrichment engine can pick whatever it wants)
	 * @return
	 */
	public <T> scala.Function1<scala.collection.Iterator<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>>, scala.collection.Iterator<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>>> inMapPartitionsPrePostGroup(final String... names) {
		return inMapPartitionsPrePostGroup(Arrays.stream(names).collect(Collectors.toList()));
	}	
	
	/** A transform/enrichment function that can be used in mapPartitions (java version) immediately following a grouping key operation and that itself groups
	 *  (NOTE operates on *java* iterables - you will need to convert in order to operate on your own groupby calls)
	 * @param grouping_fields - an ordered list of field names (dot notation), is used to create a grouping key (["?"] means the enrichment engine can pick whatever it wants)
	 * @return
	 */
	public <T> scala.Function1<scala.collection.Iterator<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>>, scala.collection.Iterator<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>>> inMapPartitionsPrePostGroup(final List<String> grouping_fields) {
		return new SerializableAbstractFunction1<scala.collection.Iterator<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>>, scala.collection.Iterator<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>>>() {
			private static final long serialVersionUID = 5772470054865757342L;

			@Override
			public scala.collection.Iterator<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>> apply(scala.collection.Iterator<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>> it1) {
				try {
					return JavaConverters.asScalaIteratorConverter(javaInMapPartitionsPrePostGroup(grouping_fields).call(JavaConverters.asJavaIteratorConverter(it1).asJava()).iterator()).asScala();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		};		
	}	
	
	/** A transform/enrichment function that can be used in mapPartitions (java version) immediately following a grouping key operation
	 *  (NOTE operates on *java* iterables - you will need to convert in order to operate on your own groupby calls)
	 * @return
	 */
	public <T> scala.Function1<scala.collection.Iterator<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>>, scala.collection.Iterator<Tuple2<Long, IBatchRecord>>> inMapPartitionsPostGroup() {
		return new SerializableAbstractFunction1<scala.collection.Iterator<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>>, scala.collection.Iterator<Tuple2<Long, IBatchRecord>>>() {
			private static final long serialVersionUID = -810181378636397195L;

			@Override
			public scala.collection.Iterator<Tuple2<Long, IBatchRecord>> apply(scala.collection.Iterator<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>> it1) {
				try {
					return JavaConverters.asScalaIteratorConverter(javaInMapPartitionsPostGroup().call(JavaConverters.asJavaIteratorConverter(it1).asJava()).iterator()).asScala();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}			
		};
	}	
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// JAVA VERSIONS
	
	/** A transform/enrichment function that can be used in mapPartitions (java version)
	 * @return
	 */
	public FlatMapFunction<Iterator<Tuple2<Long, IBatchRecord>>, Tuple2<Long, IBatchRecord>> javaInMapPartitions() {		
		return (FlatMapFunction<Iterator<Tuple2<Long, IBatchRecord>>, Tuple2<Long, IBatchRecord>> & Serializable)it -> {
			if ((null == _chain_start) && _is_streaming) {
				_chain_start = _instance_pool.getIfPresent(this);
			}
			if (null == _chain_start) {
				_chain_start = new Wrapper(Streamable.of(_pipeline_elements), _DEFAULT_BATCH_SIZE, 
										Tuples._2T(ProcessingStage.unknown, ProcessingStage.batch), Optional.empty()); //(unknown: could be input or previous stage in chain)
			}
			
			return createStream(it).<Tuple2<Long, IBatchRecord>>flatMap(id_record__islast -> {
				
				//TRACE						
				//System.out.println("javaInMapPartitions: " + id_record__islast + " @ " + this);
				
				return _chain_start.process(addIncomingRecord(id_record__islast._1()), id_record__islast._2(), true, Optional.empty()).map(t2 -> t2._1());
			})
			::iterator
			;
		};	
	}

	/** A transform/enrichment function that can be used in mapPartitions (java version) immediately preceding a grouping operation
	 * @param names - an ordered list of field names (dot notation), is used to create a grouping key (["?"] means the enrichment engine can pick whatever it wants)
	 * @return
	 */
	public FlatMapFunction<Iterator<Tuple2<Long, IBatchRecord>>, Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>> javaInMapPartitionsPreGroup(final String... names) {
		return javaInMapPartitionsPreGroup(Arrays.stream(names).collect(Collectors.toList()));
	}
	
	/** A transform/enrichment function that can be used in mapPartitions (java version) immediately preceding a grouping operation
	 * @param grouping_fields - an ordered list of field names (dot notation), is used to create a grouping key (["?"] means the enrichment engine can pick whatever it wants)
	 * @return
	 */
	public FlatMapFunction<Iterator<Tuple2<Long, IBatchRecord>>, Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>> javaInMapPartitionsPreGroup(final List<String> grouping_fields) {
		return (FlatMapFunction<Iterator<Tuple2<Long, IBatchRecord>>, Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>> & Serializable)it -> {			
			if ((null == _chain_start) && _is_streaming) {
				_chain_start = _instance_pool.getIfPresent(this);
			}
			if (null == _chain_start) {
				_chain_start = new Wrapper(Streamable.of(_pipeline_elements), _DEFAULT_BATCH_SIZE, 
										Tuples._2T(ProcessingStage.unknown, ProcessingStage.batch), Optional.of(grouping_fields)); //(unknown: could be input or previous stage in chain)
			}
			
			return createStream(it)
					.<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>>flatMap(id_record__islast -> {
				
				return _chain_start.process(addIncomingRecord(id_record__islast._1()), id_record__islast._2(), true, Optional.empty())
						.map(t2 -> Tuples._2T((IBatchRecord) new BatchRecordUtils.JsonBatchRecord(t2._2().orElse(_mapper.createObjectNode())), t2._1()));
			})
			::iterator
			;
		};	
	}

	/** A transform/enrichment function that can be used in mapPartitions (java version) immediately preceding a grouping operation and where the data is being grouped
	 * @param names - an ordered list of field names (dot notation), is used to create a grouping key (["?"] means the enrichment engine can pick whatever it wants)
	 * @return
	 */
	public FlatMapFunction<Iterator<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>>, Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>> javaInMapPartitionsPrePostGroup(final String... names) {
		return javaInMapPartitionsPrePostGroup(Arrays.stream(names).collect(Collectors.toList()));
	}
	
	/** A transform/enrichment function that can be used in mapPartitions (java version) immediately preceding a grouping operation and where the data is being grouped
	 * @param grouping_fields - an ordered list of field names (dot notation), is used to create a grouping key (["?"] means the enrichment engine can pick whatever it wants)
	 * @return
	 */
	public FlatMapFunction<Iterator<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>>, Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>> javaInMapPartitionsPrePostGroup(final List<String> grouping_fields) {
		return (FlatMapFunction<Iterator<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>>, Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>> & Serializable)it -> {			
			
			// OK so we're inside a map partition so we might well have a number of keys handled inside this one method
			// Note all objects are shared across all the "subsequent" enrichers

			return applyPostgroup(Optional.of(grouping_fields), it)
					.map(t2 -> Tuples._2T((IBatchRecord) new BatchRecordUtils.JsonBatchRecord(t2._2().orElse(_mapper.createObjectNode())), t2._1()))
					::iterator
					;			
		};	
	}

	/** A transform/enrichment function that can be used in mapPartitions (java version) immediately following a grouping key operation
	 * @return
	 */
	public FlatMapFunction<Iterator<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>>, Tuple2<Long, IBatchRecord>> javaInMapPartitionsPostGroup() {
		return (FlatMapFunction<Iterator<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>>, Tuple2<Long, IBatchRecord>> & Serializable) it -> {			
			
			// OK so we're inside a map partition so we might well have a number of keys handled inside this one method
			// Note all objects are shared across all the "subsequent" enrichers

			return applyPostgroup(Optional.empty(), it).map(t2 -> t2._1())
					::iterator
					;			
		};	
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//
	// MISC API

	/** Performs the Spark grouping from the output of a pre-group operation (java version)
 	 * @param groupable
	 * @return
	 */
	public static JavaRDD<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>> javaGroupOf(final JavaRDD<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>> groupable) {
		return groupable.groupBy(t2 -> t2._1()).map(t2 -> Tuples._2T(t2._1(), (Iterable<Tuple2<Long, IBatchRecord>>)StreamUtils.stream(t2._2()).map(tt2 -> tt2._2())::iterator));				
	}
	
	/** Performs the Spark grouping from the output of a pre-group operation (scala version)
	 *  (NOTE: returns java iterables, ie designed for use with the inMapPartition* functions above)
 	 * @param groupable
	 * @return
	 */
	public static RDD<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>> groupOf(final RDD<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>> groupable) {
		return javaGroupOf(new JavaRDD<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>>(groupable, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class))).rdd();
	}
		
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// Shared functionality:
	
	protected Stream<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> applyPostgroup(final Optional<List<String>> maybe_grouping_fields, final Iterator<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>> it) {
		return createStream(it).flatMap(key_objects__last -> {
			
			final Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>> key_objects = key_objects__last._1();
			if ((null == _chain_start) && _is_streaming) {
				_chain_start = _instance_pool.getIfPresent(this);
			}
			if (null == _chain_start) {
				_chain_start = new Wrapper(Streamable.of(_pipeline_elements), _DEFAULT_BATCH_SIZE, 
						Tuples._2T(ProcessingStage.unknown, ProcessingStage.batch), maybe_grouping_fields); //(unknown: could be input or previous stage in chain)
			}
			final Wrapper chain_start = _chain_start.cloneWrapper();

			//TRACE
			//System.out.println("Grouping ... " + key_objects._1().getJson() + " .. " + StreamUtils.stream(key_objects._2()).count() + ".. " + key_objects__last._2());
			
			final Stream<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> ret_val = createStream(key_objects._2().iterator())
					.<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>>flatMap(id_record__islast -> {

				//TRACE						
				//System.out.println("javaInMapPartitionsPostGroup: " + id_record__islast + ".. grouped by " + key_objects._1().getJson());
						
				return chain_start.process(addIncomingRecord(id_record__islast._1()), id_record__islast._2(), key_objects__last._2(), Optional.of(key_objects._1().getJson()));
			});
			return ret_val;
		})
		;
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//
	// CTORS
	
	
	/** Creates a wrapper for a single enrichment job
	 * @param aleph2_context - the Aleph2 analytic context
	 * @param name - the name of the enrichment pipeline from the job
	 * @param dummy - just to differentatiate
	 */
	@SuppressWarnings("unchecked")
	protected EnrichmentPipelineService(final IAnalyticsContext aleph2_context, boolean emit_when_done, final String... names) {
		final Map<String, EnrichmentControlMetadataBean> pipeline_elements_map =		
			aleph2_context.getJob()
				.map(j -> j.config())
				.map(cfg -> cfg.get(EnrichmentControlMetadataBean.ENRICHMENT_PIPELINE))
				.<List<Object>>map(pipe -> Patterns.match(pipe).<List<Object>>andReturn()
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
		_emit_when_done = emit_when_done;
		_pipeline_elements = pipeline_elements;		
		_is_streaming = aleph2_context.getJob().map(j -> j.analytic_type()).map(t -> MasterEnrichmentType.streaming == t).orElse(false);
		
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
	protected EnrichmentPipelineService(final IAnalyticsContext aleph2_context, boolean emit_when_done, final List<EnrichmentControlMetadataBean> pipeline_elements) {		
		_analytics_context = aleph2_context;
		_emit_when_done = emit_when_done;
		_pipeline_elements = pipeline_elements;
		_is_streaming = aleph2_context.getJob().map(j -> j.analytic_type()).map(t -> MasterEnrichmentType.streaming == t).orElse(false);
		
		if (_pipeline_elements.isEmpty()) {
			throw new RuntimeException(ErrorUtils.get(SparkErrorUtils.NO_PIPELINE_ELEMENTS_SPECIFIED, 
							aleph2_context.getBucket().map(b -> b.full_name()).orElse("(unknown bucket)"),
							aleph2_context.getJob().map(j -> j.name()).orElse("(unknown job)")));
							
		}
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//
	// MAIN LOGIC
	
	/** Utility class to handle a chained list of pipeline elements
	 * @author Alex
	 *
	 */
	protected class Wrapper {
		final protected Wrapper _next;
		final protected int _batch_size;
		final protected IEnrichmentBatchModule _batch_module;
		final protected IEnrichmentModuleContext _enrichment_context;	
		final protected EnrichmentControlMetadataBean _control;
		final protected Optional<IBucketLogger> _logger;
		final protected IEnrichmentBatchModule _clone_of;
		
		protected List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> _variable_list = new LinkedList<>();		
		protected Wrapper _variable_clone_cache = null;
		
		protected class MutableStats {
			int in = 0;
			int out = 0;
		};		
		protected final MutableStats _stats = new MutableStats();
		
		////////////////////////////////////////////////////////////////
		
		// CTORS
		
		/** User c'tor
		 * @param - list of enrichment objects
		 */
		protected Wrapper(final Streamable<EnrichmentControlMetadataBean> remaining_elements, final int default_batch_size, 
							final Tuple2<ProcessingStage, ProcessingStage> previous_next, final Optional<List<String>> next_grouping_fields)
		{
			// Set up state
			
			_clone_of = null;
			_control = remaining_elements.stream().findFirst().get();			
			_batch_size = Optionals.of(() -> _control.technology_override().get(EnrichmentControlMetadataBean.BATCH_SIZE_OVERRIDE))
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
			final Optional<String> entryPoint = BucketUtils.getBatchEntryPoint(library_beans, _control);
			_batch_module =  entryPoint.map(Optional::of).orElseGet(() -> Optional.of("com.ikanow.aleph2.analytics.services.PassthroughService")).map(Lambdas.wrap_u(ep -> {
				try {
					// (need the context classloader bit in order to the JCL classpath with the user JARs when called as part of validation from within the service)
					return (IEnrichmentBatchModule)Class.forName(ep, true, Thread.currentThread().getContextClassLoader()).newInstance();
				}
				catch (Throwable t) {
					
					_logger.ifPresent(l -> l.log(Level.ERROR, 						
							ErrorUtils.lazyBuildMessage(false, () -> "EnrichmentPipelineService", 
									() -> Optional.ofNullable(_control.name()).orElse("no_name") + ".onStageInitialize", 
									() -> null, 
									() -> ErrorUtils.get("Error initializing {1}:{2}: {0}", Optional.ofNullable(_control.name()).orElse("(no name)"), entryPoint.orElse("(unknown entry"), t.getMessage()), 
									() -> ImmutableMap.<String, Object>of("full_error", ErrorUtils.getLongForm("{0}", t)))
							));
					
					throw t; // (will trigger a mass failure of the job, which is I think what we want...)
				}
			})).get()
			;

			final boolean is_final_stage = !remaining_elements.skip(1).stream().findFirst().isPresent();
			
			// Initalize stage:
			
			_batch_module.onStageInitialize(_enrichment_context, _analytics_context.getBucket().get(), _control,  
											Tuples._2T(previous_next._1(), is_final_stage ? ProcessingStage.unknown : previous_next._2()),
											next_grouping_fields.filter(__ -> is_final_stage)
											);
			
			if (!is_final_stage) {
				final Tuple2<ProcessingStage, ProcessingStage> next__previous_next = 
						Tuples._2T(previous_next._2(), is_final_stage ? ProcessingStage.unknown : previous_next._2());
						
				_next = new Wrapper(remaining_elements.skip(1), _batch_size, next__previous_next, next_grouping_fields);
			}
			else _next = null;
			
			if (_is_streaming) { // streaming mode: register self for the future
				_instance_pool.put(EnrichmentPipelineService.this, this);
			}
		}
		
		/** Copy c'tor
		 * @param to_clone
		 */
		protected Wrapper(final Wrapper to_clone) {
			_clone_of = to_clone._batch_module;
			_batch_size = to_clone._batch_size;
			_batch_module = to_clone._batch_module.cloneForNewGrouping();
			_enrichment_context = to_clone._enrichment_context;
			_logger = to_clone._logger;
			_control = to_clone._control;
			
			// DON'T DO THIS BECAUSE ALL ENRICHERS AFTER THE FIRST ONE ARE BACK IN BATCH MODE...
//			if (null != to_clone._next) {
//				_next = new Wrapper(to_clone._next);
//			}
//			else _next = null;
			// DO THIS INSTEAD:
			_next = to_clone._next;
		}
		
		/** Clones a wrapper - if it turns out the clone is identical to the original then saves that off
		 * @return
		 */
		public Wrapper cloneWrapper() {
			if (null != _variable_clone_cache) return _variable_clone_cache;
			else return Lambdas.get(() -> {
				
				final Wrapper tmp_wrapper = new Wrapper(this);
				
				// now check if it's the same
				final boolean clone_is_identical = 
					StreamUtils.unfold(Tuples._2T(this, tmp_wrapper), old_new -> 
								Optional.ofNullable(old_new._1()._next).map(old_next -> Tuples._2T(old_next, old_new._2()._next)))
								.allMatch(old_new ->  old_new._1()._batch_module == old_new._2()._batch_module)
								;

				if (clone_is_identical) {
					_variable_clone_cache = tmp_wrapper;
				}
				return tmp_wrapper;
			});
		}
		
		////////////////////////////////////////////////////////////////
		
		// PROCESSING
		
		/** Processes the next stage of a chain of pipelines
		 * @param list
		 * @param parent_last_in_group - note only called immediately before the cleanup call
		 * @return
		 */
		@SuppressWarnings("unchecked")
		public Stream<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> process(final List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> list, boolean parent_last_in_group, boolean last_group, Optional<JsonNode> grouping_key) {
			_variable_list.addAll(list);
			if (parent_last_in_group || (_variable_list.size() > _batch_size)) {
				try {
					return createStream(Iterators.partition(_variable_list.iterator(), _batch_size)).flatMap(l_last -> {
						final boolean this_last = parent_last_in_group && l_last._2(); // (ie last dump of objects (in group if grouped) *and* final batch)
						
						// Apply batch object:
						final Function<fj.Unit, List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>>> applyObjectBatch = __ -> {
							final List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> stage_output = (List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>>)
									_enrichment_context.getUnderlyingPlatformDriver(List.class, Optional.empty()).get();
							
							stage_output.clear();
							_batch_module.onObjectBatch(l_last._1().stream().map(t2 -> t2._1()), Optional.of(l_last._1().size()), grouping_key);
							
							//TRACE
							//System.out.println("?? onObjectBatch: " + _control.name() + ": " + stage_output.size() + "... " + l_last._1().size() + " ..." + l_last._2());

							return stage_output;
						};			
						
						// Complete stage if needed
						final Function<List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>>, List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>>> applyStageComplete = out -> {
						
							if (!_is_streaming) { //(else never run)
								if ((null != _clone_of) && (_clone_of != _batch_module)) { // if this is a grouped record with cloning enabled then do a little differently
									if (this_last) _batch_module.onStageComplete(false);
									if (this_last && last_group) _clone_of.onStageComplete(true);
									
									//TRACE
									//System.out.println("?? onStageComplete: " + _control.name() + ": " + stage_output.size());						
								}
								else if (this_last && last_group) {
									_batch_module.onStageComplete(true); 
									
									//TRACE
									//System.out.println("?? onStageComplete: " + _control.name() + ": " + stage_output.size());
								}
							}
							return out;
						};

						// Update statistics
						final Function<List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>>, List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>>> applyUpdateStatistics = out -> {
							
							final int batch_in = l_last._1().size();
							final int batch_out = out.size();
							
							_stats.in += batch_in;
							_stats.out += batch_out;
							
							if (!_is_streaming || (batch_in > 0 || batch_out > 0)) {
								_logger.ifPresent(log -> log.log(Level.TRACE, 						
										ErrorUtils.lazyBuildMessage(true, () -> "EnrichmentPipelineService", 
												() -> Optional.ofNullable(_control.name()).orElse("no_name") + ".onObjectBatch", 
												() -> null, 
												() -> ErrorUtils.get("New batch stage {0} task={1} in={2} out={3} cumul_in={4}, cumul_out={5}", 
														Optional.ofNullable(_control.name()).orElse("(no name)"),  UUID, Integer.toString(batch_in), Integer.toString(batch_out), Integer.toString(_stats.in),  Integer.toString(_stats.out)),
												() -> null)
												));
							}
							
							if (this_last && !_is_streaming) {
								
								//TODO (ALEPH-63): make debug/info periods configurable
								if (!_is_streaming) {
									_logger.ifPresent(log -> log.log(Level.INFO, 						
											ErrorUtils.lazyBuildMessage(true, () -> "EnrichmentPipelineService", 
													() -> Optional.ofNullable(_control.name()).orElse("no_name") + ".onStageComplete", 
													() -> null, 
													() -> ErrorUtils.get("Completed stage {0} task={1} in={2} out={3}", 
															Optional.ofNullable(_control.name()).orElse("(no name)"),  UUID, Integer.toString(_stats.in),  Integer.toString(_stats.out)),
													() -> _mapper.convertValue(_stats, Map.class))
													));
								}
								else { // streaming, generate log every 5 minutes (DEBUG), hour (INFO)
									if (batch_in > 0 || batch_out > 0) { 
										_logger.ifPresent(log -> log.log(Level.DEBUG, 						
												ErrorUtils.lazyBuildMessage(true, () -> "EnrichmentPipelineService", 
														() -> Optional.ofNullable(_control.name()).orElse("no_name") + ".onStageComplete", 
														() -> null, 
														() -> ErrorUtils.get("Short period log stage {0} task={1} in={2} out={3}", 
																Optional.ofNullable(_control.name()).orElse("(no name)"),  UUID, Integer.toString(_stats.in),  Integer.toString(_stats.out)),
														() -> _mapper.convertValue(_stats, Map.class))
														,
														"debug_log", Arrays.asList(LoggingRules.logEveryMilliseconds(300*1000L)), Optional.empty(), LoggingMergeFunctions.replaceMessage()
												));
									}									
									_logger.ifPresent(log -> log.log(Level.INFO, 						
											ErrorUtils.lazyBuildMessage(true, () -> "EnrichmentPipelineService", 
													() -> Optional.ofNullable(_control.name()).orElse("no_name") + ".onStageComplete", 
													() -> null, 
													() -> ErrorUtils.get("Long period log stage {0} task={1} in={2} out={3}", 
															Optional.ofNullable(_control.name()).orElse("(no name)"),  UUID, Integer.toString(_stats.in),  Integer.toString(_stats.out)),
													() -> _mapper.convertValue(_stats, Map.class))
													,
													"debug_log", Arrays.asList(LoggingRules.logEveryMilliseconds(3600*1000L)), Optional.empty(), LoggingMergeFunctions.replaceMessage()
											));
								}
							}
							return out;
						};
						
						// Pipeline:
						return applyObjectBatch
								.andThen(applyStageComplete)
								.andThen(applyUpdateStatistics)
								.andThen(out -> {
									final Stream<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> ret_vals_int =  (null != _next)
											? _next.process(out, this_last, last_group, Optional.empty())
											: new ArrayList<>(out).stream() // (need to copy here since stage_output is mutable)
											;
											
									return ret_vals_int;									
								})
								.apply(Unit.unit())
								;						
					})
					.map(t2 -> {
						if (_emit_when_done) {
							@SuppressWarnings("unused")
							final Validation<BasicMessageBean, JsonNode> emit_success = 
									_analytics_context.emitObject(Optional.empty(), _analytics_context.getJob().get(), Either.left(t2._1()._2().getJson()), Optional.empty());
						}
						return t2;
					})
					;
				}
				finally {
					//TRACE
					//System.out.println("Clear " + parent_last_in_group + "... " + _variable_list.size());
					
					_variable_list = new LinkedList<>();										
				}
			}
			else return Stream.empty();
		}
	}
	
	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////
	
	// UTILS
	
	/** Adds the incoming element from the previous stage to the batch
	 * @param el
	 * @return
	 */
	protected static List<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> addIncomingRecord(final Tuple2<Long, IBatchRecord> el) {
		return Arrays.asList(Tuples._2T(el, Optional.empty()));
	}
	
	/** Handy utility to create a stream whose final tagged with "last" from an iterator 
	 * @param it
	 * @return
	 */
	private <T> Stream<Tuple2<T, Boolean>> createStream(final Iterator<T> it) {
		return  !it.hasNext()
		? 
		Stream.empty()
		: 
		StreamUtils.unfold(it, itit -> {
			return itit.hasNext() ? Optional.of(itit) : Optional.empty();
		})
		.map(itit -> Tuples._2T(itit.next(), !itit.hasNext()));		
	}
	
}
