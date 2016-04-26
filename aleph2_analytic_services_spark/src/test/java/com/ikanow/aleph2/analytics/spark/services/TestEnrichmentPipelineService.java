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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.ikanow.aleph2.core.shared.utils.BatchRecordUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean.StateDirectoryType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Tuples;

import fj.data.Either;
import fj.data.Validation;

/**
 * @author Alex
 *
 */
public class TestEnrichmentPipelineService {
	final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	static JavaSparkContext _spark;
	
	@Before
	public void setup() {
		if (null != _spark) {
			return;
		}
		_spark = new JavaSparkContext("local", "TestEnrichmentPipelineService");
	}
	
	@Test
	public void test_groupingBehavior() {
		
		//(quickly test the whole thing works!)
		{
			JavaRDD<String> test = _spark.parallelize(Arrays.asList("a", "b", "c"));		
			assertEquals(3L, test.map(s -> s + "X").count());		
		}		
		
		// (sample group)
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
	
	//////////////////////////////////////////////////////////////
	
	@Test
	public void test_inMapPartitions() {

		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, "/test").done().get();
		
		final IAnalyticsContext mock_analytics_context = Mockito.mock(IAnalyticsContext.class, Mockito.withSettings().serializable()); 		
		// ^ (can't include IAnalyticsContext in here, which is going to make life unpleasant)
		Mockito.when(mock_analytics_context.getUnderlyingPlatformDriver(Mockito.eq(IEnrichmentModuleContext.class), Mockito.any()))
				.thenAnswer(new ContextAnswer());
		Mockito.when(mock_analytics_context.getLibraryConfigs())
				.thenAnswer(new LibraryConfigsAnswer());
		Mockito.when(mock_analytics_context.getBucket())
				.thenAnswer(new BucketAnswer(test_bucket));
		
		// Empty set should fail:
		{
			final List<EnrichmentControlMetadataBean> pipeline_elements = 
					Arrays.asList();
		
			try {
				@SuppressWarnings("unused")
				final EnrichmentPipelineService under_test = EnrichmentPipelineService.create(mock_analytics_context, pipeline_elements);
				fail("Should error");
			}
			catch (Exception e) {
				// Good
			}
		}		
		// Now try with a non trivial set of enrichment elements
		{
			final List<EnrichmentControlMetadataBean> pipeline_elements = 
					Arrays.asList(
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
								.with(EnrichmentControlMetadataBean::name, "test1")
								.with(EnrichmentControlMetadataBean::entry_point, TestEnrichmentModule.class.getName())								
								.with(EnrichmentControlMetadataBean::technology_override, 
										new LinkedHashMap<String, Object>(ImmutableMap.of(
												"batch_size", "10"
												))
										)
								.with(EnrichmentControlMetadataBean::config, 
										new LinkedHashMap<String, Object>(ImmutableMap.of(
												"append_field", "test1_field",
												"stop_field", "test1_stop"
												))
										)
							.done().get()
							,
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
								.with(EnrichmentControlMetadataBean::name, "test2")
								.with(EnrichmentControlMetadataBean::entry_point, TestEnrichmentModule.class.getName())
								.with(EnrichmentControlMetadataBean::technology_override, 
										new LinkedHashMap<String, Object>(ImmutableMap.of(
												"batch_size", 7
												))
										)
								.with(EnrichmentControlMetadataBean::config, 
										new LinkedHashMap<String, Object>(ImmutableMap.of(
												"append_field", "test2_field",
												"stop_field", "test2_stop"
												))
										)
							.done().get()
							,
							BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
								.with(EnrichmentControlMetadataBean::name, "test3")
								.with(EnrichmentControlMetadataBean::entry_point, TestEnrichmentModule.class.getName())
								.with(EnrichmentControlMetadataBean::technology_override, 
										new LinkedHashMap<String, Object>(ImmutableMap.of(
												"batch_size", 20L
												))
										)
								.with(EnrichmentControlMetadataBean::config, 
										new LinkedHashMap<String, Object>(ImmutableMap.of(
												"append_field", "test3_field",
												"stop_field", "test3_stop"
												))
										)
							.done().get()
							);			
			
			// Actual test:
			{
				final EnrichmentPipelineService under_test = EnrichmentPipelineService.create(mock_analytics_context, pipeline_elements);
				
				JavaRDD<Tuple2<Long, IBatchRecord>> test = _spark.parallelize(Arrays.asList(
						_mapper.createObjectNode().put("id", "1"),
						_mapper.createObjectNode().put("id", "2").put("test1_stop", true),
						_mapper.createObjectNode().put("id", "3"),
						_mapper.createObjectNode().put("id", "4"),
						_mapper.createObjectNode().put("id", "5").put("test2_stop", true),
						_mapper.createObjectNode().put("id", "6"),
						_mapper.createObjectNode().put("id", "7"),
						_mapper.createObjectNode().put("id", "8"),
						_mapper.createObjectNode().put("id", "9").put("test3_stop", true),
						_mapper.createObjectNode().put("id", "10")
						)
						.stream()
						.<Tuple2<Long, IBatchRecord>>map(j -> Tuples._2T(0L, new BatchRecordUtils.JsonBatchRecord((JsonNode) j)))
						.collect(Collectors.toList()))
						;		
				
				JavaRDD<Tuple2<Long, IBatchRecord>> out = test.mapPartitions(under_test.javaInMapPartitions());
				assertEquals(10, out.count());			
				assertEquals(7, out.filter(t2 -> t2._2().getJson().has("test1_field")).count()); // 7 objects that pass through and aren't filtered by any stages (not the appended 1)
				assertEquals(8, out.filter(t2 -> t2._2().getJson().has("test2_field")).count()); // 8 objects that pass through (7+1 from parent) and .. (not the appended 1)
				assertEquals(9, out.filter(t2 -> t2._2().getJson().has("test3_field")).count()); // 9 objects that pass through (8+1 from parent) and .. (not the appended 1)
				assertEquals(3, out.filter(t2 -> t2._2().getJson().has("appended")).count());
				
				// Scala version:
				final EnrichmentPipelineService scala_under_test = EnrichmentPipelineService.create(mock_analytics_context, pipeline_elements);
				assertEquals(10, test.rdd().mapPartitions(scala_under_test.inMapPartitions(), true, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class)).count());
				
			}
			// Test with pre-group
			{
				final EnrichmentPipelineService under_test = EnrichmentPipelineService.create(mock_analytics_context, pipeline_elements);

				JavaRDD<Tuple2<Long, IBatchRecord>> test = _spark.parallelize(Arrays.asList(
						_mapper.createObjectNode().put("id", "1").put("grouper", "A"),
						_mapper.createObjectNode().put("id", "2").put("test1_stop", true),
						_mapper.createObjectNode().put("id", "3").put("grouper", "A"),
						_mapper.createObjectNode().put("id", "4").put("grouper", "B"),
						_mapper.createObjectNode().put("id", "5").put("test2_stop", true),
						_mapper.createObjectNode().put("id", "6").put("grouper", "B"),
						_mapper.createObjectNode().put("id", "7").put("grouper", "C"),
						_mapper.createObjectNode().put("id", "8").put("grouper", "C"),
						_mapper.createObjectNode().put("id", "9").put("test3_stop", true).put("grouper", "D"), // (wont' get promoted)
						_mapper.createObjectNode().put("id", "10").put("grouper", "D")
						)
						.stream()
						.<Tuple2<Long, IBatchRecord>>map(j -> Tuples._2T(0L, new BatchRecordUtils.JsonBatchRecord((JsonNode) j)))
						.collect(Collectors.toList()))
						;		
				
				JavaRDD<Tuple2<IBatchRecord, Tuple2<Long, IBatchRecord>>> out = 
						JavaRDD.fromRDD(
								test.rdd().mapPartitions(under_test.inMapPartitionsPreGroup("grouper"), true, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class)),
								scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class))
								;
						
				//TRACE
				//out.foreach(f -> System.out.println(f + " ... " + f._1()._2().getJson()));
				
				assertEquals(10, out.count());			
				assertEquals(7, out.filter(t2 -> t2._2()._2().getJson().has("test1_field")).count()); // 7 objects that pass through and aren't filtered by any stages (not the appended 1)
				assertEquals(8, out.filter(t2 -> t2._2()._2().getJson().has("test2_field")).count()); // 8 objects that pass through (7+1 from parent) and .. (not the appended 1)
				assertEquals(9, out.filter(t2 -> t2._2()._2().getJson().has("test3_field")).count()); // 9 objects that pass through (8+1 from parent) and .. (not the appended 1)
				assertEquals(3, out.filter(t2 -> t2._2()._2().getJson().has("appended")).count());
				assertEquals(2, out.filter(t2 -> Optional.ofNullable(t2._1().getJson().get("grouper")).map(j -> j.asText()).map(s -> s.equals("A")).orElse(false)).count());
				assertEquals(2, out.filter(t2 -> Optional.ofNullable(t2._1().getJson().get("grouper")).map(j -> j.asText()).map(s -> s.equals("B")).orElse(false)).count());
				assertEquals(2, out.filter(t2 -> Optional.ofNullable(t2._1().getJson().get("grouper")).map(j -> j.asText()).map(s -> s.equals("C")).orElse(false)).count());
				assertEquals(1, out.filter(t2 -> Optional.ofNullable(t2._1().getJson().get("grouper")).map(j -> j.asText()).map(s -> s.equals("D")).orElse(false)).count());
				assertEquals(3, out.filter(t2 -> 0 == t2._1().getJson().size()).count()); // (the injected records)
				
				// Other scala version/test can reuse the function. ie it's immutable:
				assertEquals(10, test.rdd().mapPartitions(under_test.inMapPartitionsPreGroup(Arrays.asList("group")), true, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class)).count());				
			}
			// Test with post group (no cloing of test_1 enricher)
			{
				final EnrichmentPipelineService under_test = EnrichmentPipelineService.create(mock_analytics_context, pipeline_elements);

				RDD<Tuple2<IBatchRecord, Iterable<Tuple2<Long, IBatchRecord>>>> test = 
						_spark.parallelize(Arrays.asList(
						_mapper.createObjectNode().put("id", "1").put("grouper", "A"),
						_mapper.createObjectNode().put("id", "2").put("test1_stop", true).put("grouper", "A"),
						_mapper.createObjectNode().put("id", "3").put("grouper", "A"),
						_mapper.createObjectNode().put("id", "4").put("grouper", "B"),
						_mapper.createObjectNode().put("id", "5").put("test2_stop", true).put("grouper", "B"),
						_mapper.createObjectNode().put("id", "6").put("grouper", "B"),
						_mapper.createObjectNode().put("id", "7").put("grouper", "C"),
						_mapper.createObjectNode().put("id", "8").put("grouper", "C"),
						_mapper.createObjectNode().put("id", "9").put("test3_stop", true).put("grouper", "D"), // (wont' get promoted)
						_mapper.createObjectNode().put("id", "10").put("grouper", "D")
						)
						.stream()
						.<Tuple2<Long, IBatchRecord>>
							map(j -> Tuples._2T(0L, new BatchRecordUtils.JsonBatchRecord((JsonNode) j)))
						.collect(Collectors.toList()))
						.groupBy(t2 -> (IBatchRecord) new BatchRecordUtils.JsonBatchRecord(t2._2().getJson().get("grouper")))
						.rdd()
						;		

				assertEquals(4, test.count());											
				
				JavaRDD<Tuple2<Long, IBatchRecord>> out = 
						JavaRDD.fromRDD(
								test.mapPartitions(under_test.inMapPartitionsPostGroup(), true, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class)),
								scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class))
								;
				
				assertEquals(10, out.count());							
				
			}
			//TODO: test with clone mode enabled
		}
		
	}
	
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	
	// Some enrichment services:
	
	public static class TestEnrichmentModule implements IEnrichmentBatchModule {

		Optional<List<String>> _next_grouping_fields;
		String _append_field;
		String _stop_field;
		String _name;
		IEnrichmentModuleContext _context;
		
		//TODO: boolean clone mode
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageInitialize(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean, scala.Tuple2, java.util.Optional)
		 */
		@Override
		public void onStageInitialize(IEnrichmentModuleContext context,
				DataBucketBean bucket, EnrichmentControlMetadataBean control,
				Tuple2<ProcessingStage, ProcessingStage> previous_next,
				Optional<List<String>> next_grouping_fields) {
			
			_append_field = Optional.ofNullable(control.config().get("append_field")).orElse("").toString();
			_stop_field = Optional.ofNullable(control.config().get("stop_field")).orElse("").toString();
			_context = context;
			_next_grouping_fields = next_grouping_fields;
			_name = control.name();
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onObjectBatch(java.util.stream.Stream, java.util.Optional, java.util.Optional)
		 */
		@Override
		public void onObjectBatch(Stream<Tuple2<Long, IBatchRecord>> batch,
				Optional<Integer> batch_size, Optional<JsonNode> grouping_key) {
						
			batch.forEach(t2 -> {
				final ObjectNode j = (ObjectNode) t2._2().getJson();
				if (!j.has(_stop_field)) {
					j.put(_append_field, "test");
					_context.emitMutableObject(t2._1(), j, Optional.empty(), 
								_next_grouping_fields.map(ff -> ff.stream()
							.reduce(_mapper.createObjectNode(), (acc, v) -> (ObjectNode) Optional.ofNullable(j.get(v)).map(jj -> acc.set(v, jj)).orElse(acc), (j1, j2) -> j1)
							).map(o -> (JsonNode)o).filter(o -> o.size() > 0)
						);
				}
			});
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageComplete(boolean)
		 */
		@Override
		public void onStageComplete(boolean is_original) {
			// Send one extra object
			final ObjectNode o = _mapper.createObjectNode().put("appended", _name);
			_context.emitMutableObject(1000, o, Optional.empty(), Optional.empty());
		}
		
	}

	////////////////////////////////////////////////////
	
	public static class TestEnrichmentContext implements IEnrichmentModuleContext {
		final LinkedList<Tuple2<Tuple2<Long, IBatchRecord>, Optional<JsonNode>>> _l = new LinkedList<>();
		
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
		 */
		@Override
		public Collection<Object> getUnderlyingArtefacts() {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
		 */
		@SuppressWarnings("unchecked")
		@Override
		public <T> Optional<T> getUnderlyingPlatformDriver(
				Class<T> driver_class, Optional<String> driver_options) {
			
			if (List.class.isAssignableFrom(driver_class)) return (Optional<T>) Optional.of(_l);			
			else return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getEnrichmentContextSignature(java.util.Optional, java.util.Optional)
		 */
		@Override
		public String getEnrichmentContextSignature(
				Optional<DataBucketBean> bucket,
				Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyEntryPoints(java.lang.Class, java.util.Optional)
		 */
		@Override
		public <T> Collection<Tuple2<T, String>> getTopologyEntryPoints(
				Class<T> clazz, Optional<DataBucketBean> bucket) {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyStorageEndpoint(java.lang.Class, java.util.Optional)
		 */
		@Override
		public <T> T getTopologyStorageEndpoint(Class<T> clazz,
				Optional<DataBucketBean> bucket) {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyErrorEndpoint(java.lang.Class, java.util.Optional)
		 */
		@Override
		public <T> T getTopologyErrorEndpoint(Class<T> clazz,
				Optional<DataBucketBean> bucket) {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getNextUnusedId()
		 */
		@Override
		public long getNextUnusedId() {
			
			return 0;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#convertToMutable(com.fasterxml.jackson.databind.JsonNode)
		 */
		@Override
		public ObjectNode convertToMutable(JsonNode original) {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emitMutableObject(long, com.fasterxml.jackson.databind.node.ObjectNode, java.util.Optional, java.util.Optional)
		 */
		@Override
		public Validation<BasicMessageBean, JsonNode> emitMutableObject(
				long id, ObjectNode mutated_json,
				Optional<AnnotationBean> annotations,
				Optional<JsonNode> grouping_key) {
			
			_l.add(Tuples._2T(Tuples._2T(id, new BatchRecordUtils.JsonBatchRecord(mutated_json)), grouping_key));
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emitImmutableObject(long, com.fasterxml.jackson.databind.JsonNode, java.util.Optional, java.util.Optional, java.util.Optional)
		 */
		@Override
		public Validation<BasicMessageBean, JsonNode> emitImmutableObject(
				long id, JsonNode original_json,
				Optional<ObjectNode> mutations,
				Optional<AnnotationBean> annotations,
				Optional<JsonNode> grouping_key) {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#storeErroredObject(long, com.fasterxml.jackson.databind.JsonNode)
		 */
		@Override
		public void storeErroredObject(long id, JsonNode original_json) {
			
			
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#externalEmit(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, fj.data.Either, java.util.Optional)
		 */
		@Override
		public Validation<BasicMessageBean, JsonNode> externalEmit(
				DataBucketBean bucket,
				Either<JsonNode, Map<String, Object>> object,
				Optional<AnnotationBean> annotations) {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#flushBatchOutput(java.util.Optional)
		 */
		@Override
		public CompletableFuture<?> flushBatchOutput(
				Optional<DataBucketBean> bucket) {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getServiceContext()
		 */
		@Override
		public IServiceContext getServiceContext() {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getGlobalEnrichmentModuleObjectStore(java.lang.Class, java.util.Optional)
		 */
		@Override
		public <S> Optional<ICrudService<S>> getGlobalEnrichmentModuleObjectStore(
				Class<S> clazz, Optional<String> collection) {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucketObjectStore(java.lang.Class, java.util.Optional, java.util.Optional, java.util.Optional)
		 */
		@Override
		public <S> ICrudService<S> getBucketObjectStore(Class<S> clazz,
				Optional<DataBucketBean> bucket, Optional<String> collection,
				Optional<StateDirectoryType> type) {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucket()
		 */
		@Override
		public Optional<DataBucketBean> getBucket() {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getModuleConfig()
		 */
		@Override
		public Optional<SharedLibraryBean> getModuleConfig() {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucketStatus(java.util.Optional)
		 */
		@Override
		public Future<DataBucketStatusBean> getBucketStatus(
				Optional<DataBucketBean> bucket) {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getLogger(java.util.Optional)
		 */
		@Override
		public IBucketLogger getLogger(Optional<DataBucketBean> bucket) {
			
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emergencyDisableBucket(java.util.Optional)
		 */
		@Override
		public void emergencyDisableBucket(Optional<DataBucketBean> bucket) {
			
			
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emergencyQuarantineBucket(java.util.Optional, java.lang.String)
		 */
		@Override
		public void emergencyQuarantineBucket(Optional<DataBucketBean> bucket,
				String quarantine_duration) {
			
			
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#initializeNewContext(java.lang.String)
		 */
		@Override
		public void initializeNewContext(String signature) {
			
			
		}
		
	}
	
	////////////////////////////////////////////////////
	
	// Analytics context
	
	@SuppressWarnings("rawtypes")
	public static class ContextAnswer implements Answer, Serializable {
		private static final long serialVersionUID = -3489108224499090639L;

		/* (non-Javadoc)
		 * @see org.mockito.stubbing.Answer#answer(org.mockito.invocation.InvocationOnMock)
		 */
		@Override
		public Object answer(InvocationOnMock invocation) throws Throwable {
			return Optional.of(new TestEnrichmentContext());
		}
		
	}
	@SuppressWarnings("rawtypes")
	public static class LibraryConfigsAnswer implements Answer, Serializable {
		private static final long serialVersionUID = -2951282076341800662L;

		/* (non-Javadoc)
		 * @see org.mockito.stubbing.Answer#answer(org.mockito.invocation.InvocationOnMock)
		 */
		@Override
		public Object answer(InvocationOnMock invocation) throws Throwable {
			return Collections.emptyMap();
		}
		
	}
	@SuppressWarnings("rawtypes")
	public static class BucketAnswer implements Answer, Serializable {
		private static final long serialVersionUID = -7059333322269908505L;
		
		final DataBucketBean _bucket;
		
		BucketAnswer(final DataBucketBean bucket) { _bucket = bucket; }
		
		/* (non-Javadoc)
		 * @see org.mockito.stubbing.Answer#answer(org.mockito.invocation.InvocationOnMock)
		 */
		@Override
		public Object answer(InvocationOnMock invocation) throws Throwable {
			return Optional.of(_bucket);
		}
		
	}
	
}
