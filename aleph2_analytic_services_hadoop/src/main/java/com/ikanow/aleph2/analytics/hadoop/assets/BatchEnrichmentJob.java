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
/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/package com.ikanow.aleph2.analytics.hadoop.assets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;
import scala.Tuple3;

import com.codepoetics.protonpack.StreamUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule.ProcessingStage;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable;
import com.ikanow.aleph2.analytics.hadoop.services.BatchEnrichmentContext;

import java.util.Arrays;

import fj.data.Either;

/** Encapsulates a Hadoop job intended for batch enrichment or analytics
 * @author jfreydank
 */
public class BatchEnrichmentJob{

	public static String BATCH_SIZE_PARAM = "aleph2.batch.batchSize";
	public static String BE_CONTEXT_SIGNATURE = "aleph2.batch.beContextSignature";
	public static String BE_DEBUG_MAX_SIZE = "aleph2.batch.debugMaxSize";

	private static final Logger logger = LogManager.getLogger(BatchEnrichmentJob.class);
	private static final org.apache.log4j.Logger _v1_logger = org.apache.log4j.LogManager.getLogger(BatchEnrichmentJob.class);
	
	public BatchEnrichmentJob(){
		logger.debug("BatchEnrichmentJob constructor");		
	}
	
	/** Common functions between mapper and reducer
	 * @author Alex
	 *
	 */
	protected static abstract class BatchEnrichmentBase implements IBeJobConfigurable {

		//////////////////////////////////////////////////////////////////////////////////////////////////
		
		// COMMON
		
		protected boolean _mapper_only_job = false;
		
		protected DataBucketBean _dataBucket = null;
		protected BatchEnrichmentContext _enrichmentContext = null;

		protected int _batchSize = 100;
		protected List<Tuple3<IEnrichmentBatchModule, BatchEnrichmentContext, EnrichmentControlMetadataBean>> _ecMetadata = null;
		
		protected List<Tuple2<Long, IBatchRecord>> _batch = new ArrayList<Tuple2<Long, IBatchRecord>>();
		
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setDataBucket(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
		 */
		@Override
		public void setDataBucket(DataBucketBean dataBucketBean) {
			this._dataBucket = dataBucketBean;			
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEnrichmentContext(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext)
		 */
		@Override
		public void setEnrichmentContext(BatchEnrichmentContext enrichmentContext) {
			this._enrichmentContext = enrichmentContext;
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setBatchSize(int)
		 */
		@Override
		public void setBatchSize(int bs) {
			this._batchSize=bs;			
		}

		/** Checks if we should send a batch of objects to the next stage in the pipeline
		 * @param flush
		 */
		protected void checkBatch(boolean flush){
			if (flush)
				_v1_logger.info("Completing job");
			
			if((_batch.size()>= _batchSize) || flush){
				final Iterator<Tuple3<IEnrichmentBatchModule, BatchEnrichmentContext, EnrichmentControlMetadataBean>> it = _ecMetadata.iterator();
				List<Tuple2<Long, IBatchRecord>> mutable_start = _batch;
				while (it.hasNext()) {
					final Tuple3<IEnrichmentBatchModule, BatchEnrichmentContext, EnrichmentControlMetadataBean> t3 = it.next();				
					
					t3._2().clearOutputRecords();
					t3._1().onObjectBatch(mutable_start.stream(), Optional.of(mutable_start.size()), Optional.empty());
					mutable_start = t3._2().getOutputRecords();
	
					if (flush)
						_v1_logger.info("Stage " + Optional.ofNullable(t3._3().name()).orElse("(no name)") + " output records=" + mutable_start.size() + " final_stage=" + !it.hasNext());
					
					if (!it.hasNext()) { // final stage output anything we have here
						completeBatchFinalStage(mutable_start);
					}
				}				
				_batch.clear();
			}		
		}
		
		/** Completes the final stage of a mapper/reducer/combiner
		 */
		public abstract void completeBatchFinalStage(final List<Tuple2<Long, IBatchRecord>> output_objects);
		
		/** Enrichment config initialization delegate - gets given a stream generator that modifies the incoming stream of jobs and only 
		 *  passes those that apply to the given mapper/reducer/combiner
		 * @param ecMetadata 
		 * @param streamGenerator
		 */
		public void setEcMetadata(List<EnrichmentControlMetadataBean> ecMetadata, UnaryOperator<Stream<EnrichmentControlMetadataBean>> streamGenerator)
		{
			_mapper_only_job = 
					ecMetadata.stream()
						.filter(cfg -> Optional.ofNullable(cfg.enabled()).orElse(true))
						.noneMatch(cfg -> !Optionals.ofNullable(cfg.dependencies()).isEmpty());
			
			final Map<String, SharedLibraryBean> library_beans = _enrichmentContext.getAnalyticsContext().getLibraryConfigs();
			this._ecMetadata = streamGenerator.apply(ecMetadata.stream().filter(cfg -> Optional.ofNullable(cfg.enabled()).orElse(true)))									
									.<Tuple3<IEnrichmentBatchModule, BatchEnrichmentContext, EnrichmentControlMetadataBean>>flatMap(ecm -> {
										final Optional<String> entryPoint = BucketUtils.getBatchEntryPoint(library_beans, ecm);
										
										_v1_logger.info("Trying to launch stage " + Optional.ofNullable(ecm.name()).orElse("(no name)") + " with entry point = " + entryPoint);
										
										return entryPoint.map(Stream::of).orElseGet(() -> Stream.of(BePassthroughModule.class.getName()))
												.flatMap(Lambdas.flatWrap_i(ep -> (IEnrichmentBatchModule)Class.forName(ep).newInstance()))
												.map(mod -> {			
													_v1_logger.info("Completed initialization of stage " + Optional.ofNullable(ecm.name()).orElse("(no name)"));
													
													final BatchEnrichmentContext cloned_context = new BatchEnrichmentContext(_enrichmentContext, _batchSize);
													Optional.ofNullable(library_beans.get(ecm.module_name_or_id())).ifPresent(lib -> cloned_context.setModule(lib));													
													return Tuples._3T(mod, cloned_context, ecm);
												});
									})
									.collect(Collectors.toList());
		}
	
	}
	
	/** Pulls out batch enrichment parameters from the Hadoop configuration file
	 *  (Common to mapper and reducer)
	 * @param beJobConfigurable
	 * @param configuration
	 * @throws Exception
	 */
	public static void extractBeJobParameters(IBeJobConfigurable beJobConfigurable, Configuration configuration) throws Exception{
		
		final String contextSignature = configuration.get(BE_CONTEXT_SIGNATURE);  
		final BatchEnrichmentContext enrichmentContext = (BatchEnrichmentContext) ContextUtils.getEnrichmentContext(contextSignature);
		
		beJobConfigurable.setEnrichmentContext(enrichmentContext);
		final DataBucketBean dataBucket = enrichmentContext.getBucket().get();
		beJobConfigurable.setDataBucket(dataBucket);
		final List<EnrichmentControlMetadataBean> config = Optional.ofNullable(dataBucket.batch_enrichment_configs()).orElse(Collections.emptyList());
		beJobConfigurable.setEcMetadata(config.isEmpty()
											? Arrays.asList(BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get())
											: config
				);
		beJobConfigurable.setBatchSize(configuration.getInt(BATCH_SIZE_PARAM,100));	
	}
	
	//////////////////////////////////////////////////////////////////////////////////////////////////
	
	// MAPPER
		
	/** All the mapper functionality, just not in mapper form because of java8 multiple inheritance rules
	 * @author Alex
	 */
	protected static class BatchEnrichmentBaseMapper extends BatchEnrichmentBase {
 
		/** Setup delegate
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void setup(Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>.Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob setup");
			
			try {
				BatchEnrichmentJob.extractBeJobParameters(this, context.getConfiguration());
			} catch (Exception e) {
				throw new IOException(e);
			}			
			_v1_logger.info("Setup BatchEnrichmentJob for " + this._enrichmentContext.getBucket().map(b -> b.full_name()).orElse("unknown"));
			
			final Iterator<Tuple3<IEnrichmentBatchModule, BatchEnrichmentContext, EnrichmentControlMetadataBean>> it = _ecMetadata.iterator();
			ProcessingStage mutable_prev_stage = ProcessingStage.input;
			while (it.hasNext()) {
				final Tuple3<IEnrichmentBatchModule, BatchEnrichmentContext, EnrichmentControlMetadataBean> t3 = it.next();
				
				_v1_logger.info("Set up enrichment module " + t3._2().getClass().getSimpleName() + " name " + Optional.ofNullable(t3._3().name()).orElse("(no name)"));
				
				t3._1().onStageInitialize(t3._2(), _dataBucket, t3._3(), 
						Tuples._2T(
							mutable_prev_stage
							, 
							!it.hasNext()
								? (_mapper_only_job ? ProcessingStage.output : ProcessingStage.grouping)
								: ProcessingStage.batch
						),
						Optional.empty() //TODO (ALEPH-12): if next stage is not mapper then pass in fields
						);	
			}
			
		} // setup
		
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEcMetadata(com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean)
		 */
		@Override
		public void setEcMetadata(List<EnrichmentControlMetadataBean> ecMetadata) {
			// Only take the initial set of pre-reducer jobs
			this.setEcMetadata(ecMetadata, s -> StreamUtils.takeWhile(s, cfg -> Optionals.ofNullable(cfg.grouping_fields()).isEmpty()));
		}
		
		/** Mapper delegate
		 * @param key
		 * @param value
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void map(String key, Tuple2<Long, IBatchRecord> value,
				Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>.Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob map");
			
			_batch.add(value);
			checkBatch(false);
		} // map

		/** cleanup delegate
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void cleanup(
				Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>.Context context)
				throws IOException, InterruptedException {
			checkBatch(true);
			
			//DEBUG
			//System.out.println("Flushing output....." + new java.util.Date());
			
			_ecMetadata.stream().forEach(ecm -> ecm._1().onStageComplete(true));
			if (null != _enrichmentContext) {
				_enrichmentContext.flushBatchOutput(Optional.empty()).join();
			}
						
			//DEBUG
			//System.out.println("Completed Flushing output....." + new java.util.Date());			
		}


		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.assets.BatchEnrichmentJob.BatchEnrichmentBase#completeBatchFinalStage(java.util.List)
		 */
		@Override
		public void completeBatchFinalStage(
				List<Tuple2<Long, IBatchRecord>> output_objects) {
			final IAnalyticsContext analytics_context = _enrichmentContext.getAnalyticsContext();
			output_objects.forEach(record ->
				analytics_context.emitObject(Optional.empty(), _enrichmentContext.getJob(), Either.left(record._2().getJson()), Optional.empty()));
		}
		
	}
	
	/** Mapper implementation - just delegate everything to BatchEnrichmentBaseMapper
	 * @author Alex
	 */
	public static class BatchEnrichmentMapper extends Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>> {
		protected final BatchEnrichmentBaseMapper _delegate = new BatchEnrichmentBaseMapper();
		
		/** User c'tor
		 */
		public BatchEnrichmentMapper(){
			super();
			logger.debug("BatchErichmentMapper constructor");
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void setup(Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>.Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob setup");
			
			_delegate.setup(context);
			
		} // setup

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(String key, Tuple2<Long, IBatchRecord> value,
				Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>.Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob map");
			
			_delegate.map(key, value, context);
		} // map


		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void cleanup(
				Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>.Context context)
				throws IOException, InterruptedException {
			_delegate.cleanup(context);
		}


	} //BatchErichmentMapper

	//////////////////////////////////////////////////////////////////////////////////////////////////
	
	// REDUCER (combiner below)
			
	protected static class BatchEnrichmentBaseReducer extends BatchEnrichmentBase {
		protected final boolean is_combiner;
		
		public BatchEnrichmentBaseReducer(final boolean is_combiner) {
			this.is_combiner = is_combiner;
		}
		
		/** Setup delegate
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void setup(
				Reducer<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>, String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		}
		
		/** Reduce delegate
		 * @param arg0
		 * @param arg1
		 * @param arg2
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void reduce(
				String arg0,
				Iterable<Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> arg1,
				Reducer<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>, String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		}

		/** Cleanup delegate
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		protected void cleanup(
				Reducer<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>, String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		}

		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEcMetadata(com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean)
		 */
		@Override
		public void setEcMetadata(List<EnrichmentControlMetadataBean> ecMetadata) {
			// Only take the reducer + (reducer-not-combiner) post reducer jobs 
			this.setEcMetadata(ecMetadata, is_combiner
					? s -> s.filter(cfg -> !Optionals.ofNullable(cfg.grouping_fields()).isEmpty())
					: s -> StreamUtils.skipWhile(s, cfg -> Optionals.ofNullable(cfg.grouping_fields()).isEmpty()));
		}

		@Override
		public void completeBatchFinalStage(
				List<Tuple2<Long, IBatchRecord>> output_objects) {
			// TODO Auto-generated method stub			
		}
		
	}
		
	/** The reducer version
	 * @author jfreydank
	 */
	public static class BatchEnrichmentReducer extends Reducer<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>, String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> {
		protected BatchEnrichmentBaseReducer _delegate;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void cleanup(
				Reducer<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>, String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(
				String arg0,
				Iterable<Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> arg1,
				Reducer<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>, String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(arg0, arg1, arg2);
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void setup(
				Reducer<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>, String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>.Context context)
				throws IOException, InterruptedException {			
			_delegate = new BatchEnrichmentBaseReducer(false);
			_delegate.setup(context);
		}

	} // reducer

	//////////////////////////////////////////////////////////////////////////////////////////////////
	
	// COMBINER
			
	/** The combiner version
	 * @author jfreydank
	 */
	public static class BatchEnrichmentCombiner extends  BatchEnrichmentReducer {
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void setup(
				Reducer<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>, String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>.Context context)
				throws IOException, InterruptedException {			
			_delegate = new BatchEnrichmentBaseReducer(false);
			_delegate.setup(context);
		}				
	}
	
}
