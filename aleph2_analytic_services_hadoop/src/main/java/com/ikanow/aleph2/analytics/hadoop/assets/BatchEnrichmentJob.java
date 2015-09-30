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
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;
import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.analytics.hadoop.data_model.BeJobBean;
import com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable;

/** Encapsulates a Hadoop job intended for batch enrichment or analytics
 * @author jfreydank
 */
public class BatchEnrichmentJob{

	public static String BATCH_SIZE_PARAM = "batchSize";
	public static String BE_META_BEAN_PARAM = "metadataName";
	public static String BE_CONTEXT_SIGNATURE = "beContextSignature";

	private static final Logger logger = LogManager.getLogger(BatchEnrichmentJob.class);
	
	public BatchEnrichmentJob(){
		logger.debug("BatchEnrichmentJob constructor");		
	}
	
	/** Mapper implementation
	 * @author Alex
	 */
	public static class BatchEnrichmentMapper extends Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>		
	implements IBeJobConfigurable {

		protected DataBucketBean _dataBucket = null;
		protected IEnrichmentBatchModule _enrichmentBatchModule = null;			
		protected IEnrichmentModuleContext _enrichmentContext = null;

		private int _batchSize = 100;
		protected BeJobBean _beJob = null;
		protected EnrichmentControlMetadataBean _ecMetadata = null;
		protected SharedLibraryBean _beSharedLibrary = null;
		
		protected List<Tuple2<Long, IBatchRecord>> _batch = new ArrayList<Tuple2<Long, IBatchRecord>>();
				
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
			
			try {
				BatchEnrichmentJob.extractBeJobParameters(this, context.getConfiguration());
			} catch (Exception e) {
				throw new IOException(e);
			}			
			
			// TODO (ALEPH-12) check where final_stage is defined
			final boolean final_stage = true;
			_enrichmentBatchModule.onStageInitialize(_enrichmentContext, _dataBucket, final_stage);
			
		} // setup

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(String key, Tuple2<Long, IBatchRecord> value,
				Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>.Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob map");
			
			_batch.add(value);
			checkBatch(false);
		} // map

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEcMetadata(com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean)
		 */
		@Override
		public void setEcMetadata(EnrichmentControlMetadataBean ecMetadata) {
			this._ecMetadata = ecMetadata;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setBeSharedLibrary(com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean)
		 */
		@Override
		public void setBeSharedLibrary(SharedLibraryBean beSharedLibrary) {
			this._beSharedLibrary = beSharedLibrary;
		}

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
		public void setEnrichmentContext(IEnrichmentModuleContext enrichmentContext) {
			this._enrichmentContext = enrichmentContext;
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void cleanup(
				Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>.Context context)
				throws IOException, InterruptedException {
			checkBatch(true);
			_enrichmentBatchModule.onStageComplete(true);		
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setBatchSize(int)
		 */
		@Override
		public void setBatchSize(int bs) {
			this._batchSize=bs;
			
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEnrichmentBatchModule(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule)
		 */
		@Override
		public void setEnrichmentBatchModule(IEnrichmentBatchModule ebm) {
			this._enrichmentBatchModule = ebm;
			
		}
		
		/** Checks if we should send a batch of objects to the next stage in the pipeline
		 * @param flush
		 */
		protected void checkBatch(boolean flush){
			if((_batch.size()>= _batchSize) || flush){
				_enrichmentBatchModule.onObjectBatch(_batch.stream(), Optional.of(_batch.size()), Optional.empty());
				_batch.clear();
			}		
		}
		
	} //BatchErichmentMapper

	/** The reducer version
	 * @author jfreydank
	 */
	public static class BatchEnrichmentReducer extends Reducer<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>, String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> {

		
	} // reducer

	/** Pulls out batch enrichment parameters from the Hadoop configuration file
	 * @param beJobConfigurable
	 * @param configuration
	 * @throws Exception
	 */
	public static void extractBeJobParameters(IBeJobConfigurable beJobConfigurable, Configuration configuration) throws Exception{
		
		final String contextSignature = configuration.get(BE_CONTEXT_SIGNATURE);  
		final IEnrichmentModuleContext enrichmentContext = ContextUtils.getEnrichmentContext(contextSignature);
		beJobConfigurable.setEnrichmentContext(enrichmentContext);
		final DataBucketBean dataBucket = enrichmentContext.getBucket().get();
		beJobConfigurable.setDataBucket(dataBucket);
		final SharedLibraryBean beSharedLibrary = enrichmentContext.getModuleConfig();
		beJobConfigurable.setBeSharedLibrary(beSharedLibrary);		
		beJobConfigurable.setEcMetadata(BeJobBean.extractEnrichmentControlMetadata(dataBucket, configuration.get(BE_META_BEAN_PARAM)));
		beJobConfigurable.setBatchSize(configuration.getInt(BATCH_SIZE_PARAM,100));	
		beJobConfigurable.setEnrichmentBatchModule(
				Optional.ofNullable(beSharedLibrary.batch_enrichment_entry_point())
						.<IEnrichmentBatchModule>map(Lambdas.wrap_u(entry_point -> (IEnrichmentBatchModule)Class.forName(entry_point).newInstance()))
						.orElseGet(() -> new BePassthroughModule())) //(default)
				;
	}
	
}
