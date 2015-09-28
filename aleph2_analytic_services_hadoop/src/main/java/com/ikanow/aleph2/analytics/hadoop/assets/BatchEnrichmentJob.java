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
	 *  TODO: there seem to be 2 of these, this one and the one in BatchEnrichmentJob?
	 * @author Alex
	 */
	public static class BatchEnrichmentMapper extends Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>		
	implements IBeJobConfigurable {

		protected DataBucketBean dataBucket = null;
		protected IEnrichmentBatchModule enrichmentBatchModule = null;			

		protected IEnrichmentModuleContext enrichmentContext = null;

		@SuppressWarnings("unused")
		private int batchSize = 100;
		protected BeJobBean beJob = null;;
		protected EnrichmentControlMetadataBean ecMetadata = null;
		protected SharedLibraryBean beSharedLibrary = null;
		
		/** User c'tore
		 */
		public BatchEnrichmentMapper(){
			super();
			System.out.println("BatchErichmentMapper constructor");
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void setup(Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>.Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob setup");
			try{
				

			//extractBeJobParameters(this,  context.getConfiguration());
			
			
			}
			catch(Exception e){
				logger.error("Caught Exception",e);
			}

		} // setup

		

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(String key, Tuple2<Long, IBatchRecord> value,
				Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>.Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob map");
			context.write(key, value); //(the writer is where the batch enrichment code is batched and written)
		} // map


		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEcMetadata(com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean)
		 */
		@Override
		public void setEcMetadata(EnrichmentControlMetadataBean ecMetadata) {
			this.ecMetadata = ecMetadata;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setBeSharedLibrary(com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean)
		 */
		@Override
		public void setBeSharedLibrary(SharedLibraryBean beSharedLibrary) {
			this.beSharedLibrary = beSharedLibrary;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setDataBucket(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
		 */
		@Override
		public void setDataBucket(DataBucketBean dataBucketBean) {
			this.dataBucket = dataBucketBean;
			
		}
			
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEnrichmentContext(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext)
		 */
		@Override
		public void setEnrichmentContext(IEnrichmentModuleContext enrichmentContext) {
			this.enrichmentContext = enrichmentContext;
		}
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void cleanup(
				Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>.Context context)
				throws IOException, InterruptedException {
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setBatchSize(int)
		 */
		@Override
		public void setBatchSize(int bs) {
			this.batchSize=bs;
			
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEnrichmentBatchModule(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule)
		 */
		@Override
		public void setEnrichmentBatchModule(IEnrichmentBatchModule ebm) {
			this.enrichmentBatchModule = ebm;
			
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
		
		String contextSignature = configuration.get(BE_CONTEXT_SIGNATURE);  
		IEnrichmentModuleContext enrichmentContext = ContextUtils.getEnrichmentContext(contextSignature);
		beJobConfigurable.setEnrichmentContext(enrichmentContext);
		DataBucketBean dataBucket = enrichmentContext.getBucket().get();
		beJobConfigurable.setDataBucket(dataBucket);
		SharedLibraryBean beSharedLibrary = enrichmentContext.getModuleConfig();
		beJobConfigurable.setBeSharedLibrary(beSharedLibrary);		
		beJobConfigurable.setEcMetadata(BeJobBean.extractEnrichmentControlMetadata(dataBucket, configuration.get(BE_META_BEAN_PARAM)).get());	
		beJobConfigurable.setBatchSize(configuration.getInt(BATCH_SIZE_PARAM,100));	
		beJobConfigurable.setEnrichmentBatchModule((IEnrichmentBatchModule)Class.forName(beSharedLibrary.batch_enrichment_entry_point()).newInstance());
	}

}
