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
******************************************************************************/
package com.ikanow.aleph2.analytics.hadoop.assets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

/** Output Writer specific to batch enrichment
 *  (Note it's actually in here that the calls to the batch enrichment module implementation live)
 * @author jfreydank
 */
public class BeFileOutputWriter extends RecordWriter<String, Tuple2<Long, IBatchRecord>>{

	static final Logger _logger = LogManager.getLogger(BeFileOutputWriter.class); 
	List<Tuple2<Long, IBatchRecord>> batch = new ArrayList<Tuple2<Long, IBatchRecord>>();

	Configuration _configuration = null;
	IEnrichmentModuleContext _enrichmentContext = null;
	DataBucketBean _dataBucket = null;
	SharedLibraryBean _beSharedLibrary = null;
	EnrichmentControlMetadataBean _ecMetadata = null;
	private int _batchSize = 100;
	private IEnrichmentBatchModule _enrichmentBatchModule = null;
	
	/** User c'tor
	 * @param configuration
	 * @param enrichmentContext
	 * @param enrichmentBatchModule
	 * @param dataBucket
	 * @param beSharedLibrary
	 * @param ecMetadata
	 */
	public BeFileOutputWriter(Configuration configuration, IEnrichmentModuleContext enrichmentContext,IEnrichmentBatchModule enrichmentBatchModule,DataBucketBean dataBucket,
			SharedLibraryBean beSharedLibrary, EnrichmentControlMetadataBean ecMetadata) {
		super();
		this._configuration = configuration;
		this._enrichmentContext =  enrichmentContext;
		this._enrichmentBatchModule = enrichmentBatchModule;
		this._dataBucket = dataBucket;
		this._beSharedLibrary = beSharedLibrary;
		this._ecMetadata = ecMetadata;
		
		// TODO (ALEPH-12) check where final_stage is defined
		boolean final_stage = true;
		enrichmentBatchModule.onStageInitialize(enrichmentContext, dataBucket, final_stage);

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void write(String key, Tuple2<Long, IBatchRecord> value) throws IOException, InterruptedException {
				batch.add(value);
		checkBatch(false);
	}

	/** Checks if we should send a batch of objects to the next stage in the pipeline
	 * @param flush
	 */
	protected void checkBatch(boolean flush){
		if((batch.size()>=_batchSize) || flush){
			_enrichmentBatchModule.onObjectBatch(batch.stream(), Optional.empty(), Optional.empty());
			batch.clear();
		}		
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		checkBatch(true);
		_enrichmentBatchModule.onStageComplete(true);		
	}

}
