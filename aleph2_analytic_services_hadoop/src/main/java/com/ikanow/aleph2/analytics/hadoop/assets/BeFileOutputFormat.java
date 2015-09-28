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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import scala.Tuple2;

import com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

/** Output Format specific to batch enrichment
 *  (Note it's actually in here that the calls to the batch enrichment module implementation live)
 *  TODO (ALEPH-12): not sure if we actually use any element of the FileOutputFormat here?
 * @author jfreydank
 */
public class BeFileOutputFormat extends OutputFormat<String, Tuple2<Long, IBatchRecord>> implements IBeJobConfigurable{

	private EnrichmentControlMetadataBean _ecMetadata;
	private SharedLibraryBean _beSharedLibrary;
	private DataBucketBean _dataBucket;
	private IEnrichmentModuleContext _enrichmentContext;
	private IEnrichmentBatchModule _enrichmentBatchModule = null;			


	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.lib.output.FileOutputFormat#getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public RecordWriter<String, Tuple2<Long, IBatchRecord>> getRecordWriter(TaskAttemptContext jobContext)
			throws IOException, InterruptedException {
		try {
			BatchEnrichmentJob.extractBeJobParameters(this, jobContext.getConfiguration());
		} catch (Exception e) {
			throw new IOException(e);
		}
		return new BeFileOutputWriter(jobContext.getConfiguration(), _enrichmentContext,_enrichmentBatchModule,_dataBucket,_beSharedLibrary,_ecMetadata);
	}

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
	 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setBatchSize(int)
	 */
	@Override
	public void setBatchSize(int int1) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobConfigurable#setEnrichmentBatchModule(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule)
	 */
	@Override
	public void setEnrichmentBatchModule(IEnrichmentBatchModule enrichmentBatchModule) {
		this._enrichmentBatchModule = enrichmentBatchModule;
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.OutputFormat#checkOutputSpecs(org.apache.hadoop.mapreduce.JobContext)
	 */
	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException,
			InterruptedException {
		// Nothing to do here
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		return new BeOutputCommiter();
	}

	/** Currently empty output committer (currently: always commits, so will reproduce the v1 bug where failed reduces result
	 *  in duplicate data)
	 * @author Alex
	 */
	public class BeOutputCommiter extends OutputCommitter {

	    public void abortTask( TaskAttemptContext taskContext ){
	    }

	    public void cleanupJob( JobContext jobContext ){
	    }

	    public void commitTask( TaskAttemptContext taskContext ){
	    }

	    public boolean needsTaskCommit( TaskAttemptContext taskContext ){
	    	return true;
	    }

	    public void setupJob( JobContext jobContext ){
	    }

	    public void setupTask( TaskAttemptContext taskContext ){
	    }
	}
}
