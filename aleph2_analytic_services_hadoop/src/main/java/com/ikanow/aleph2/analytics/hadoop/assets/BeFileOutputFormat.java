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

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;

/** Output Format specific to batch enrichment
 *  (Note it's actually in here that the calls to the batch enrichment module implementation live)
 *  TODO (ALEPH-12): not sure if we actually use any element of the FileOutputFormat here?
 * @author jfreydank
 */
public class BeFileOutputFormat extends OutputFormat<String, Tuple2<Long, IBatchRecord>> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.lib.output.FileOutputFormat#getRecordWriter(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public RecordWriter<String, Tuple2<Long, IBatchRecord>> getRecordWriter(TaskAttemptContext jobContext)
			throws IOException, InterruptedException {
		return new BeFileOutputWriter();
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
