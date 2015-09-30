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
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;

/** Output Writer specific to batch enrichment
 *  (Doesn't currently do anything, all the outputting occurs via the context)
 * @author jfreydank
 */
public class BeFileOutputWriter extends RecordWriter<String, Tuple2<Long, IBatchRecord>>{
	static final Logger _logger = LogManager.getLogger(BeFileOutputWriter.class); 
	
	/** User c'tor
	 */
	public BeFileOutputWriter() {
		super();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordWriter#write(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void write(String key, Tuple2<Long, IBatchRecord> value) throws IOException, InterruptedException {
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordWriter#close(org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
	}

}
