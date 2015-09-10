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
 ******************************************************************************/
package com.ikanow.aleph2.analytics.hadoop.assets;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/** A more generic multiple inputs than the one provided by Hadoop
 * @author Alex
 */
@SuppressWarnings("rawtypes") 
public class Aleph2MultipleInputFormatBuilder {

	// (Check out http://grepcode.com/file/repo1.maven.org/maven2/com.ning/metrics.serialization-all/2.0.0-pre1/org/apache/hadoop/mapreduce/lib/input/MultipleInputs.java)
	
	public static final String ALEPH2_MULTI_INPUT_FORMAT_PREFIX = "aleph2.input.multi."; // (then the overwrite values

	/** User c'tor, just for setting up the job via addInput
	 */
	public Aleph2MultipleInputFormatBuilder() 	{}
	
	/** Add a another path to this input format
	 * @param unique_name
	 * @param input_format_clazz
	 * @param extra_config
	 * @param path
	 */
	public void addInput(final String unique_name, 
			final Job job,
			final Class<? extends InputFormat> input_format_clazz, 
			final Map<String, Object> extra_config, 
			final Optional<List<Path>> paths)
	{
		//TODO (ALEPH-12)
		job.setInputFormatClass(Aleph2MultipleInputFormat.class);
	}

	public static class Aleph2MultipleInputFormat<K, V> extends InputFormat<K, V> {
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
		 */
		@Override
		public RecordReader<K, V> createRecordReader(InputSplit arg0,
				TaskAttemptContext arg1) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
		 */
		@Override
		public List<InputSplit> getSplits(JobContext arg0) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}
	}	
}
