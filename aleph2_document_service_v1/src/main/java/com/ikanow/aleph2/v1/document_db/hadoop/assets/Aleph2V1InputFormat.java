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
package com.ikanow.aleph2.v1.document_db.hadoop.assets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BSONObject;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.infinit.e.data_model.custom.InfiniteMongoInputFormat;

/** Extends the old v1 code and places a v2 facade around it
 * @author Alex
 */
public class Aleph2V1InputFormat extends InfiniteMongoInputFormat {

	/** Simple implementation of IBatchRecord
	 * @author jfreydank
	 */
	public static class BatchRecord implements IBatchRecord {
		public BatchRecord(final JsonNode json, final ByteArrayOutputStream content) {
			_json = json;
			_content = content;
		}		
		public JsonNode getJson() { return _json; }
		public Optional<ByteArrayOutputStream> getContent() { return Optional.ofNullable(_content); }		
		protected final JsonNode _json; protected final ByteArrayOutputStream _content;
	}	
		
	/* (non-Javadoc)
	 * @see org.elasticsearch.hadoop.mr.EsInputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
	 */
	@Override
	public List<InputSplit> getSplits(JobContext context) {
		return super.getSplits(context);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.infinit.e.data_model.custom.InfiniteMongoInputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
		return new V1DocumentDbRecordReader(super.createRecordReader(split, context));
	}
	
	/** Wraps the mongodb object in a JsonNode
	 * @author Alex
	 */
	public static class V1DocumentDbRecordReader extends RecordReader<String, Tuple2<Long, IBatchRecord>> {
		final protected RecordReader<Object, BSONObject> _delegate;
		
		/** User c'tor
		 * @param delegate
		 */
		V1DocumentDbRecordReader(final RecordReader<Object, BSONObject> delegate) {
			_delegate = delegate;
		}
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			_delegate.initialize(split, context);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return _delegate.nextKeyValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return _delegate.getProgress();
		}

		@Override
		public void close() throws IOException {
			_delegate.close();
		}

		@Override
		public String getCurrentKey() throws IOException, InterruptedException {
			return _delegate.getCurrentKey().toString();
		}

		@Override
		public Tuple2<Long, IBatchRecord> getCurrentValue() throws IOException,
				InterruptedException {
			// This is the only trivial bit of code here
			
			// TODO Auto-generated method stub
			return null;
		}
		
	}
}
