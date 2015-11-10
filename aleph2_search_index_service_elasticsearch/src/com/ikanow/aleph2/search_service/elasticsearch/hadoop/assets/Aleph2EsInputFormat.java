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
package com.ikanow.aleph2.search_service.elasticsearch.hadoop.assets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.elasticsearch.hadoop.mr.EsInputFormat;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.search_service.elasticsearch.utils.JsonNodeWritableUtils;

/** Aleph2 overlay that converts Text/MapWritable into the format required by BatchEnrichmentModule
 * Removes the generics parameters because we're going to transform them en-route, and this makes it easier
 * @author Alex
 */
@SuppressWarnings("rawtypes")
public class Aleph2EsInputFormat extends EsInputFormat { //<Text, MapWritable>
	/** ,,-separated list of <,-separated-indexes>/<,-separated-types>
	 */
	public static final String ALEPH2_RESOURCE = "aleph2.es.resource";	
	
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
	public List getSplits(JobContext context) throws IOException {
		final String[] indexes = context.getConfiguration().get(ALEPH2_RESOURCE, "").split(",,");
				
		return Arrays.stream(indexes).flatMap(Lambdas.wrap_u(index -> {
			context.getConfiguration().set("es.resource.read", index.replace(" ", "%20"));
			return super.getSplits(context).stream();
		}))
		.collect(Collectors.toList())
		;
	}

	/* (non-Javadoc)
	 * @see org.elasticsearch.hadoop.mr.EsInputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public ShardRecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new Aleph2EsRecordReader(super.createRecordReader(split, context));
    }
	
	/** Record reader that wraps the ES
	 * @author Alex
	 */
	public static class Aleph2EsRecordReader extends  ShardRecordReader<String, Tuple2<Long, IBatchRecord>> {
		protected final ShardRecordReader _delegate;
		
		/** User c'tor
		 * @param delegate
		 */
		Aleph2EsRecordReader(ShardRecordReader delegate) {
			_delegate = delegate;
		}
		
		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
		 */
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException {
			_delegate.initialize(split, context);
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#nextKeyValue()
		 */
		@Override
		public boolean nextKeyValue() throws IOException {
			return _delegate.nextKeyValue();
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#getCurrentKey()
		 */
		@Override
		public String getCurrentKey() throws IOException {
			final Text t = (Text) _delegate.getCurrentKey();
			return null == t 
					? null
					: t.toString();
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#getCurrentValue()
		 */
		@Override
		public Tuple2<Long, IBatchRecord> getCurrentValue() {
			final MapWritable m = (MapWritable) _delegate.getCurrentValue();
			return Tuples._2T(0L, (IBatchRecord)new BatchRecord(JsonNodeWritableUtils.from(m), null));
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#getProgress()
		 */
		@Override
		public float getProgress() {
			return _delegate.getProgress();
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#close()
		 */
		@Override
		public void close() throws IOException {
			_delegate.close();			
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#createKey()
		 */
		@Override
		public String createKey() {
			throw new RuntimeException("Internal logic error");
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#createValue()
		 */
		@Override
		public Tuple2<Long, IBatchRecord> createValue() {
			throw new RuntimeException("Internal logic error");
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#setCurrentKey(java.lang.Object, java.lang.Object)
		 */
		@Override
		protected String setCurrentKey(String hadoopKey, Object object) {
			throw new RuntimeException("Internal logic error");
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#setCurrentValue(java.lang.Object, java.lang.Object)
		 */
		@Override
		protected Tuple2<Long, IBatchRecord> setCurrentValue(
				Tuple2<Long, IBatchRecord> hadoopValue, Object object) {
			throw new RuntimeException("Internal logic error");
		}		
	}
}
