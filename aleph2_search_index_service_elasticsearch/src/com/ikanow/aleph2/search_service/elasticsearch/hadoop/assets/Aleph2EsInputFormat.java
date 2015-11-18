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
package com.ikanow.aleph2.search_service.elasticsearch.hadoop.assets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;



import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
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
	@SuppressWarnings("unchecked")
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		return getSplits(Lambdas.wrap_u(ctxt -> (List<InputSplit>)super.getSplits(ctxt)), context);
	}
	
	public static List<InputSplit> getSplits(Function<JobContext, List<InputSplit>> get_splits, JobContext context) {
		final String[] indexes = context.getConfiguration().get(ALEPH2_RESOURCE, "").split(",,");
		
		return Arrays.stream(indexes).<InputSplit>flatMap(Lambdas.wrap_u(index -> {
			context.getConfiguration().set("es.resource.read", index.replace(" ", "%20"));
			return get_splits.apply(context).stream();
		}))
		.collect(Collectors.<InputSplit>toList())
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
	public static class Aleph2EsRecordReader extends ShardRecordReader<String, Tuple2<Long, IBatchRecord>> {
		protected final RecordReader _delegate;
		
		/** User c'tor
		 * @param delegate
		 */
		Aleph2EsRecordReader(RecordReader delegate) {
			_delegate = delegate;
		}
		
		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
		 */
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException {
			Lambdas.wrap_runnable_u(() -> _delegate.initialize(split, context)).run();
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#nextKeyValue()
		 */
		@Override
		public boolean nextKeyValue() throws IOException {
			return Lambdas.wrap_u(() -> _delegate.nextKeyValue()).get();
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#getCurrentKey()
		 */
		@Override
		public String getCurrentKey() throws IOException {
			return Lambdas.wrap_u(() -> {
				final Text t = (Text) _delegate.getCurrentKey();
				return Optional.ofNullable(t).map(tt -> tt.toString()).orElse(null); 
			}).get();
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#getCurrentValue()
		 */
		@Override
		public Tuple2<Long, IBatchRecord> getCurrentValue() {
			return Lambdas.wrap_u(() -> {
				final MapWritable m = (MapWritable) _delegate.getCurrentValue();
				return Tuples._2T(0L, (IBatchRecord)new BatchRecord(JsonNodeWritableUtils.from(m), null));
			}).get();
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#getProgress()
		 */
		@Override
		public float getProgress() {
			return Lambdas.wrap_u(() -> _delegate.getProgress()).get();
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
			throw new RuntimeException("Internal logic error (createKey)");
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#createValue()
		 */
		@Override
		public Tuple2<Long, IBatchRecord> createValue() {
			throw new RuntimeException("Internal logic error (createValue)");
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#setCurrentKey(java.lang.Object, java.lang.Object)
		 */
		@Override
		protected String setCurrentKey(String hadoopKey, Object object) {
			throw new RuntimeException("Internal logic error (setCurrentKey)");
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#setCurrentValue(java.lang.Object, java.lang.Object)
		 */
		@Override
		protected Tuple2<Long, IBatchRecord> setCurrentValue(
				Tuple2<Long, IBatchRecord> hadoopValue, Object object) {
			throw new RuntimeException("Internal logic error (setCurrentValue)");
		}		
	}
}
