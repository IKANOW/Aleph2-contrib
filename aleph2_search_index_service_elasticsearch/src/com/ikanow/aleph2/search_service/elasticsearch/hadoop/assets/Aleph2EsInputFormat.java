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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;










import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.elasticsearch.hadoop.mr.EsInputFormat;








import org.elasticsearch.hadoop.mr.HadoopCfgUtils;

import scala.Tuple2;










import com.ikanow.aleph2.core.shared.utils.BatchRecordUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.utils.JsonUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.search_service.elasticsearch.utils.JsonNodeWritableUtils;
import com.ikanow.aleph2.shared.crud.elasticsearch.utils.ElasticsearchUtils;

/** Aleph2 overlay that converts Text/MapWritable into the format required by BatchEnrichmentModule
 * Removes the generics parameters because we're going to transform them en-route, and this makes it easier
 * @author Alex
 */
@SuppressWarnings("rawtypes")
public class Aleph2EsInputFormat extends EsInputFormat { //<Text, MapWritable>
	/** ,,-separated list of <,-separated-indexes>/<,-separated-types>
	 */
	public static final String ALEPH2_RESOURCE = "aleph2.es.resource";	
	
	/** The temp field name for the metadata before it's copied into the main object
	 */
	public static final String ALEPH2_META_FIELD = "__a2_esm";
	
	/** Copy of batch enrichment config param for hadoop, annoyingly has to be duplicated based on github docs
	 *  (Note that is can be assigned independently using input_bean.config().record_limit_request())
	 */
	public static final String BE_DEBUG_MAX_SIZE = "aleph2.batch.debugMaxSize";		
	
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
		
		protected long _max_records_per_split = Long.MAX_VALUE;
		protected long _mutable_curr_records = 0L;
		
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

			// Test case:
			_max_records_per_split = Optional.ofNullable(context.getConfiguration().get(BE_DEBUG_MAX_SIZE))
										.map(str -> Long.parseLong(str))
										.orElse(Long.MAX_VALUE);

			// Spark has some issues with oddly set config it would appear?
			{
				final Configuration config = context.getConfiguration();
				try {					
					HadoopCfgUtils.getTaskTimeout(config);
				}
				catch (Exception e) {
					//TRACE: this happened intermittently then stopped
					System.out.println("ODD CONFIG ISSUE IN SPARK:");
			        for (java.util.Map.Entry<String, String> entry : config) {
			            System.out.println("CFG: " + entry.getKey() + " = " + entry.getValue());
			        }
					context.getConfiguration().set("mapreduce.task.timeout", "300000");					
				}
			}
			
			Lambdas.wrap_runnable_u(() -> _delegate.initialize(split, context)).run();
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#nextKeyValue()
		 */
		@Override
		public boolean nextKeyValue() throws IOException {
			return (_mutable_curr_records++ < _max_records_per_split) // (debug version) 
					&& Lambdas.wrap_u(() -> _delegate.nextKeyValue()).get();
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

		protected static final Text _ID = new Text(JsonUtils._ID);
		protected static final Text _INDEX = new Text(ElasticsearchUtils._INDEX);
		protected static final Text _TYPE = new Text(ElasticsearchUtils._TYPE);
		protected static final Text ALEPH2_META_FIELD_TEXT = new Text(ALEPH2_META_FIELD);
		
		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#getCurrentValue()
		 */
		@Override
		public Tuple2<Long, IBatchRecord> getCurrentValue() {
			return Lambdas.wrap_u(() -> {
				final MapWritable m = (MapWritable) _delegate.getCurrentValue();
				// Add the _id, _index, and _type
				m.computeIfAbsent(_ID, Lambdas.wrap_u(k -> (Text) _delegate.getCurrentKey()));
				final Optional<MapWritable> maybe_meta = Optional.ofNullable(m.remove(ALEPH2_META_FIELD_TEXT)).filter(w -> w instanceof MapWritable).map(w -> (MapWritable)w);
				maybe_meta.map(mw -> mw.get(_INDEX)).ifPresent(index -> m.computeIfAbsent(_INDEX, k -> index));
				maybe_meta.map(mw -> mw.get(_TYPE)).ifPresent(index -> m.computeIfAbsent(_TYPE, k -> index));
				
				return Tuples._2T(0L, (IBatchRecord)new BatchRecordUtils.JsonBatchRecord(JsonNodeWritableUtils.from(m)));
			}).get();
		}

		/* (non-Javadoc)
		 * @see org.elasticsearch.hadoop.mr.EsInputFormat.ShardRecordReader#getProgress()
		 */
		@Override
		public float getProgress() {
			return ((_max_records_per_split != Long.MAX_VALUE) && (0 != _max_records_per_split))
					? (float)_mutable_curr_records/(float)_max_records_per_split // (debug version)
					: Lambdas.wrap_u(() -> _delegate.getProgress()).get();
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
