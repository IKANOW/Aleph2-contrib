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
package com.ikanow.aleph2.storage_service_hdfs.services;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.http.impl.cookie.DateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.TimeUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.UuidUtils;
import com.ikanow.aleph2.storage_service_hdfs.utils.HdfsErrorUtils;

/** Generic service for writing data out to HDFS
 * @author Alex
 */
public class HfdsDataWriteService<T> implements IDataWriteService<T> {
	protected static final Logger _logger = LogManager.getLogger();	

	/////////////////////////////////////////////////////////////
	
	// TOP LEVEL SERVICE
	
	protected final DataBucketBean _bucket;
	protected final IStorageService.StorageStage _stage;
	protected final FileContext _dfs;
	protected final IStorageService _storage_service;
	
	// (currently just share on of these across all users of this service, basically across the process/classloader)
	protected final SetOnce<BatchHdfsWriteService> _writer = new SetOnce<>();
	
	protected final static String _process_id = UuidUtils.get().getRandomUuid().substring(14);
	
	/** User constructor
	 * @param bucket
	 */
	public HfdsDataWriteService(final DataBucketBean bucket, final IStorageService.StorageStage stage, final IStorageService storage_service) {
		_bucket = bucket;
		_stage = stage;
		_storage_service = storage_service;
		_dfs = storage_service.getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
	}
	
	/** Lazy initialization for batch writer
	 */
	public void setup() {
		if (!_writer.isSet()) {
			_writer.trySet(new BatchHdfsWriteService());
		}		
	}	
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#storeObject(java.lang.Object)
	 */
	@Override
	public CompletableFuture<Supplier<Object>> storeObject(T new_object) {
		setup();
		_writer.get().storeObject(new_object);
		return CompletableFuture.completedFuture(() -> { return null; });
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#storeObjects(java.util.List)
	 */
	@Override
	public CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<T> new_objects) {
		setup();
		_writer.get().storeObjects(new_objects);
		return CompletableFuture.completedFuture(Tuples._2T(() -> Collections.emptyList(), () -> (long)new_objects.size()));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#countObjects()
	 */
	@Override
	public CompletableFuture<Long> countObjects() {
		// (return a future exception)
		return FutureUtils.returnError(new RuntimeException(ErrorUtils.get(HdfsErrorUtils.OPERATION_NOT_SUPPORTED, "countObjects")));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#deleteDatastore()
	 */
	@Override
	public CompletableFuture<Boolean> deleteDatastore() {
		throw new RuntimeException(ErrorUtils.get(HdfsErrorUtils.OPERATION_NOT_SUPPORTED, "deleteDatastore"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getCrudService()
	 */
	@Override
	public Optional<ICrudService<T>> getCrudService() {
		throw new RuntimeException(ErrorUtils.get(HdfsErrorUtils.OPERATION_NOT_SUPPORTED, "getCrudService"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getRawService()
	 */
	@Override
	public IDataWriteService<JsonNode> getRawService() {
		return new HfdsDataWriteService<JsonNode>(_bucket, _stage, _storage_service);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <X> Optional<X> getUnderlyingPlatformDriver(Class<X> driver_class,
			Optional<String> driver_options) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService#getBatchWriteSubservice()
	 */
	@Override
	public Optional<IBatchSubservice<T>> getBatchWriteSubservice() {
		setup();
		return Optional.of(_writer.get());
	}

	/////////////////////////////////////////////////////////////
	
	/** BATCH SUB SERVICE
	 * @author alex
	 */
	public class BatchHdfsWriteService implements IBatchSubservice<T> {
		final protected LinkedBlockingQueue<Object> _shared_queue = new LinkedBlockingQueue<>();
		public class MutableState {
			int max_objects = 5000; // (5K objects)
			long size_kb = 20L*1024L; // (20MB)
			Duration flush_interval = Duration.ofMinutes(1L); // (1 minute)
			int write_threads = 2;
			ThreadPoolExecutor _workers = null;
		}
		final protected MutableState _state = new MutableState();
		
		/** User constructor
		 */
		public BatchHdfsWriteService() {
			// Launch the executor service
			fillUpEmptyQueue();
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService.IBatchSubservice#setBatchProperties(java.util.Optional, java.util.Optional, java.util.Optional, java.util.Optional)
		 */
		@Override
		public void setBatchProperties(Optional<Integer> max_objects,
				Optional<Long> size_kb, Optional<Duration> flush_interval,
				Optional<Integer> write_threads) {
			synchronized (this) {
				_state.max_objects = max_objects.orElse(_state.max_objects);
				_state.size_kb = size_kb.orElse(_state.size_kb);
				_state.flush_interval = flush_interval.orElse(_state.flush_interval);
				// (write threads change ignore for now)
				
				int old_write_threads = _state.write_threads;
				_state.write_threads = write_threads.orElse(_state.write_threads);
				if (old_write_threads < _state.write_threads) { // easy case, just expand
					_state._workers.setCorePoolSize(_state.write_threads);
					_state._workers.setMaximumPoolSize(_state.write_threads);
					for (int i = old_write_threads; i < _state.write_threads; ++i) {
						_state._workers.execute(new WriterWorker());
					}								
				}
				else if (old_write_threads > _state.write_threads) { // this is a bit ugly, nuke the existing worker queue
					_state._workers.shutdownNow();
					fillUpEmptyQueue();
				}

			}
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService.IBatchSubservice#storeObjects(java.util.List)
		 */
		@Override
		public void storeObjects(List<T> new_objects) {
			_shared_queue.add(new_objects);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService.IBatchSubservice#storeObject(java.lang.Object)
		 */
		@Override
		public void storeObject(T new_object) {
			_shared_queue.add(new_object);
		}

		////////////////////////////////////////
		
		// UTILITY
		
		/** Fills up queue
		 */
		private void fillUpEmptyQueue() {
			_state._workers = new ThreadPoolExecutor(_state.write_threads, _state.write_threads, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
			
			for (int i = 0; i < _state.write_threads; ++i) {
				_state._workers.execute(new WriterWorker());
			}			
		}
	}

	/////////////////////////////////////////////////////////////
	
	// BATCH SUB SERVICE - WORKER THREAD
	
	/** A worker thread
	 * @author alex
	 */
	public class WriterWorker implements Runnable {
		
		protected static final String SPOOL_DIR = "/.spooldir/";
		
		public class MutableState {
			boolean terminate = false;
			Optional<String> codec = Optional.empty();
			int segment = 1;
			int curr_objects;
			long curr_size_b;
			Path curr_path;
			OutputStream out;
		}
		final protected MutableState _state = new MutableState();
		
		final protected String _thread_id = UuidUtils.get().getRandomUuid().substring(14);
		
		/* (non-Javadoc)
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			_logger.info("Starting HDFS worker thread: " + getFilename());
			
			Runtime.getRuntime().addShutdownHook(new Thread(Lambdas.wrap_runnable_i(() -> {
				_state.terminate = true;
				complete_segment();
			})));
			
			// (Some internal mutable state)
			boolean more_objects = false;
			int max_objects = 5000; // (5K objects)
			long size_kb = 20L*1024L; // (20MB)
			Duration flush_interval = Duration.ofMinutes(1L); // (1 minute)
			long timeout = flush_interval.get(ChronoUnit.NANOS);
			
			try {
				for (; !_state.terminate && !Thread.interrupted();) {
					if (!more_objects) {
						synchronized (_writer) {
							max_objects = _writer.get()._state.max_objects;
							size_kb = _writer.get()._state.size_kb;
							flush_interval = _writer.get()._state.flush_interval;
							timeout = flush_interval.get(ChronoUnit.NANOS);
						}
					}
					Object o = _writer.get()._shared_queue.poll(timeout, TimeUnit.NANOSECONDS);
					if (null == o) {
						complete_segment();
						more_objects = false;
						continue;
					}
					if (null == _state.out) {
						new_segment();
					}
					write(o);
					if ((_state.curr_objects > max_objects)
							||
						((1024L*_state.curr_size_b) > size_kb))
					{
						complete_segment();
						more_objects = false;
					}
					else {
						more_objects = null != _writer.get()._shared_queue.peek();
					}
				}
			}
			catch (Exception e) { // assume this is an interrupted error and fall through to....
			}
			try { // always try to complete current segment before exiting
				complete_segment();
			}
			catch (Exception ee) {}
			
			_logger.info("Terminating HDFS worker thread: " + getFilename());			
		}
		/** Write the object(s) out to the stream
		 * @param o
		 * @return
		 * @throws IOException 
		 */
		protected void write(final Object o) throws IOException {
			String s = null;
			if (o instanceof List) {
				@SuppressWarnings({ "rawtypes", "unchecked" })
				List<Object> l = (List)o;
				l.stream().forEach(Lambdas.wrap_consumer_u(ol -> write(ol)));
				return;
			}
			else if (o instanceof String) {
				s = ((String) o);
				if (!s.endsWith("\n")) s += "\n"; //(i think it will a fair bit)
			}
			else if (o instanceof JsonNode) {
				s = ((JsonNode) o).toString() + "\n";
			}
			else {
				s = BeanTemplateUtils.toJson(o).toString() + "\n";
			}
			_state.out.write(s.getBytes());
			_state.curr_objects++;
			_state.curr_size_b += s.getBytes().length;
		}
		
		/** Create a new segment
		 * @throws Exception
		 */
		protected void new_segment() throws Exception {
			if (null == _state.out) { // (otherwise we already have a segment) 
				_state.codec = getCanonicalCodec(_bucket.data_schema().storage_schema(), _stage); // (recheck the codec)			
				
				_state.curr_path = new Path(getBasePath(_storage_service.getRootPath(), _bucket, _stage) + "/" + SPOOL_DIR + "/" + getFilename());
				try { _dfs.mkdir(_state.curr_path.getParent(), FsPermission.getDefault(), true); } catch (Exception e) {}
				
				_state.out = wrapOutputInCodec(_state.codec, _dfs.create(_state.curr_path, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)));
			}
		}
		
		/** Completes an existing segment
		 * @throws IOException 
		 */
		protected synchronized void complete_segment() throws IOException {
			if ((null != _state.out) && (_state.curr_objects > 0)) {
				_state.out.close();
				_state.out = null;
				_state.segment++;
				
				final Date now = new Date();
				final Path path =  new Path(getBasePath(_storage_service.getRootPath(), _bucket, _stage) + "/" + getSuffix(now, _bucket, _stage) + "/" + _state.curr_path.getName());
				try { _dfs.mkdir(path.getParent(), FsPermission.getDefault(), true); } catch (Exception e) {} // (fails if already exists?)
				_dfs.rename(_state.curr_path, path);				
			}
		}
		/** Returns the filename corresponding to this object
		 * @return
		 */
		protected String getFilename() {
			final String suffix = getExtension(_stage) + _state.codec.map(s -> "." + s).orElse("");
			return _process_id + "_" + _thread_id + "_" + _state.segment + suffix;
		}
		
	}
	
	//////////////////////////////////////////////////////////////////////
	
	// UTILITIES
	
	/** Returns the codec string (normalized, eg "gzip" -> "gz")
	 * @param storage_schema
	 * @param stage
	 * @return
	 */
	public static Optional<String> getCanonicalCodec(final DataSchemaBean.StorageSchemaBean storage_schema, final IStorageService.StorageStage stage) {
		return Optional.ofNullable(getStorageSubSchema(storage_schema, stage))
						.map(ss -> ss.codec())
						.map(codec -> {
							if (codec.equalsIgnoreCase("gzip")) {
								return "gz";
							}
							if (codec.equalsIgnoreCase("snappy")) {
								return "sz";
							}
							if (codec.equalsIgnoreCase("snappy_framed")) {
								return "fr.sz";
							}
							else return codec;
						})
						.map(String::toLowerCase)
						;
	}
	
	/** Wraps an output stream in one of the supported codecs
	 * @param codec
	 * @param original_output
	 * @return
	 */
	public static OutputStream wrapOutputInCodec(final Optional<String> codec, final OutputStream original_output) {
		return codec.map(Lambdas.wrap_u(c -> {
					if (c.equals("gz")) {
						return new java.util.zip.GZIPOutputStream(original_output);
					}
					else if (c.equals("sz")) {
						return new org.xerial.snappy.SnappyOutputStream(original_output);
					}
					else if (c.equals("fr.sz")) {
						return new org.xerial.snappy.SnappyFramedOutputStream(original_output);
					}
					else return null; // (fallback to no codec)
					
				}))
				.orElse(original_output);
	}
	
	/** V simple utility - if we know it's JSON then use that otherwise use nothing
	 * @param stage
	 * @return
	 */
	public static String getExtension(final IStorageService.StorageStage stage) {
		if (IStorageService.StorageStage.raw == stage) {
			return "";
		}
		else {
			return ".json";				
		}
	}
	
	/** Utility function to map the storage type/bucket to a base directory 
	 * @param bucket
	 * @param stage
	 * @return
	 */
	public static String getBasePath(final String root_path, final DataBucketBean bucket, final IStorageService.StorageStage stage) {
		return Optional.of(Patterns.match().<String>andReturn()
								.when(__ -> stage == IStorageService.StorageStage.raw, __ -> IStorageService.STORED_DATA_SUFFIX_RAW)
								.when(__ -> stage == IStorageService.StorageStage.json, __ -> IStorageService.STORED_DATA_SUFFIX_JSON)
								.when(__ -> stage == IStorageService.StorageStage.processed, __ -> IStorageService.STORED_DATA_SUFFIX_PROCESSED)
								.otherwiseAssert()
							)
						.map(s -> root_path + bucket.full_name() + s)
						.get();
	}
	
	/** Gets the time based suffix, or IStorageService.NO_TIME_SUFFIX if it's not temporal
	 * @param bucket
	 * @param stage
	 * @return
	 */
	public static String getSuffix(final Date now, final DataBucketBean bucket, final IStorageService.StorageStage stage) {
		return Optionals.of(() -> bucket.data_schema().storage_schema())
				.map(store -> getStorageSubSchema(store, stage))
				.map(ss -> ss.grouping_time_period())
				.<String>map(period -> TimeUtils.getTimePeriod(period)
								.map(d -> TimeUtils.getTimeBasedSuffix(d, Optional.empty()))
								.validation(fail -> null, success -> DateUtils.formatDate(now, success))
						)
				.orElse(IStorageService.NO_TIME_SUFFIX)
				;
	}
	
	/** Super low level utility to pick out the right storage sub-schema
	 * @param store
	 * @param stage
	 * @return
	 */
	public static StorageSchemaBean.StorageSubSchemaBean getStorageSubSchema(final StorageSchemaBean store, final IStorageService.StorageStage stage) {
		return Patterns.match().<StorageSchemaBean.StorageSubSchemaBean>andReturn()
						.when(__ -> stage == IStorageService.StorageStage.raw, __ -> store.raw())
						.when(__ -> stage == IStorageService.StorageStage.json, __ -> store.json())
						.when(__ -> stage == IStorageService.StorageStage.processed, __ -> store.processed())
						.otherwiseAssert();		
	}

}
