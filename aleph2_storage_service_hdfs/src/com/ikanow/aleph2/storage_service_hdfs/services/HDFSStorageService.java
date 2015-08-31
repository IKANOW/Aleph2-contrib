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

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.TimeUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.storage_service_hdfs.utils.HdfsErrorUtils;

import fj.data.Validation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.collect.ImmutableMap;

import scala.Tuple2;

/** The storage service implementation backed by HDFS (not actually clear if another implementation is possible)
 * @author alex
 */
public class HDFSStorageService implements IStorageService {
	private static final Logger _logger = LogManager.getLogger();	
	
	final protected GlobalPropertiesBean _globals;
	final protected ConcurrentHashMap<Tuple2<String, Optional<String>>, Optional<Object>> _obj_cache = new ConcurrentHashMap<>();
	
	@Inject 
	HDFSStorageService(GlobalPropertiesBean globals) {
		_globals = globals;	
	}
	
	@Override
	public String getRootPath() {		
		return _globals.distributed_root_dir();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(
			Class<T> driver_class, Optional<String> driver_options) {
		
		return (Optional<T>)_obj_cache.computeIfAbsent(Tuples._2T(driver_class.toString(), driver_options), 
				__ -> getUnderlyingPlatformDriver_internal(driver_class, driver_options).map(o -> (T)o)
				);
	}
	
	/** Internal version of getUnderlyingPlatform driver, ie input to cache
	 * @param driver_class
	 * @param driver_options
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <T> Optional<T> getUnderlyingPlatformDriver_internal(
			Class<T> driver_class, Optional<String> driver_options) {
		T driver = null;
		try {
			if(driver_class!=null){
				final Configuration config = getConfiguration();
				URI uri = driver_options.isPresent() ? new URI(driver_options.get()) : getUri(config);
				
				if (driver_class.isAssignableFrom(AbstractFileSystem.class)) {
	
					AbstractFileSystem fs = AbstractFileSystem.createFileSystem(uri, config);
					
					return (Optional<T>) Optional.of(fs);
				}			
				else if(driver_class.isAssignableFrom(FileContext.class)){				
					FileContext fs = FileContext.getFileContext(AbstractFileSystem.createFileSystem(uri, config), config);
					return (Optional<T>) Optional.of(fs);
				}
				else if(driver_class.isAssignableFrom(RawLocalFileSystem.class)){
					return Optional.of(driver_class.newInstance());
				}
			} // !=null
		} 
		catch (Exception e) {
			_logger.error("Caught Exception:",e);
		}
		return Optional.ofNullable(driver);
	}

	/** 
	 * Override this function with system specific configuration
	 * @return
	 */
	protected Configuration getConfiguration(){
		Configuration config = new Configuration(false);
		
		if (new File(_globals.local_yarn_config_dir()).exists()) {
			config.addResource(new Path(_globals.local_yarn_config_dir() +"/yarn-site.xml"));
			config.addResource(new Path(_globals.local_yarn_config_dir() +"/core-site.xml"));
			config.addResource(new Path(_globals.local_yarn_config_dir() +"/hdfs-site.xml"));
		}
		else {
			config.addResource("default_fs.xml");						
		}
		// These are not added by Hortonworks, so add them manually
		config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");									
		config.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
		
		return config;
		
	}

	protected URI getUri(Configuration configuration){
		URI uri = null;
		try {			
			String uriStr = configuration.get("fs.default.name", configuration.get("fs.defaultFS"));
			if(uriStr==null){
				// default with localhost
				uriStr = "hdfs://localhost:8020";
			}
			uri = new URI(uriStr);
		} catch (URISyntaxException e) {
			_logger.error("Caught Exception:",e);
		}
		return uri;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean)
	 */
	@Override
	public Tuple2<String, List<BasicMessageBean>> validateSchema(final StorageSchemaBean schema, final DataBucketBean bucket) {
		return Tuples._2T("",  Collections.emptyList());
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Arrays.asList(this);
	}

	////////////////////////////////////////////////////////////////////////////////
	
	// GENERIC DATA INTERFACE
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider#getDataService()
	 */
	@Override
	public Optional<IDataServiceProvider.IGenericDataService> getDataService() {
		return _data_service;
	}

	/** Implementation of GenericDataService
	 * @author alex
	 */
	public class HdfsDataService implements IDataServiceProvider.IGenericDataService {

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getWritableDataService(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, java.util.Optional)
		 */
		@Override
		public <O> Optional<IDataWriteService<O>> getWritableDataService(
				Class<O> clazz, DataBucketBean bucket,
				Optional<String> options, Optional<String> secondary_buffer) {
			// TODO (ALEPH-??): Support this, the basic idea is - create a uuid based on process + thread id
			// (in a path possibly based on the timestamp)
			// append all data into that (add write settings to generic settings and implement - batch size would be the max file size before segmenting etc)
			return Optional.empty();
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getReadableCrudService(java.lang.Class, java.util.Collection, java.util.Optional)
		 */
		@Override
		public <O> Optional<ICrudService<O>> getReadableCrudService(
				Class<O> clazz, Collection<DataBucketBean> buckets,
				Optional<String> options) {
			// Not supported by HDFS
			return Optional.empty();
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#getSecondaryBufferList(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
		 */
		@Override
		public Collection<String> getSecondaryBufferList(DataBucketBean bucket) {
			// TODO (#28): support secondary buffers
			return Collections.emptyList();
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#switchCrudServiceToPrimaryBuffer(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
		 */
		@Override
		public CompletableFuture<BasicMessageBean> switchCrudServiceToPrimaryBuffer(
				DataBucketBean bucket, Optional<String> secondary_buffer) {
			// TODO (#28): support secondary buffers
			return CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage("HdfsDataService", "switchCrudServiceToPrimaryBuffer", ErrorUtils.NOT_YET_IMPLEMENTED, "switchCrudServiceToPrimaryBuffer"));
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#handleAgeOutRequest(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
		 */
		@Override
		public CompletableFuture<BasicMessageBean> handleAgeOutRequest(final DataBucketBean bucket) {
			
			//TODO (ALEPH-40) make these 3 calls async
			
			final Optional<BasicMessageBean> raw_result =
					Optionals.of(() -> bucket.data_schema().storage_schema().raw_exist_age_max())
					.map(bound -> handleAgeOutRequest_Worker("Raw", bucket.full_name() + IStorageService.STORED_DATA_SUFFIX_RAW, bound));
			
			final Optional<BasicMessageBean> json_result =
					Optionals.of(() -> bucket.data_schema().storage_schema().json_exist_age_max())
					.map(bound -> handleAgeOutRequest_Worker("Json", bucket.full_name() + IStorageService.STORED_DATA_SUFFIX_JSON, bound));
			
			final Optional<BasicMessageBean> processed_result =
					Optionals.of(() -> bucket.data_schema().storage_schema().processed_exist_age_max())
					.map(bound -> handleAgeOutRequest_Worker("Processed", bucket.full_name() + IStorageService.STORED_DATA_SUFFIX_PROCESSED, bound));
			
			return Arrays.asList(raw_result, json_result, processed_result)
					.stream()
					.filter(o -> o.isPresent())
					.map(o -> o.get())
					.reduce(Optional.<BasicMessageBean>empty()
						,
						(acc, v) -> {
							return acc
								.map(accv -> {
									return BeanTemplateUtils.clone(accv)
												.with(BasicMessageBean::success, accv.success() && v.success())
												.with(BasicMessageBean::message,
														accv.message() + "\n" + v.message())
												.with(BasicMessageBean::details,
														null == accv.details()
														? v.details()
														: accv.details() // (slightly hacky but only create details if we're adding loggable)
														)												
											.done();
								})
								.map(Optional::of)
								.orElse(Optional.of(v));
						},
						(acc1, acc2) -> acc1 // (can't happen since not parallel)
					)
					.map(CompletableFuture::completedFuture)
					.orElseGet(() -> {
						return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(
								HDFSStorageService.class.getSimpleName(), 
								"handleAgeOutRequest", HdfsErrorUtils.NO_AGE_OUT_SETTINGS));									
					})
					;
		}

		/** Actual processing to perform to age out check
		 * @param path - the path (including raw/json/processed)
		 * @param lower_bound - the user specified maximum age
		 * @return if age out specified then the status of the age out
		 */
		protected BasicMessageBean handleAgeOutRequest_Worker(final String name, final String path, final String lower_bound) {
			
			// 0) Convert the bound into a time

			final Optional<Long> deletion_bound = Optional.of(lower_bound)
					.map(s -> TimeUtils.getDuration(s, Optional.of(new Date())))
					.filter(Validation::isSuccess)
					.map(v -> v.success())
					.map(duration -> 1000L*duration.getSeconds())
				;
		
			if (!deletion_bound.isPresent()) {
				return ErrorUtils.buildErrorMessage(HDFSStorageService.class.getSimpleName(), 
						"handleAgeOutRequest", HdfsErrorUtils.AGE_OUT_SETTING_NOT_PARSED, name, lower_bound.toString());
			}
						
			// 1) Get all the sub-directories of Path

			final FileContext dfs = getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
			final String bucket_root = getRootPath() + "/" + path;
			try {			
				final Path p = new Path(bucket_root);
				final RemoteIterator<LocatedFileStatus> it = dfs.util().listFiles(p, false);
				final AtomicInteger n_deleted = new AtomicInteger(0);
				while (it.hasNext()) {
					final LocatedFileStatus file = it.next();
					final String dir = file.getPath().getName();
					
					// 2) Any of them that are "date-like" get checked
					
					final Validation<String, Date> v = TimeUtils.getDateFromSuffix(dir);
					v.filter(d -> d.getTime() <= deletion_bound.get())
						.forEach(Lambdas.wrap_consumer_u(s -> {
							n_deleted.incrementAndGet();
							dfs.delete(p, true);
						}));					
				}
				final BasicMessageBean message = ErrorUtils.buildSuccessMessage(HDFSStorageService.class.getSimpleName(), 
						"handleAgeOutRequest",
						"{0}: deleted {1} directories", name, n_deleted.get());
				
				return (n_deleted.get() > 0)
						? BeanTemplateUtils.clone(message).with(BasicMessageBean::details, 
								ImmutableMap.builder().put("loggable", true).build()
								).done()
						: message
						;
			}
			catch (Exception e) {
				return ErrorUtils.buildErrorMessage(HDFSStorageService.class.getSimpleName(), 
						"handleAgeOutRequest", ErrorUtils.getLongForm("{1} error = {0}", e, name));
			}			
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService#handleBucketDeletionRequest(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, boolean)
		 */
		@Override
		public CompletableFuture<BasicMessageBean> handleBucketDeletionRequest(
				DataBucketBean bucket, Optional<String> secondary_buffer,
				boolean bucket_getting_deleted) {
			
			if (secondary_buffer.isPresent()) {
				return CompletableFuture.completedFuture(ErrorUtils.buildErrorMessage("HdfsDataService", "handleBucketDeletionRequest", ErrorUtils.NOT_YET_IMPLEMENTED, "secondary_buffer != Optional.empty()"));				
			}
			
			final FileContext dfs = getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
			final String bucket_root = getRootPath() + "/" + bucket.full_name();
			try {			
				final Path p = new Path(bucket_root + IStorageService.STORED_DATA_SUFFIX);
				
				if (doesPathExist(dfs, p)) {			
					dfs.delete(p, true);			
					dfs.mkdir(p, FsPermission.getDefault(), true);
					
					return CompletableFuture.completedFuture(
							ErrorUtils.buildSuccessMessage("HdfsDataService", "handleBucketDeletionRequest", ErrorUtils.get("Deleted data from bucket = {0}", bucket.full_name()))
							);
				}
				else {
					return CompletableFuture.completedFuture(
							ErrorUtils.buildSuccessMessage("HdfsDataService", "handleBucketDeletionRequest", ErrorUtils.get("(No storage for bucket {0})", bucket.full_name()))
							);
				}
			}
			catch (Throwable t) {			
				return CompletableFuture.completedFuture(
						ErrorUtils.buildErrorMessage("HdfsDataService", "handleBucketDeletionRequest", ErrorUtils.getLongForm("Error deleting data from bucket = {1} : {0}", t, bucket.full_name()))
						);
			}
		}
	}
	protected Optional<IDataServiceProvider.IGenericDataService> _data_service = Optional.of(new HdfsDataService());
	
	/////////////////////////////////////////////////////////////////////////////////
	
	// UTILITY
	
	/** Utility function for checking existence of a path
	 * @param dfs
	 * @param path
	 * @param storage_service
	 * @return
	 * @throws Exception
	 */
	public static boolean doesPathExist(final FileContext dfs, final Path path) throws Exception {
		try {
			dfs.getFileStatus(path);
			return true;
		}
		catch (FileNotFoundException fe) {
			return false;
		} 		
	}	
		
}
