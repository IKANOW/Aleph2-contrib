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
package com.ikanow.aleph2.logging.service;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.logging.data_model.LoggingServiceConfigBean;
import com.ikanow.aleph2.logging.utils.LoggingUtils;
import com.ikanow.aleph2.management_db.services.DataBucketCrudService;

/**
 * Implementation of ILoggingService that reads the management schema of a data bucket and writes out
 * to those locations.
 * @author Burch
 *
 */
public class LoggingService implements ILoggingService {
	
	private final static Logger _logger = LogManager.getLogger();
	protected final static Cache<String, IDataWriteService<JsonNode>> bucket_writable_cache = CacheBuilder.newBuilder().expireAfterAccess(30, TimeUnit.MINUTES).build();
	
	protected final LoggingServiceConfigBean properties;
	protected final ISearchIndexService search_index_service;
	protected final IStorageService storage_service;
	
	@Inject
	public LoggingService(
			final LoggingServiceConfigBean properties, 
			final ISearchIndexService search_index_service,
			final IStorageService storage_service) {
		this.properties = properties;
		this.search_index_service = search_index_service;
		this.storage_service = storage_service;
	}	

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#getLogger(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public CompletableFuture<IBucketLogger> getLogger(DataBucketBean bucket) {
		return getBucketLogger(bucket, getWritable(bucket), false, storage_service);		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#getSystemLogger(java.util.Optional)
	 */
	@Override
	public CompletableFuture<IBucketLogger> getSystemLogger(DataBucketBean bucket) {
		return getBucketLogger(bucket, getWritable(bucket), true, storage_service);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#getExternalLogger(java.lang.String)
	 */
	@Override
	public CompletableFuture<IBucketLogger> getExternalLogger(final String subsystem) {
		final DataBucketBean bucket = LoggingUtils.getExternalBucket(subsystem, Optional.ofNullable(properties.default_system_log_level()).orElse(Level.OFF));		
		return getBucketLogger(bucket, getWritable(bucket), true, storage_service);
	}
	
	/**
	 * Retrieves a writable for the given bucket, trys to find it in the cache first, creates a new one if it can't and adds it to the cache for future requests.
	 * 
	 * @param log_bucket
	 * @return
	 * @throws ExecutionException 
	 */
	private IDataWriteService<JsonNode> getWritable(final DataBucketBean log_bucket) {
		try {
			return bucket_writable_cache.get(getWritableCacheKey(log_bucket), () -> {
				return LoggingUtils.getLoggingServiceForBucket(search_index_service, storage_service, log_bucket); //TODO will I need temporal/columnar services also
			});
		} catch (ExecutionException e) {
			_logger.error("Error getting writable for bucket: " + log_bucket.full_name() + " return an empty logger instead that ignores requests", e);
			return new LoggingUtils.EmptyWritable<JsonNode>();
		}
	}	

	/**
	 * Creates the bucket logger for the given bucket.  First attempts to write the
	 * output path, if that fails returns an exceptioned completable.
	 * 
	 * @param bucket
	 * @param writable
	 * @param b
	 * @param storage_service2
	 */
	private CompletableFuture<IBucketLogger> getBucketLogger(DataBucketBean bucket,
			IDataWriteService<JsonNode> writable, boolean isSystem,
			IStorageService storage_service2) {
		//initial the logging bucket path in case it hasn't been created yet
		try {
			DataBucketCrudService.createFilePaths(bucket, storage_service);
		} catch (Exception e) {
			_logger.error("Error creating logging bucket file path: " + bucket.full_name(), e);
			CompletableFuture<IBucketLogger> future_error = new CompletableFuture<IBucketLogger>();
			future_error.completeExceptionally(e);
			return future_error;
		}
		return CompletableFuture.completedFuture(new BucketLogger(bucket, getWritable(bucket), isSystem));
	}
	
	/**
	 * Returns the key to cache writables on, currently "bucket.full_name:bucket.modified"
	 * @param bucket
	 * @return
	 */
	private static String getWritableCacheKey(final DataBucketBean bucket) { 
		return bucket.full_name() + ":" + Optional.ofNullable(bucket.modified()).map(d->d.toString());
	}
	
	/**
	 * Implementation of the IBucketLogger that just filters log messages based on the ManagementSchema in
	 * the DatabucketBean and pushes objects into a writable created from the same schema at initialization of this object.
	 * @author Burch
	 *
	 */
	private class BucketLogger implements IBucketLogger {
		final IDataWriteService<JsonNode> logging_writable;
		final boolean isSystem;
		final DataBucketBean bucket;
		final String date_field;
		final Level default_log_level;  //holds the default log level for quick matching
		final ImmutableMap<String, Level> bucket_logging_thresholds; //holds bucket logging overrides for quick matching
		
		public BucketLogger(final DataBucketBean bucket, final IDataWriteService<JsonNode> logging_writable, final boolean isSystem) {
			this.bucket = bucket;
			this.logging_writable = logging_writable;
			this.isSystem = isSystem;
			this.bucket_logging_thresholds = LoggingUtils.getBucketLoggingThresholds(bucket);
			this.date_field = Optional.ofNullable(properties.default_time_field()).orElse("date");
			this.default_log_level = isSystem ? Optional.ofNullable(properties.default_system_log_level()).orElse(Level.OFF) : Optional.ofNullable(properties.default_user_log_level()).orElse(Level.OFF);			
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean)
		 */
		@Override
		public CompletableFuture<?> log(final Level level, final BasicMessageBean message) {		
			if ( LoggingUtils.meetsLogLevelThreshold(level, bucket_logging_thresholds, message.source(), default_log_level)) {
				//create log message to output:				
				final JsonNode logObject = LoggingUtils.createLogObject(level, bucket, message, isSystem, date_field);
				
				//send message to output log file
				_logger.debug("LOGGING MSG: " + logObject.toString());		
				return logging_writable.storeObject(logObject);
			} else {
				return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(this.getClass().getName(), "Log message dropped, below threshold", "n/a"));
			}			
		}		
	}
}
