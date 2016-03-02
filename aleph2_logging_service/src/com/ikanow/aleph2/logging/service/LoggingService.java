package com.ikanow.aleph2.logging.service;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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
	public IBucketLogger getLogger(DataBucketBean bucket) {
		return new BucketLogger(bucket, getWritable(bucket), false);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#getSystemLogger(java.util.Optional)
	 */
	@Override
	public IBucketLogger getSystemLogger(DataBucketBean bucket) {		
		return new BucketLogger(bucket, getWritable(bucket), true);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#getExternalLogger(java.lang.String)
	 */
	@Override
	public IBucketLogger getExternalLogger(final String subsystem) {
		final DataBucketBean bucket = LoggingUtils.getExternalBucket(subsystem, Optional.ofNullable(properties.default_system_log_level()).orElse(Level.OFF));
		return new BucketLogger(bucket, getWritable(bucket), true);
	}
	
	/**
	 * Retrieves a writable for the given bucket, trys to find it in the cache first, creates a new one if it can't and adds it to the cache for future requests.
	 * 
	 * @param log_bucket
	 * @return
	 */
	private IDataWriteService<JsonNode> getWritable(final DataBucketBean log_bucket) {
		//TODO do I need to put a lock around checking/updating the cache?
		return Optional.ofNullable(bucket_writable_cache.getIfPresent(log_bucket.full_name())).orElseGet(() -> {	
			_logger.debug("cache miss, adding writable to cache for bucket: " + log_bucket.full_name());
			final IDataWriteService<JsonNode> logging_service = LoggingUtils.getLoggingServiceForBucket(search_index_service, storage_service, log_bucket); //TODO will I need temporal/columnar services also
			bucket_writable_cache.put(log_bucket.full_name() + ":" + Optional.ofNullable(log_bucket.modified()).map(d->d.toString()), logging_service);
			return logging_service;
		});
	}	
	
	private class BucketLogger implements IBucketLogger {
		final IDataWriteService<JsonNode> logging_writable;
		final boolean isSystem;
		final DataBucketBean bucket;
		final String date_field;
		final Level default_log_level;
		//TODO need to store the potential logging thresholds on creation in here, so I can quickly match against them
		
		public BucketLogger(final DataBucketBean bucket, final IDataWriteService<JsonNode> logging_writable, final boolean isSystem) {
			this.bucket = bucket;
			this.logging_writable = logging_writable;
			this.isSystem = isSystem;
			//TODO should I be reaching out of this object to grab the properties like this :/ or just pass them in
			this.date_field = Optional.ofNullable(properties.default_time_field()).orElse("date");
			this.default_log_level = isSystem ? Optional.ofNullable(properties.default_system_log_level()).orElse(Level.OFF) : Optional.ofNullable(properties.default_user_log_level()).orElse(Level.OFF);
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger#log(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean)
		 */
		@Override
		public CompletableFuture<?> log(final Level level, final BasicMessageBean message) {	
			//TODO cache the logging levels as we need them so we don't have to look through the bucket object every time
			//means i'd have to move meetsLogLevelThreshold inside this object
			if ( LoggingUtils.meetsLogLevelThreshold(level, bucket, message.source(), default_log_level) ) {				
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
