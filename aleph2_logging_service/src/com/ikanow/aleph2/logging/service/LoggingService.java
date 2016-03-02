package com.ikanow.aleph2.logging.service;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
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
	//TODO might need storage_service also if its turned on
	
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
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#log(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean)
	 */
	@Override
	public CompletableFuture<?> log(final Level level, final DataBucketBean bucket, final BasicMessageBean message) {
		if ( LoggingUtils.meetsLogLevelThreshold(level, bucket, message.source(), Optional.ofNullable(properties.default_user_log_level()).orElse(Level.OFF)) ) {
			//get logger to write out to
			final IDataWriteService<JsonNode> logging_service = getWritable(bucket); // getLoggingServiceForBucket(bucket);
			
			//create log message to output:
			final JsonNode logObject = createLogObject(level, bucket, message, false);
			
			//send message to output log file
			_logger.debug("LOGGING_SERVICE_BUCKET: " + logObject.toString());		
			return logging_service.storeObject(logObject);
		} else {
			return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(this.getClass().getName(), "Log message dropped, below threshold", "n/a"));
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService#systemLog(org.apache.logging.log4j.Level, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean)
	 */
	@Override
	public CompletableFuture<?> systemLog(final Level level, final Optional<DataBucketBean> bucket, final BasicMessageBean message) {
		final DataBucketBean log_bucket = bucket.orElse(LoggingUtils.getExternalBucket(message.source(), Optional.ofNullable(properties.default_system_log_level()).orElse(Level.OFF)));
		if ( LoggingUtils.meetsLogLevelThreshold(level, log_bucket, message.source(), Optional.ofNullable(properties.default_system_log_level()).orElse(Level.OFF) ) ) {
			//get logger to write out to
			final IDataWriteService<JsonNode> logging_service = getWritable(log_bucket);//getLoggingServiceForBucket(log_bucket);
			
			//create log message to output:
			final JsonNode logObject = createLogObject(level, log_bucket, message, true);
			
			//send message to output log file
			_logger.debug("LOGGING_SERVICE_SYSTEM: " + logObject.toString());		
			return logging_service.storeObject(logObject);
		} else {
			return CompletableFuture.completedFuture(ErrorUtils.buildSuccessMessage(this.getClass().getName(), "Log message dropped, below threshold", "n/a"));
		}
	}
	
	/**
	 * Builds a jsonnode log message object, contains fields for date, message, generated_by, bucket, subsystem, and severity
	 * 
	 * @param level
	 * @param bucket
	 * @param message
	 * @param isSystemMessage
	 * @return
	 */
	private JsonNode createLogObject(final Level level, final DataBucketBean bucket, final BasicMessageBean message, final boolean isSystemMessage) {
		final ObjectMapper _mapper = new ObjectMapper();
		return Optional.ofNullable(message.details()).map(d -> _mapper.convertValue(d, ObjectNode.class)).orElseGet(() -> _mapper.createObjectNode())
				.put(Optional.ofNullable(properties.default_time_field()).orElse("date"), message.date().getTime()) //TODO can I actually pass in a date object/need to?
				.put("message", ErrorUtils.show(message))
				.put("generated_by", isSystemMessage ? "system" : "user")
				.put("bucket", bucket.full_name())
				.put("subsystem", message.source())
				.put("severity", level.toString());			
	}
	
	/**
	 * Retrieves a writable for the given bucket, trys to find it in the cache first, creates a new one if it can't and adds it to the cache for future requests.
	 * 
	 * @param log_bucket
	 * @return
	 */
	private IDataWriteService<JsonNode> getWritable(final DataBucketBean log_bucket) {
		//TODO do I need to put a lock around checking/updating the cache?
		//TODO will this be wrong if someone makes changes to the bucket?  Do I need to actual hash the bucket json to make a key instead? that way I can tell if something has changed? <full_name>_hash
		return Optional.ofNullable(bucket_writable_cache.getIfPresent(log_bucket.full_name())).orElseGet(() -> {	
			_logger.debug("cache miss, adding writable to cache for bucket: " + log_bucket.full_name());
			final IDataWriteService<JsonNode> logging_service = LoggingUtils.getLoggingServiceForBucket(search_index_service, storage_service, log_bucket); //TODO will I need temporal/columnar services also
			bucket_writable_cache.put(log_bucket.full_name(), logging_service);
			return logging_service;
		});
		
		
	}	
}
