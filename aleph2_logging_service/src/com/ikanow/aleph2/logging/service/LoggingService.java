package com.ikanow.aleph2.logging.service;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

public class LoggingService implements ILoggingService {
	private final static Logger _logger = LogManager.getLogger();
	private final static String LOGGING_PREFIX = "/aleph2_logging";
	protected final ISearchIndexService search_index_service;
	
	@Inject
	public LoggingService(final ISearchIndexService search_index_service) {
		this.search_index_service = search_index_service;
	}
	
	@Override
	public CompletableFuture<?> log(String message) {
		//log system to /system/<tech>
		//get logger to write out to
		final DataBucketBean external_bucket = getSystemBucket("TODO");
		final IDataWriteService<BasicMessageBean> logging_service = getLoggingServiceForBucket(external_bucket);
		
		//create BMB to output
		final BasicMessageBean bmb = BeanTemplateUtils.build(BasicMessageBean.class)
				.with(BasicMessageBean::message, message)
				.with(BasicMessageBean::date, new Date())
				.done().get();
		
		//send message to output log file
		_logger.debug("LOGGING_SERVICE_SYSTEM: " + convertBasicMessageBeanToString(bmb));
		return logging_service.storeObject(bmb);
	}

	

	@Override
	public CompletableFuture<?> log(String external_service_name, String message) {
		//log external to /external<tech>
		//get logger to write out to
		final DataBucketBean system_bucket = getExternalBucket(external_service_name);
		final IDataWriteService<BasicMessageBean> logging_service = getLoggingServiceForBucket(system_bucket);
		
		//create BMB to output
		final BasicMessageBean bmb = BeanTemplateUtils.build(BasicMessageBean.class)
				.with(BasicMessageBean::message, message)
				.with(BasicMessageBean::date, new Date())
				.done().get();
		
		//send message to output log file
		_logger.debug("LOGGING_SERVICE_EXTERNAL: " + convertBasicMessageBeanToString(bmb));
		return logging_service.storeObject(bmb);
	}
	
	private DataBucketBean getSystemBucket(final String technology_name) {
		return BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/system/" + technology_name + "/")
				.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class)
						.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(SearchIndexSchemaBean.class)
								.with(SearchIndexSchemaBean::enabled, true)
								.done().get())
						.done().get())
				.done().get();
	}
	
	private DataBucketBean getExternalBucket(final String technology_name) {
		return BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/external/" + technology_name + "/")
				.with(DataBucketBean::data_schema, BeanTemplateUtils.build(DataSchemaBean.class)
						.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(SearchIndexSchemaBean.class)
								.with(SearchIndexSchemaBean::enabled, true)
								.done().get())
						.done().get())
				.done().get();
	}

	@Override
	public CompletableFuture<?> log(final DataBucketBean bucket, final String message) {		
		//get logger to write out to
		final IDataWriteService<BasicMessageBean> logging_service = getLoggingServiceForBucket(bucket);
		
		//create BMB to output
		final BasicMessageBean bmb = BeanTemplateUtils.build(BasicMessageBean.class)
				.with(BasicMessageBean::message, message)
				.with(BasicMessageBean::date, new Date())
				.done().get();
		
		//send message to output log file
		_logger.debug("LOGGING_SERVICE_BUCKET: " + convertBasicMessageBeanToString(bmb));		
		return logging_service.storeObject(bmb);
	}
	
	/**
	 * Returns back a IDataWriteService pointed at a logging output location for the given bucket
	 * @param bucket
	 * @return
	 */
	protected IDataWriteService<BasicMessageBean> getLoggingServiceForBucket(final DataBucketBean bucket) {
		//change the bucket.full_name to point to a logging location
		final DataBucketBean bucket_logging = LoggingService.convertBucketToLoggingBucket(bucket);
		
		//return crudservice pointing to this path
		return search_index_service.getDataService().get().getWritableDataService(BasicMessageBean.class, bucket_logging, Optional.empty(), Optional.empty()).get();
	}

	/**
	 * Returns back a DataBucketBean w/ the full_name changed to reflect the logging path.
	 * i.e. just prefixes the bucket.full_name with LOGGING_PREFIX (/alelph2_logging)
	 * 
	 * @param bucket
	 * @return
	 */
	public static DataBucketBean convertBucketToLoggingBucket(final DataBucketBean bucket) {
		//TODO need to modify search_index_schema to add a temporal element to time out items after 30d
		return BeanTemplateUtils.clone(bucket)
				.with(DataBucketBean::full_name, LOGGING_PREFIX + bucket.full_name())
				.done();
	}
	
	/**
	 * Converts a BMB to a String we can output in a log message.
	 * 
	 * @param bean
	 * @return
	 */
	protected String convertBasicMessageBeanToString(final BasicMessageBean bean) {
		//TODO get rid of this function, don't need it anymore
		return new StringBuilder()
		.append("(")
		.append(bean.date())
		.append(") ")
		.append(bean.message())
		.toString();		
	}

	
}
