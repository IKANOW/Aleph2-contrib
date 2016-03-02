/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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
package com.ikanow.aleph2.logging.utils;

import java.util.Arrays;
import java.util.Optional;

import org.apache.logging.log4j.Level;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.ManagementSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.ColumnarSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.LoggingSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.TemporalSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

/**
 * @author Burch
 *
 */
public class LoggingUtils {

	private final static String LOGGING_PREFIX = "/aleph2_logging";
	private static final String BUCKETS_PREFIX = "/buckets";
	private static final String EXTERNAL_PREFIX = "/external";
	
	/**
	 * Returns true if the given log message meets the bucket/subsystems minimal logging level, false otherwise.
	 * 
	 * @param level
	 * @param bucket
	 * @return
	 */
	public static boolean meetsLogLevelThreshold(final Level level, final DataBucketBean bucket, final String subsystem, final Level default_log_level) {
		final Level curr_min_level = getMinimalLogLevel(bucket, subsystem, default_log_level);		
		return curr_min_level.isLessSpecificThan(level);
	}
	
	/**
	 * Returns the minimal acceptable logging level for the given bucket/subsystem.  Determines this by checking
	 * if the subsystem is in log_level_overrides and uses that logging level, otherwise uses the default log_level, falls back
	 * to turning logging off.
	 * 
	 * @param bucket
	 * @return
	 */
	private static Level getMinimalLogLevel(final DataBucketBean bucket, final String subsystem, final Level default_log_level) {
		if (bucket.management_schema() != null &&
				bucket.management_schema().logging_schema() != null ) {
			//check if an override exists otherwise return default level
			return Optional.ofNullable( bucket.management_schema().logging_schema().log_level_overrides().get(subsystem))
					.orElse(bucket.management_schema().logging_schema().log_level());
		}
		return default_log_level; //default to off if logging service isn't configured		
	}
	
	/**
	 * Returns back a DataBucketBean w/ the full_name changed to reflect the logging path.
	 * i.e. just prefixes the bucket.full_name with LOGGING_PREFIX (/alelph2_logging)
	 * 
	 * @param bucket
	 * @return
	 */
	public static DataBucketBean convertBucketToLoggingBucket(final DataBucketBean bucket) {
		return BeanTemplateUtils.clone(bucket)
				.with(DataBucketBean::full_name, LOGGING_PREFIX + BUCKETS_PREFIX + bucket.full_name())
				.with(DataBucketBean::data_schema, getLoggingDataSchema(bucket.management_schema()))
				.done();
	}
	
	/**
	 * Returns a DataSchemaBean with the passed in mgmt schema schemas copied over or defaults inserted
	 * 
	 * @param mgmt_schema
	 * @return
	 */
	private static DataSchemaBean getLoggingDataSchema(final ManagementSchemaBean mgmt_schema) {
		//TODO set these to proper defaults
		return BeanTemplateUtils.build(DataSchemaBean.class)
				.with(DataSchemaBean::columnar_schema, Optional.ofNullable(mgmt_schema.columnar_schema()).orElse(BeanTemplateUtils.build(ColumnarSchemaBean.class)
							.with(ColumnarSchemaBean::enabled, true)
							.with(ColumnarSchemaBean::field_type_include_list, Arrays.asList("NUMBER", "STRING"))
						.done().get()))
				.with(DataSchemaBean::storage_schema, Optional.ofNullable(mgmt_schema.storage_schema()).orElse(BeanTemplateUtils.build(StorageSchemaBean.class)
							.with(StorageSchemaBean::enabled, true)
							.with(StorageSchemaBean::processed, BeanTemplateUtils.build(StorageSubSchemaBean.class)
										.with(StorageSubSchemaBean::exist_age_max, "1 year")
										.with(StorageSubSchemaBean::grouping_time_period, "1 month")
									.done().get())
						.done().get()))
				.with(DataSchemaBean::search_index_schema, Optional.ofNullable(mgmt_schema.search_index_schema()).orElse(BeanTemplateUtils.build(SearchIndexSchemaBean.class)
							.with(SearchIndexSchemaBean::enabled, true)
						.done().get()))
				.with(DataSchemaBean::temporal_schema, Optional.ofNullable(mgmt_schema.temporal_schema()).orElse(BeanTemplateUtils.build(TemporalSchemaBean.class)
							.with(TemporalSchemaBean::enabled, true)
							.with(TemporalSchemaBean::exist_age_max, "1 month")
						.done().get()))
			.done().get();	
	}
	

	
	/**
	 * Builds a minimal bucket pointing the full path to the external bucket/subsystem
	 * 
	 * @param subsystem
	 * @return
	 */
	public static DataBucketBean getExternalBucket(final String subsystem, final Level default_log_level) {
		return BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, EXTERNAL_PREFIX + "/" + subsystem + "/")	
				.with(DataBucketBean::management_schema, BeanTemplateUtils.build(ManagementSchemaBean.class)
							.with(ManagementSchemaBean::logging_schema, BeanTemplateUtils.build(LoggingSchemaBean.class)
										.with(LoggingSchemaBean::log_level, default_log_level)
									.done().get())
						.done().get())
				.done().get();
	}
	

	
	/**
	 * Returns back a IDataWriteService pointed at a logging output location for the given bucket
	 * @param bucket
	 * @return
	 */
	public static IDataWriteService<JsonNode> getLoggingServiceForBucket(final ISearchIndexService search_index_service, final IStorageService storage_service, final DataBucketBean bucket) {
		//change the bucket.full_name to point to a logging location
		final DataBucketBean bucket_logging = LoggingUtils.convertBucketToLoggingBucket(bucket);
		
		//return crudservice pointing to this path
		//TODO in the future need to switch to wrapper that gets the actual services we need (currently everything is ES so this if fine)
		return search_index_service.getDataService().get().getWritableDataService(JsonNode.class, bucket_logging, Optional.empty(), Optional.empty()).get();
	}
}
