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
package com.ikanow.aleph2.search_service.elasticsearch.utils;

import java.io.IOException;
import java.util.Optional;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

/** A collection of utilities for converting buckets into Elasticsearch attributes
 * @author Alex
 */
public class ElasticsearchIndexUtils {


	/** Returns the base index name (before any date strings, splits etc) have been appended
	 * @param bucket
	 * @return
	 */
	public static String getBaseIndexName(final DataBucketBean bucket) {
		return bucket._id().toLowerCase().replace("-", "_");
	}
	
	/** Converts any index back to its spawning bucket
	 * @param index_name - the elasticsearch index name
	 * @return
	 */
	public static String getBucketIdFromIndexName(String index_name) {
		return index_name.replaceFirst("([a-z0-9]+_[a-z0-9]+_[a-z0-9]+_[a-z0-9]+_[a-z0-9]+).*", "$1").replace("_", "-");
	}
	
	/** Create a template to be applied to all indexes generated from this bucket
	 * @param bucket
	 * @return
	 */
	public XContentBuilder getTemplateMapping(final DataBucketBean bucket) {
		//TODO just call getMapping and register with the bucket's base index name
		return null;
	}
	
	/** Creates a mapping for the bucket
	 * @param bucket
	 * @return
	 * @throws IOException 
	 */
	public XContentBuilder getMapping(final DataBucketBean bucket, Optional<XContentBuilder> to_embed) {
		try {
			final XContentBuilder start = to_embed.orElse(XContentFactory.jsonBuilder().startObject());
			if (null == bucket.data_schema()) return start;
			if ((null != bucket.data_schema().search_index_schema()) && 
					Optional.ofNullable(bucket.data_schema().search_index_schema().enabled()).orElse(true))
			{
				//TODO search schema
				
			}
			if ((null != bucket.data_schema().temporal_schema()) && 
					Optional.ofNullable(bucket.data_schema().temporal_schema().enabled()).orElse(true))
			{
				//TODO temporal schema
				
			}
			if ((null != bucket.data_schema().columnar_schema()) && 
					Optional.ofNullable(bucket.data_schema().columnar_schema().enabled()).orElse(true))
			{
				//TODO columnar_schema schema				
			}
			return start;//TODO
		}
		catch (IOException e) {
			//Handle fake "IOException"
			return null;
		}
	}
	
	
}
