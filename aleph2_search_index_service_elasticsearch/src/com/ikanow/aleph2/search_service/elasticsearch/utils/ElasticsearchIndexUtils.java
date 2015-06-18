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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.utils.Tuples;

import fj.data.Either;

/** A collection of utilities for converting buckets into Elasticsearch attributes
 * @author Alex
 */
public class ElasticsearchIndexUtils {


	/////////////////////////////////////////////////////////////////////
	
	// INDEX NAMES
	
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
	
	/////////////////////////////////////////////////////////////////////
	
	// MAPPINGS - DEFAULTS
	
	/** Builds a lookup table of settings 
	 * @param mapping - the mapping to use
	 * @param type - if the index has a specific type, lookup that and _default_ ; otherwise just _default
	 * @return
	 */
	public static LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> parseDefaultMapping(final JsonNode mapping, Optional<String> type) {
		final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> ret = 
				Optional.ofNullable(mapping.get("mappings"))
					.map(m -> m.get(type.orElse("_default_")))
					.filter(i -> !i.isNull())
					.map(i -> {
						final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> props = getProperties(i);						
						props.putAll(getTemplates(i));
						return props;
					})
					.orElse(new LinkedHashMap<>());
		
		if (type.isPresent()) { // If this was on behalf of a specific type then also roll up the defaults
			ret.putAll(parseDefaultMapping(mapping, Optional.empty()));
		}		
		return ret;
	}
	
	protected static LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> getProperties(JsonNode index) {
		return Optional.ofNullable(index.get("properties"))
					.filter(p -> !p.isNull())
					.map(p -> {
						return StreamSupport.stream(Spliterators.spliteratorUnknownSize(p.fields(), Spliterator.ORDERED), false)
							.collect(Collectors.
									<Map.Entry<String, JsonNode>, Either<String, Tuple2<String, String>>, JsonNode, LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode>>
									toMap(
										kv -> Either.<String, Tuple2<String, String>>left(kv.getKey()),
										kv -> kv.getValue(),
										(v1, v2) -> v1, // (should never happen)
										() -> new LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode>()
									));
					})
					.orElse(new LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode>());
	}
	
	protected static LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> getTemplates(JsonNode index) {
		return Optional.ofNullable(index.get("dynamic_templates"))
					.filter(p -> !p.isNull())					
					.map(p -> {
						return StreamSupport.stream(Spliterators.spliteratorUnknownSize(p.fields(), Spliterator.ORDERED), false)
							.collect(Collectors.
									<Map.Entry<String, JsonNode>, Either<String, Tuple2<String, String>>, JsonNode, LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode>>
									toMap(
										kv -> Either.right(buildMatchPair(kv.getValue())),
										kv -> kv.getValue().get("mapping"),
										(v1, v2) -> v1, // (should never happen)
										() -> new LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode>()
									));
					})
					.orElse(new LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode>());
	}
	
	protected static Tuple2<String, String> buildMatchPair(final JsonNode template) {
		return Tuples._2T(
				Optional.ofNullable(template.get("match")).map(j -> j.asText().replace("*", "STAR")).orElse("STAR")
				,
				Optional.ofNullable(template.get("match_mapping_type")).map(j -> j.asText()).orElse("STAR")
				);
	}
	
	/////////////////////////////////////////////////////////////////////
	
	// MAPPINGS - CREATION
	
	/** Create a template to be applied to all indexes generated from this bucket
	 * @param bucket
	 * @return
	 */
	public XContentBuilder getTemplateMapping(final DataBucketBean bucket) {
		//TODO just call getMapping and register with the bucket's base index name
		return null;
	}

	/** Creates a mapping for the bucket - temporal elements
	 * @param bucket
	 * @return
	 * @throws IOException 
	 */
	public static XContentBuilder getTemporalMapping(final DataBucketBean bucket, Optional<XContentBuilder> to_embed) {
		try {
			final XContentBuilder start = to_embed.orElse(XContentFactory.jsonBuilder().startObject());
			if (null == bucket.data_schema()) return start;
			
			// Nothing to be done here
			
			return start;
		}
		catch (IOException e) {
			//Handle fake "IOException"
			return null;
		}
	}

	/** Creates a mapping for the bucket - columnar elements
	 * @param bucket
	 * @return
	 * @throws IOException 
	 */
	public static XContentBuilder getColumnarMapping(final DataBucketBean bucket, Optional<XContentBuilder> to_embed) {
		try {
			final XContentBuilder start = to_embed.orElse(XContentFactory.jsonBuilder().startObject());
			if (!Optional.ofNullable(bucket.data_schema()).map(DataSchemaBean::columnar_schema).isPresent()) return start;

			// Quick guide to mappings
			// under mappings you can specify either
			// - specific types
			// - _default_, which applies to anything that doesn't match that type
			//   - then under each type (or _default_)..
			//      - you can specify dynamic_templates/properties/_all/dynamic_date_formats/date_detection/numeric_detection
			//         - under properties you can then specify types and then fields
			//         - under dynamic_templates you can specify fields
			//           - under fields you can specify type/fielddata(*)/similarity/analyzer/etc
			//
			// (*) https://www.elastic.co/guide/en/elasticsearch/reference/current/fielddata-formats.html
			//
			// OK so we can specify parts of mappings in the following ways:
			// - COLUMNAR: 
			//   - based on field name .. maps to path_match
			//   - based on type .. maps to match_mapping_type
			//   (and then for these columnar types we want to specify 
			//      "type": "{dynamic_type}", "index": "no", "fielddata": { "format": "doc_values" } // (or disabled)
			//      but potentially would like to be able to add more info as well/instead
			//      so maybe need a default and then per-key override
			//
			// OK ... then in addition, we want to be able to set other elements of the search from the search override schema
			// The simplest way of doing this is probably just to force matching on fields/patterns and then to merge them
			
			/**/
			//DataSchemaBean.ColumnarSchemaBean schema = bucket.data_schema().columnar_schema();
		
			// Lots to do here:
			
			/**/
			//final Optional<Pattern> exclude = Optional.ofNullable(schema.field_exclude_pattern()).map(r -> Pattern.compile(r));
			
//			Optionals.ofNullable(schema.field_include_list()).stream()
//				.filter(f -> exclude.map())
			
			
			return start;
		}
		catch (IOException e) {
			//Handle fake "IOException"
			return null;
		}
	}
	
	/** Creates a mapping for the bucket - search service elements
	 * @param bucket
	 * @return
	 * @throws IOException 
	 */
	public static XContentBuilder getSearchServiceMapping(final DataBucketBean bucket, Optional<XContentBuilder> to_embed) {
		try {
			final XContentBuilder start = to_embed.orElse(XContentFactory.jsonBuilder().startObject());
			if (null == bucket.data_schema()) return start;
			
			//TODO: user can add extra mapping elements to nest objects etc
			
			return start;
		}
		catch (IOException e) {
			//Handle fake "IOException"
			return null;
		}
	}

	//TODO: need the schema checks here
//	public XContentBuilder getMapping(final DataBucketBean bucket, Optional<XContentBuilder> to_embed) {
//		try {
//			final XContentBuilder start = to_embed.orElse(XContentFactory.jsonBuilder().startObject());
//			if (null == bucket.data_schema()) return start;
//			if ((null != bucket.data_schema().search_index_schema()) && 
//					Optional.ofNullable(bucket.data_schema().search_index_schema().enabled()).orElse(true))
//			{
//				//TODO search schema
//				
//			}
//			if ((null != bucket.data_schema().temporal_schema()) && 
//					Optional.ofNullable(bucket.data_schema().temporal_schema().enabled()).orElse(true))
//			{
//				//TODO temporal schema
//				
//			}
//			if ((null != bucket.data_schema().columnar_schema()) && 
//					Optional.ofNullable(bucket.data_schema().columnar_schema().enabled()).orElse(true))
//			{
//				//TODO columnar_schema schema				
//			}
//			return start;//TODO
//		}
//		catch (IOException e) {
//			//Handle fake "IOException"
//			return null;
//		}
//	}
	
	
}
