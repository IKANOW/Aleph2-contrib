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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean;

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
	
	/** Returns either a specifc type name, or "_default_" if auto types are used
	 * @param bucket
	 * @return
	 */
	public static String getTypeKey(final DataBucketBean bucket, final ObjectMapper mapper) {
		return Optional.ofNullable(bucket.data_schema())
					.map(DataSchemaBean::search_index_schema)				
					.filter(s -> Optional.ofNullable(s.enabled()).orElse(true))
					.map(DataSchemaBean.SearchIndexSchemaBean::technology_override_schema)
					.map(t -> BeanTemplateUtils.from(mapper.convertValue(t, JsonNode.class), SearchIndexSchemaDefaultBean.class).get())
					.<String>map(cfg -> {
						return Patterns.match(cfg.collide_policy()).<String>andReturn()
								.when(cp -> SearchIndexSchemaDefaultBean.CollidePolicy.error == cp, 
										__ -> Optional.ofNullable(cfg.type_name_or_prefix()).orElse("data_object")) // (ie falls through to default below)
								.otherwise(__ -> null);
					})
				.orElse("_default_"); // (the default - "auto type")
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
					.map(m -> {
						if (!m.isObject()) throw new RuntimeException("mappings must be object");
						return m;
					})
					.map(m -> Optional.ofNullable(m.get(type.orElse("_default_")))
												.map(mm -> !mm.isNull() ? mm : m.get("_default_"))
											.orElse(m.get("_default_")))
					.filter(m -> !m.isNull())
					.map(i -> {
						if (!i.isObject()) throw new RuntimeException(type + " must be object");
						return i;
					})
					.map(i -> {
						final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> props = getProperties(i);						
						props.putAll(getTemplates(i));
						return props;
					})
					.orElse(new LinkedHashMap<>());
		
		return ret;
	}
	
	/** Get a set of field mappings from the "properties" section of a mapping
	 * @param index
	 * @return
	 */
	protected static LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> getProperties(JsonNode index) {
		return Optional.ofNullable(index.get("properties"))
					.filter(p -> !p.isNull())
					.map(p -> {
						if (!p.isObject()) throw new RuntimeException("properties must be object");
						return p;
					})
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
	
	/** Get a set of field mappings from the "dynamic_templates" section of a mapping
	 * @param index
	 * @return
	 */
	protected static LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> getTemplates(JsonNode index) {
		return Optional.ofNullable(index.get("dynamic_templates"))
					.filter(p -> !p.isNull())					
					.map(p -> {
						if (!p.isArray()) throw new RuntimeException("dynamic_templates must be object");
						return p;
					})
					.map(p -> {
						return StreamSupport.stream(Spliterators.spliteratorUnknownSize(p.elements(), Spliterator.ORDERED), false)
							.map(pf -> {
								if (!pf.isObject()) throw new RuntimeException("dynamic_templates[*] must be object");
								return pf;
							})
							.flatMap(pp -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(pp.fields(), Spliterator.ORDERED), false))
							.collect(Collectors.
								<Map.Entry<String, JsonNode>, Either<String, Tuple2<String, String>>, JsonNode, LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode>>
								toMap(
									kv -> Either.right(buildMatchPair(kv.getValue())),
									kv -> kv.getValue(),
									(v1, v2) -> v1, // (should never happen)
									() -> new LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode>()
								));
					})
					.orElse(new LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode>());
	}
	
	/** Builds a match pair from a field mapping
	 * @param template
	 * @return
	 */
	protected static Tuple2<String, String> buildMatchPair(final JsonNode template) {
		return Tuples._2T(
				Optional.ofNullable(template.get("match")).map(j -> j.asText()).orElse("*")
				,
				Optional.ofNullable(template.get("match_mapping_type")).map(j -> j.asText()).orElse("*")
				);
	}
	
	/** Creates a single string from a match/match_mapping_type pair
	 * @param field_info
	 * @return
	 */
	protected static String getFieldNameFromMatchPair(final Tuple2<String, String> field_info) {
		return field_info._1().replace("*", "STAR").replace("_", "BAR") + "_" + field_info._2().replace("*", "STAR");
	};
	
	
	
	/////////////////////////////////////////////////////////////////////
	
	// MAPPINGS - CREATION
	
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
	
	
	///////////////////////////////////////////////////////////////
	
	// TEMPORAL PROCESSING
	
	/** Creates a mapping for the bucket - temporal elements
	 * @param bucket
	 * @return
	 * @throws IOException 
	 */
	public static XContentBuilder getTemporalMapping(final DataBucketBean bucket, final Optional<XContentBuilder> to_embed) {
		try {
			final XContentBuilder start = to_embed.orElse(XContentFactory.jsonBuilder().startObject());
			if (!Optional.ofNullable(bucket.data_schema())
					.map(DataSchemaBean::temporal_schema)
					.filter(s -> Optional.ofNullable(s.enabled()).orElse(true))
					.isPresent()) 
						return start;			
			
			// Nothing to be done here
			
			return start;
		}
		catch (IOException e) {
			//Handle fake "IOException"
			return null;
		}
	}

	///////////////////////////////////////////////////////////////
	
	// COLUMNAR PROCESSING
	
	// (Few constants to tidy stuff up)
	protected final static String BACKUP_FIELD_MAPPING_PROPERTIES = "{\"index\":\"not_analyzed\"}";
	protected final static String BACKUP_FIELD_MAPPING_TEMPLATES = "{\"mapping\":" + BACKUP_FIELD_MAPPING_PROPERTIES + "}";
	protected final static String DEFAULT_FIELDDATA_NAME = "_default";
	protected final static String DISABLED_FIELDDATA = "{\"format\":\"disabled\"}";
	
	
	/** Creates a mapping for the bucket - columnar elements
	 * @param bucket
	 * @return
	 * @throws IOException 
	 */
	public static XContentBuilder getColumnarMapping(final DataBucketBean bucket, Optional<XContentBuilder> to_embed,
														final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> field_lookups,
														final JsonNode default_not_analyzed, final JsonNode default_analyzed,
														final ObjectMapper mapper)
	{
		try {
			final XContentBuilder start = to_embed.orElse(XContentFactory.jsonBuilder().startObject());
			if (!Optional.ofNullable(bucket.data_schema())
					.map(DataSchemaBean::columnar_schema)
					.filter(s -> Optional.ofNullable(s.enabled()).orElse(true))
					.isPresent()) 
						return start;			
			
			final Map<Either<String, Tuple2<String, String>>, JsonNode> column_lookups = Stream.of(			
				createFieldIncludeLookups(Optionals.ofNullable(bucket.data_schema().columnar_schema().field_include_list()).stream(),
						fn -> Either.left(fn), 
						field_lookups, default_not_analyzed, default_analyzed, mapper
						)
						,
				createFieldExcludeLookups(Optionals.ofNullable(bucket.data_schema().columnar_schema().field_exclude_list()).stream(),
						fn -> Either.left(fn), 
						field_lookups, mapper
						)
						,
				createFieldIncludeLookups(Optionals.ofNullable(bucket.data_schema().columnar_schema().field_include_pattern_list()).stream(),
						fn -> Either.right(Tuples._2T(fn, "*")), 
						field_lookups, default_not_analyzed, default_analyzed, mapper
						)
						,	
				createFieldIncludeLookups(Optionals.ofNullable(bucket.data_schema().columnar_schema().field_type_include_list()).stream(),
						fn -> Either.right(Tuples._2T("*", fn)), 
						field_lookups, default_not_analyzed, default_analyzed, mapper
						)
						,			
				createFieldExcludeLookups(Optionals.ofNullable(bucket.data_schema().columnar_schema().field_exclude_pattern_list()).stream(),
						fn -> Either.right(Tuples._2T(fn, "*")), 
						field_lookups, mapper
						)
						,
				createFieldExcludeLookups(Optionals.ofNullable(bucket.data_schema().columnar_schema().field_type_exclude_list()).stream(),
						fn -> Either.right(Tuples._2T("*", fn)), 
						field_lookups, mapper
					)
			)
			.flatMap(x -> x)
			.collect(Collectors.toMap(
					t2 -> t2._1(),
					t2 -> t2._2()
					));
			;			
			
			final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> combined_lookups = new LinkedHashMap<>();
			combined_lookups.putAll(column_lookups);
			combined_lookups.putAll(field_lookups); // (should overwrite them, ie field lookups get prio)
			
			// Build the mapping in 2 stages - left (properties) then right (dynamic_templates)
			
			final XContentBuilder properties = combined_lookups.entrySet().stream()
							// properties not dynamic_templates
							.filter(kv -> kv.getKey().isLeft())
							// overwrite with version built using columns if it exists
							.map(kv -> Tuples._2T(kv.getKey(), column_lookups.getOrDefault(kv.getKey(), kv.getValue())))
							.reduce(
								start.startObject("properties"), 
								Lambdas.wrap_u((acc, t2) -> acc.rawField(t2._1().left().value(), t2._2().toString().getBytes())), // (left by construction) 
								(acc1, acc2) -> acc1) // (not actually possible)
							.endObject();

			final XContentBuilder templates = combined_lookups.entrySet().stream()
					// properties not dynamic_templates
					.filter(kv -> kv.getKey().isRight())
					// overwrite with version built using columns if it exists
					.map(kv -> Tuples._2T(kv.getKey(), column_lookups.getOrDefault(kv.getKey(), kv.getValue())))
					.reduce(
							properties.startArray("dynamic_templates"),
							Lambdas.wrap_u((acc, t2) -> acc.startObject()
															.rawField(getFieldNameFromMatchPair(t2._1().right().value()), t2._2().toString().getBytes()) // (right by construction)
														.endObject()),  						
							(acc1, acc2) -> acc1) // (not actually possible)
					.endArray();
						
			return templates;
		}
		catch (IOException e) {
			//Handle in-practice-impossible "IOException"
			return null;
		}
	}
	
	/** Creates a list of JsonNodes containing the mapping for fields that will enable field data
	 * @param instream
	 * @param f
	 * @param field_lookups
	 * @param default_not_analyzed
	 * @param default_analyzed
	 * @param mapper
	 * @return
	 */
	protected static Stream<Tuple2<Either<String, Tuple2<String, String>>, JsonNode>> createFieldIncludeLookups(final Stream<String> instream,
								final Function<String, Either<String, Tuple2<String, String>>> f,
								final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> field_lookups,
								final JsonNode default_not_analyzed, final JsonNode default_analyzed,
								final ObjectMapper mapper)
	{
		return instream.<Tuple2<Either<String, Tuple2<String, String>>, JsonNode>>
			map(Lambdas.wrap_u(fn -> {
				final Either<String, Tuple2<String, String>> either = f.apply(fn);
				
				final ObjectNode mutable_field_metadata = (ObjectNode) Optional.ofNullable(field_lookups.get(either))
														.map(j -> j.deepCopy())
														.orElse(either.either(
																Lambdas.wrap_fj_u(__ -> mapper.readTree(BACKUP_FIELD_MAPPING_PROPERTIES)),
																Lambdas.wrap_fj_u(__ -> mapper.readTree(BACKUP_FIELD_MAPPING_TEMPLATES))
																));

				final ObjectNode mutable_field_mapping = either.isLeft()
									? mutable_field_metadata
									: (ObjectNode) mutable_field_metadata.get("mapping");
				
				if (either.isRight()) {
					mutable_field_metadata
						.put("match", either.right().value()._1())
						.put("match_mapping_type", either.right().value()._2());
				}
				
				final boolean is_analyzed = Optional.ofNullable(mutable_field_mapping.get("index"))
												.filter(j -> !j.isNull() && j.isTextual())
												.map(jt -> jt.asText().equalsIgnoreCase("analyzed") || jt.asText().equalsIgnoreCase("yes"))
												.orElse(false); 
				
				final JsonNode fielddata_settings = is_analyzed ? default_analyzed : default_not_analyzed;
				
				Optional.ofNullable(						
					Optional.ofNullable(fielddata_settings.get(either.<String>either
																(left->left, right->getFieldNameFromMatchPair(right))))
							.filter(j -> !j.isNull())
							.orElse(fielddata_settings.get(DEFAULT_FIELDDATA_NAME))
					)
					.ifPresent(j -> { if (!mutable_field_mapping.has("fielddata")) // (ie if user specifies fielddata in the search index schema then respect that)
											mutable_field_mapping.set("fielddata", j); });
				
				return Tuples._2T(either, mutable_field_metadata); 
			}));

	}
	
	/** Creates a list of JsonNodes containing the mapping for fields that will _disable_ field data
	 * @param instream
	 * @param f
	 * @param field_lookups
	 * @param default_not_analyzed
	 * @param default_analyzed
	 * @param mapper
	 * @return
	 */
	protected static Stream<Tuple2<Either<String, Tuple2<String, String>>, JsonNode>> createFieldExcludeLookups(final Stream<String> instream,
			final Function<String, Either<String, Tuple2<String, String>>> f,
			final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> field_lookups,
			final ObjectMapper mapper)
	{
		return instream.<Tuple2<Either<String, Tuple2<String, String>>, JsonNode>>
			map(Lambdas.wrap_u(fn -> {
				final Either<String, Tuple2<String, String>> either = f.apply(fn);
				final ObjectNode mutable_field_metadata = (ObjectNode) Optional.ofNullable(field_lookups.get(either))
														.map(j -> j.deepCopy())
														.orElse(either.either(
																Lambdas.wrap_fj_u(__ -> mapper.readTree(BACKUP_FIELD_MAPPING_PROPERTIES)),
																Lambdas.wrap_fj_u(__ -> mapper.readTree(BACKUP_FIELD_MAPPING_TEMPLATES))
																));

				final ObjectNode mutable_field_mapping = either.isLeft()
									? mutable_field_metadata
									: (ObjectNode) mutable_field_metadata.get("mapping");
				
				if (either.isRight()) {
					mutable_field_metadata
						.put("match", either.right().value()._1())
						.put("match_mapping_type", either.right().value()._2());
				}				
																
				if (!mutable_field_mapping.has("fielddata"))  // (ie if user specifies fielddata in the search index schema then respect that)
					mutable_field_mapping.set("fielddata", mapper.readTree(DISABLED_FIELDDATA));
				
				return Tuples._2T(either, mutable_field_metadata); 
			}));
	}

	///////////////////////////////////////////////////////////////
	
	// SEARCH PROCESSING
		
	/** Creates a mapping for the bucket - search service elements .. up to but not including the mapping + type
	 *  NOTE: creates an embedded object that is {{, ie must be closed twice subsequently in order to be a well formed JSON object
	 * @param bucket
	 * @return
	 * @throws IOException 
	 */
	public static XContentBuilder getSearchServiceMapping(final DataBucketBean bucket,
															final ElasticsearchIndexServiceConfigBean config,
															final Optional<XContentBuilder> to_embed,
															final ObjectMapper mapper)
	{
		try {
			final XContentBuilder start = to_embed.orElse(XContentFactory.jsonBuilder().startObject());

			// (Nullable)
			final ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean search_schema =
					Optional.ofNullable(bucket.data_schema())
						.map(DataSchemaBean::search_index_schema)				
						.filter(s -> Optional.ofNullable(s.enabled()).orElse(true))
						.map(DataSchemaBean.SearchIndexSchemaBean::technology_override_schema)
						.map(m -> BeanTemplateUtils.from(mapper.convertValue(m, JsonNode.class), 
								ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean.class).get())
					.orElse(config.search_technology_override());
			
			//(very briefly Nullable)
			final JsonNode settings = Optional.ofNullable(search_schema)
											.map(s -> s.settings())
											.map(o -> mapper.convertValue(o, JsonNode.class))
										.orElse(Optional.ofNullable(config.search_technology_override()).map(cfg -> cfg.settings())
												.map(o -> mapper.convertValue(o, JsonNode.class)).orElse(null));
			
			// Settings
			
			final String type_key = getTypeKey(bucket, mapper);
			
			return Lambdas.wrap_u(__ -> {
				if (null == settings) { // nothing to do
					return start;
				}
				else {
					return start.rawField("settings", settings.toString().getBytes());
				}
			})
			// Mappings and overrides
			.andThen(Lambdas.wrap_u(json -> json.startObject("mappings").startObject(type_key)))
			// More mapping overrides
			.andThen(Lambdas.wrap_u(json -> {
				
				return Optional.ofNullable(search_schema)
								.map(ss -> ss.mapping_overrides())
								.map(m -> m.get(type_key))
							.orElse(Collections.emptyMap())
							.entrySet().stream()
							.reduce(json, 
									Lambdas.wrap_u(
											(acc, kv) -> acc.rawField(kv.getKey(), mapper.convertValue(kv.getValue(), JsonNode.class).toString().getBytes())), 
									(acc1, acc2) -> acc1 // (can't actually ever happen)
									)
							;
			}))
			.apply(null);
		}
		catch (IOException e) {
			//Handle fake "IOException"
			return null;
		}
	}

	///////////////////////////////////////////////////////////////
	
	// TEMPLATE CREATION
	
	/** Create a template to be applied to all indexes generated from this bucket
	 * @param bucket
	 * @return
	 */
	public static XContentBuilder getTemplateMapping(final DataBucketBean bucket) {
		try {		
			final XContentBuilder start = XContentFactory.jsonBuilder().startObject()
											.field("template", 
													getBaseIndexName(bucket) + "*"
													);
			
			return start;
		}
		catch (IOException e) {
			//Handle fake "IOException"
			return null;
		}
	}
	
	/** The control method to build up the mapping from the constituent parts
	 * @param bucket
	 * @param field_lookups
	 * @param default_not_analyzed
	 * @param default_analyzed
	 * @param mapper
	 * @return
	 */
	public static XContentBuilder getFullMapping(final DataBucketBean bucket, final ElasticsearchIndexServiceConfigBean config,
			final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> field_lookups,
			final JsonNode default_not_analyzed, final JsonNode default_analyzed,
			final ObjectMapper mapper)
	{
		return Lambdas.wrap_u(__ -> getTemplateMapping(bucket))
				.andThen(json -> getSearchServiceMapping(bucket, config, Optional.of(json), mapper))
				.andThen(json -> getColumnarMapping(bucket, Optional.of(json), field_lookups, default_not_analyzed, default_analyzed, mapper))
				.andThen(Lambdas.wrap_u(json -> json.endObject().endObject())) // (close the objects from the search service mapping)
				.andThen(json -> getTemporalMapping(bucket, Optional.of(json)))
			.apply(null);
	}
	
}
