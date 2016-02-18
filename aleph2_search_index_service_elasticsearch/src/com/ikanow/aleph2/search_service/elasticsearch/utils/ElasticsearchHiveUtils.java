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

package com.ikanow.aleph2.search_service.elasticsearch.utils;

import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;

import fj.data.Validation;

/** Utilities for managing the connection to the Hive metastore
 * @author Alex
 */
public class ElasticsearchHiveUtils {

	private static final Set<String> _allowed_types = 
			ImmutableSet.<String>of(
					"TINYINT", "SMALLINT", "INT", "BIGINT", "BOOLEAN", "FLOAT", "DOUBLE", "STRING", "BINARY", "TIMESTAMP", "DECIMAL",
					"DATE", "VARCHAR", "CHAR"
					);
	
	/** https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
	 * @param prefix_string
	 * @param structure
	 * @return
	 */
	public Validation<String, String> generateHiveSchema(final String prefix_string, final JsonNode structure, final boolean top_level) {
		
		return Patterns.match(structure).<Validation<String, String>>andReturn()
			.when(TextNode.class, t -> _allowed_types.contains(t), t -> { //TODO: handle decimal with precision
				return Validation.success(prefix_string + (top_level ? " " : ": ") + t);
			})
			//TODO: ugh this is different between struct and table...
			.when(ObjectNode.class, o -> { // struct, format
				
				return Optionals.streamOf(o.fields(), false)
							.<Validation<String, String>>reduce(
								Validation.success(prefix_string + (top_level ? "(" : "STRUCT<"))
								, 
								(acc, kv) -> {
									return acc.<Validation<String, String>>validation(
											fail -> Validation.fail(fail),
											success -> {
												final String pre_prefix = Lambdas.get(() -> {
													if (success.length() == prefix_string.length()) return "";
													else return ",";
												});
												return generateHiveSchema(pre_prefix + prefix_string + kv.getKey() + " ", kv.getValue(), false);
											}
											)
											;
								}
								,
								(acc1, acc2) -> acc1 // (never called)
								)
								.map(success -> success + (top_level ? ")" : ">"))
							;
			})
			.when(ArrayNode.class, a -> 1 == a.size(), a -> { // array, format [ data_type ]
				return generateHiveSchema(prefix_string + "ARRAY<", a.get(0), false).map(success -> success + ">");
			})
			.when(ArrayNode.class, a -> (a.size() > 1) && a.get(0).isObject(), a -> { // union, format [ {} data_type_1 ... ]
				Optionals.streamOf(a.iterator(), false).skip(1)
							.<Validation<String, String>>reduce(
									Validation.success(prefix_string + "UNIONTYPE<")
									,
									(acc, j) -> {
										return acc.<Validation<String, String>>validation(
												fail -> Validation.fail(fail),
												success -> {
													final String pre_prefix = Lambdas.get(() -> {
														if (success.length() == prefix_string.length()) return "";
														else return ",";
													});
													return generateHiveSchema(pre_prefix + prefix_string + " ", j, false);													
												});										
									}
									,
									(acc1, acc2) -> acc1 // (never called)
									)
									.map(success -> success + ">")
									;
				return null;
			})
			.when(ArrayNode.class, a -> (2 == a.size()) && a.get(0).isTextual(), a -> { // map, format [ key value ]
				return generateHiveSchema(prefix_string + "MAP<", a.get(0), false)
							.bind(success -> generateHiveSchema(success + ", ", a.get(1), false))
							.map(success -> success + ">");
			})
			.otherwise(() -> Validation.fail(ErrorUtils.get("Unrecognized element in schema declaration after {0}: {1}", prefix_string, structure)))
			;		
	}
}
