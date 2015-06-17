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
package com.ikanow.aleph2.shared.crud.elasticsearch.utils;

import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;
import java.util.function.Function;

import com.ikanow.aleph2.data_model.utils.Functions;
import com.ikanow.aleph2.data_model.utils.Patterns;

/** Utilities around the ElasticsearchContext ADTs
 * @author Alex
 */
public class ElasticsearchContextUtils {

	/** Utility function to figure out the grouping period based on the index name
	 * @param date_format
	 * @return the date grouping type
	 */
	private static ChronoUnit getIndexGroupingPeriod_slow(String date_format) {
		
		final SimpleDateFormat d = new SimpleDateFormat(date_format);
		final long try_date_boxes[] = { 3601L, 21L*3600L, 8L*24L*3600L, 32L*24L*3600L, 367L*34L*3600L };
		final ChronoUnit ret_date_boxes[] = { ChronoUnit.HOURS, ChronoUnit.DAYS, ChronoUnit.WEEKS, ChronoUnit.MONTHS, ChronoUnit.YEARS };
		final Date now = new Date();
		final String now_string = d.format(now);
		for (int i = 0; i < try_date_boxes.length; ++i) {
			final String test_str = d.format(new Date(now.getTime() + 1000L*try_date_boxes[i]));
			if (!now_string.equals(test_str)) {
				return ret_date_boxes[i];
			}
		}
		return ChronoUnit.FOREVER;
	}	
	/** Memoized version of getIndexGroupingPeriod_slow
	 */
	final public Function<String, ChronoUnit> getIndexGroupingPeriod = Functions.memoize(ElasticsearchContextUtils::getIndexGroupingPeriod_slow);
	
	/** 1-ups an auto type
	 * @param prefix
	 * @param current_type
	 * @return
	 */
	public static String getNextAutoType(final String prefix, final String current_type) {
		return Optional.of(current_type)
				.map(s -> s.substring(prefix.length()))
				.map(ns -> 1 + Integer.parseInt(ns))
				.map(n -> prefix + Integer.toString(n))
				.get();
	}
	
	/** Returns the suffix of a time-based index given the grouping period
	 * @param grouping_period - the grouping period
	 * @return the index suffix, ie added to the base index
	 */
	public static String getIndexSuffix(final ChronoUnit grouping_period) {
		return Patterns.match(grouping_period).<String>andReturn()
				.when(p -> ChronoUnit.SECONDS == p, __ -> "_{YYYY-MM-dd-hh}") // (too granular, just use hours)
				.when(p -> ChronoUnit.MINUTES == p, __ -> "_{YYYY-MM-dd-hh}") // (too granular, just use hours)
				.when(p -> ChronoUnit.HOURS == p, __ -> "_{YYYY-MM-dd-hh}")
				.when(p -> ChronoUnit.DAYS == p, __ -> "_{YYYY-MM-dd}")
				.when(p -> ChronoUnit.WEEKS == p, __ -> "_{YYYY.ww}")
				.when(p -> ChronoUnit.MONTHS == p, __ -> "_{YYYY-MM}")
				.when(p -> ChronoUnit.YEARS == p, __ -> "_{YYYY-MM}")
				.otherwise(__ -> "");
	}
}
