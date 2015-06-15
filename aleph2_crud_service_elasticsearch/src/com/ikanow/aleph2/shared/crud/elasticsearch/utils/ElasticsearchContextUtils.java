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
import java.util.Date;
import java.util.Optional;
import java.util.function.Function;

import com.ikanow.aleph2.data_model.utils.Functions;

/** Utilities around the ElasticsearchContext ADTs
 * @author Alex
 */
public class ElasticsearchContextUtils {

	public enum DateGroupingTypes { HOURLY, DAILY, WEEKLY, MONTHLY, YEARLY, NONE };
	
	/** Utility function to figure out the grouping period based on the index name
	 * @param date_format
	 * @return the date grouping type
	 */
	private static DateGroupingTypes getIndexGroupingPeriod_slow(String date_format) {
		final SimpleDateFormat d = new SimpleDateFormat(date_format);
		final long try_date_boxes[] = { 3601L, 21L*3600L, 8L*24L*3600L, 32L*24L*3600L, 367L*34L*3600L };
		final DateGroupingTypes ret_date_boxes[] = { DateGroupingTypes.HOURLY, DateGroupingTypes.DAILY, DateGroupingTypes.WEEKLY, DateGroupingTypes.MONTHLY, DateGroupingTypes.YEARLY };
		final Date now = new Date();
		final String now_string = d.format(now);
		for (int i = 0; i < try_date_boxes.length; ++i) {
			final String test_str = d.format(new Date(now.getTime() + 1000L*try_date_boxes[i]));
			if (!now_string.equals(test_str)) {
				return ret_date_boxes[i];
			}
		}
		return DateGroupingTypes.NONE;
	}	
	/** Memoized version of getIndexGroupingPeriod_slow
	 */
	final public Function<String, DateGroupingTypes> getIndexGroupingPeriod = Functions.memoize(ElasticsearchContextUtils::getIndexGroupingPeriod_slow);
	
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
	
}
