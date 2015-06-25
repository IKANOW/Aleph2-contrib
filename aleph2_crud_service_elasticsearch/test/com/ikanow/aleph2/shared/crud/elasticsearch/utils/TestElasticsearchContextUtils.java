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

import static org.junit.Assert.*;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.ikanow.aleph2.data_model.utils.Tuples;

public class TestElasticsearchContextUtils {

	@Test
	public void test_timeSplit() {
		assertEquals("test1_test2", ElasticsearchContextUtils.reconstructTimedBasedSplitIndex("test1", "test2"));
		
		assertEquals(Tuples._2T("test1_1", "date-bit-here"),
				ElasticsearchContextUtils.splitTimeBasedIndex("test1_1_{date-bit-here}"));
	}
	
	@Test
	public void test_getNextAutoType() {
		
		assertEquals("test_2", ElasticsearchContextUtils.getNextAutoType("test_", "test_1"));
	}
	
	@Test
	public void test_timePeriods() {
		String s1, s2, s3, s4, s5, s6, s7;
		
		assertEquals("_{yyyy-MM-dd-HH}", s1 = ElasticsearchContextUtils.getIndexSuffix(ChronoUnit.SECONDS));
		assertEquals("_{yyyy-MM-dd-HH}", s2 = ElasticsearchContextUtils.getIndexSuffix(ChronoUnit.MINUTES));
		assertEquals("_{yyyy-MM-dd-HH}", s3 = ElasticsearchContextUtils.getIndexSuffix(ChronoUnit.HOURS));
		assertEquals("_{yyyy-MM-dd}", s4 = ElasticsearchContextUtils.getIndexSuffix(ChronoUnit.DAYS));
		assertEquals("_{YYYY.ww}", s5 = ElasticsearchContextUtils.getIndexSuffix(ChronoUnit.WEEKS));
		assertEquals("_{yyyy-MM}", s6 = ElasticsearchContextUtils.getIndexSuffix(ChronoUnit.MONTHS));
		assertEquals("_{yyyy}", s7 = ElasticsearchContextUtils.getIndexSuffix(ChronoUnit.YEARS));
		assertEquals("", ElasticsearchContextUtils.getIndexSuffix(ChronoUnit.CENTURIES));
		
		assertEquals(ChronoUnit.HOURS, 
				ElasticsearchContextUtils.getIndexGroupingPeriod.apply(s1.substring(2).replace("}", "")));
		assertEquals(ChronoUnit.HOURS, 
				ElasticsearchContextUtils.getIndexGroupingPeriod.apply(s2.substring(2).replace("}", "")));
		assertEquals(ChronoUnit.HOURS, 
				ElasticsearchContextUtils.getIndexGroupingPeriod.apply(s3.substring(2).replace("}", "")));
		assertEquals(ChronoUnit.DAYS, 
				ElasticsearchContextUtils.getIndexGroupingPeriod.apply(s4.substring(2).replace("}", "")));
		assertEquals(ChronoUnit.WEEKS, 
				ElasticsearchContextUtils.getIndexGroupingPeriod.apply(s5.substring(2).replace("}", "")));
		assertEquals(ChronoUnit.MONTHS, 
				ElasticsearchContextUtils.getIndexGroupingPeriod.apply(s6.substring(2).replace("}", "")));
		assertEquals(ChronoUnit.YEARS, 
				ElasticsearchContextUtils.getIndexGroupingPeriod.apply(s7.substring(2).replace("}", "")));
		
	}
	
	@Test
	public void test_IndexesFromDateRange() {
		
		Calendar c1 = GregorianCalendar.getInstance();
		Calendar c2 = GregorianCalendar.getInstance();
		
		c1.set(2000, 5, 1); c2.set(2005,  5, 1);
		final Stream<String> res1 = ElasticsearchContextUtils.getIndexesFromDateRange("test_{yyyy}", 
				Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()));
		
		assertEquals(Arrays.asList("test_2000","test_2001","test_2002","test_2003","test_2004","test_2005"), 
				res1.collect(Collectors.toList()));		
		
		assertEquals(Arrays.asList("test"), ElasticsearchContextUtils.getIndexesFromDateRange("test", 
				Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime())).collect(Collectors.toList()));
		
		c1.set(2015, 5, 1); c2.set(2015, 5, 3);
		final Stream<String> res2 = ElasticsearchContextUtils.getIndexesFromDateRange("test_{yyyy-MM-dd}", 
				Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()));
		
		assertEquals(Arrays.asList("test_2015-06-01","test_2015-06-02","test_2015-06-03"), 
				res2.collect(Collectors.toList()));

		
		c1.set(2004, 11, 28); c2.set(2005,  0, 2);
		final Stream<String> res3 = ElasticsearchContextUtils.getIndexesFromDateRange("test_{yyyy-MM}", 
				Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()));
		
		assertEquals(Arrays.asList("test_2004-12","test_2005-01"), 
				res3.collect(Collectors.toList()));

		final Stream<String> res4 = ElasticsearchContextUtils.getIndexesFromDateRange("test_{YYYY-ww}", 
				Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()));
		
		assertEquals(Arrays.asList("test_2005-01","test_2005-02"), 
				res4.collect(Collectors.toList()));
				
		c1.set(2004, 11, 28, 22, 53, 01); c2.set(2004, 11, 29, 02, 00, 00);
		final Stream<String> res5 = ElasticsearchContextUtils.getIndexesFromDateRange("test_{yyyy-MM-dd-HH}", 
				Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()));
		
		assertEquals(Arrays.asList("test_2004-12-28-22", "test_2004-12-28-23", "test_2004-12-29-00", "test_2004-12-29-01", "test_2004-12-29-02"), 
				res5.collect(Collectors.toList()));
				
		
	}
	
}
