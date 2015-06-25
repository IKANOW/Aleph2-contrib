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
package com.ikanow.aleph2.shared.crud.elasticsearch.data_model;

import static org.junit.Assert.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Test;

import scala.Tuple2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.shared.crud.elasticsearch.utils.ElasticsearchContextUtils;

@SuppressWarnings("unused")
public class TestElasticsearchContext {

	@Test
	public void test_context() throws JsonProcessingException, IOException {
		
		// Just going to test the non-trivial logic:
		
		final ElasticsearchContext.IndexContext.ReadOnlyIndexContext.TimedRoIndexContext index_context_1 = 
				new ElasticsearchContext.IndexContext.ReadOnlyIndexContext.TimedRoIndexContext(Arrays.asList("test1_{yyyy}", "test_2_{yyyy.MM}", "test3"));
		
		assertEquals(Arrays.asList("test1_*", "test_2_*", "test3"), index_context_1.getReadableIndexList(Optional.empty()));
		
		final Calendar c1 = GregorianCalendar.getInstance();
		final Calendar c2 = GregorianCalendar.getInstance();		
		c1.set(2004, 11, 28); c2.set(2005,  0, 2);
		assertEquals(Arrays.asList("test1_2004*", "test1_2005*", "test_2_2004.12*", "test_2_2005.01*", "test3*"), 
				index_context_1.getReadableIndexList(Optional.of(Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()))));

		
		final ElasticsearchContext.IndexContext.ReadWriteIndexContext.TimedRwIndexContext index_context_2 = 
				new ElasticsearchContext.IndexContext.ReadWriteIndexContext.TimedRwIndexContext("test1_{yyyy}", Optional.of("@timestamp"));
		
		final ElasticsearchContext.IndexContext.ReadWriteIndexContext.TimedRwIndexContext index_context_3 = 
				new ElasticsearchContext.IndexContext.ReadWriteIndexContext.TimedRwIndexContext("test_2_{yyyy.MM}", Optional.empty());

		final ElasticsearchContext.IndexContext.ReadWriteIndexContext.TimedRwIndexContext index_context_4 = 
				new ElasticsearchContext.IndexContext.ReadWriteIndexContext.TimedRwIndexContext("test3", Optional.of("@timestamp"));
		
		assertEquals(Arrays.asList("test1_2004*", "test1_2005*"), index_context_2.getReadableIndexList(Optional.of(Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()))));
		assertEquals(Arrays.asList("test_2_2004.12*", "test_2_2005.01*"), index_context_3.getReadableIndexList(Optional.of(Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()))));
		assertEquals(Arrays.asList("test3*"), index_context_4.getReadableIndexList(Optional.of(Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()))));

		// Check gets the right timestamp when writing objects into an index
		
		c1.set(2004, 11, 28, 12, 00, 01);
		final ObjectNode obj = ((ObjectNode)BeanTemplateUtils.configureMapper
									(Optional.empty()).readTree("{\"val\":\"test1\"}".getBytes()))
										.put("@timestamp", c1.getTime().getTime());
		
		final String expected3 = new SimpleDateFormat("yyyy.MM").format(new Date());
		
		assertEquals("test1_2004", index_context_2.getWritableIndex(Optional.of(obj)));
		assertEquals("test_2_" + expected3, index_context_3.getWritableIndex(Optional.of(obj)));
		assertEquals("test3", index_context_4.getWritableIndex(Optional.of(obj)));
	}
}
