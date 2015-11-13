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
 *******************************************************************************/
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
import com.ikanow.aleph2.shared.crud.elasticsearch.services.MockElasticsearchCrudServiceFactory;
import com.ikanow.aleph2.shared.crud.elasticsearch.utils.ElasticsearchContextUtils;

import fj.data.Either;

@SuppressWarnings("unused")
public class TestElasticsearchContext {

	@Test
	public void test_context() throws JsonProcessingException, IOException {
		
		final Calendar c1 = GregorianCalendar.getInstance();
		final Calendar c2 = GregorianCalendar.getInstance();
		final Calendar cnow = GregorianCalendar.getInstance();
		
		// Just going to test the non-trivial logic:
		{
			final ElasticsearchContext.IndexContext.ReadOnlyIndexContext.TimedRoIndexContext index_context_1 = 
					new ElasticsearchContext.IndexContext.ReadOnlyIndexContext.TimedRoIndexContext(Arrays.asList("test1_{yyyy}", "test_2_{yyyy.MM}", "test3__MYID"));
			
			assertEquals(Arrays.asList("test1*", "test_2*", "test3__MYID*"), index_context_1.getReadableIndexList(Optional.empty()));
			
			c1.set(2004, 11, 28); c2.set(2005,  0, 2);
			assertEquals(Arrays.asList("test1_2004*", "test1_2005*", "test_2_2004.12*", "test_2_2005.01*", "test3__MYID*"), 
					index_context_1.getReadableIndexList(Optional.of(Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()))));
		}
		
		// Check get an invalid entry if the index list is empty
		{
			final ElasticsearchContext.IndexContext.ReadOnlyIndexContext.TimedRoIndexContext index_context_1 = 
					new ElasticsearchContext.IndexContext.ReadOnlyIndexContext.TimedRoIndexContext(Arrays.asList());
			
			assertEquals(Arrays.asList(ElasticsearchContext.NO_INDEX_FOUND), index_context_1.getReadableIndexList(Optional.empty()));
		}
		
		// Some timestamp testing
		{
			final ElasticsearchContext.IndexContext.ReadWriteIndexContext.TimedRwIndexContext index_context_2 = 
					new ElasticsearchContext.IndexContext.ReadWriteIndexContext.TimedRwIndexContext("test1_{yyyy}", Optional.of("@timestamp"), Optional.empty(), Either.left(true));
			
			assertTrue("timestamp field present", index_context_2.timeField().isPresent());
			assertEquals("@timestamp", index_context_2.timeField().get());
			
			final ElasticsearchContext.IndexContext.ReadWriteIndexContext.TimedRwIndexContext index_context_3 = 
					new ElasticsearchContext.IndexContext.ReadWriteIndexContext.TimedRwIndexContext("test_2_{yyyy.MM}", Optional.empty(), Optional.empty(), Either.left(true));		

			assertFalse("no timestamp field", index_context_3.timeField().isPresent());
			
			cnow.setTime(new Date());
			int month = cnow.get(Calendar.MONTH) + 1;
			assertEquals("test_2_" + cnow.get(Calendar.YEAR) + "." + ((month < 10) ? ("0" + month) : (month)), 
					index_context_3.getWritableIndex(Optional.of(BeanTemplateUtils.configureMapper(Optional.empty()).createObjectNode())));
			
			// This isn't supported any more, valid strings only
//			final ElasticsearchContext.IndexContext.ReadWriteIndexContext.TimedRwIndexContext index_context_4 = 
//					new ElasticsearchContext.IndexContext.ReadWriteIndexContext.TimedRwIndexContext("test3", Optional.of("@timestamp"));
			
			assertEquals(Arrays.asList("test1*"), index_context_2.getReadableIndexList(Optional.empty()));
			assertEquals(Arrays.asList("test_2*"), index_context_3.getReadableIndexList(Optional.empty()));
			
			assertEquals(Arrays.asList("test1_2004*", "test1_2005*"), index_context_2.getReadableIndexList(Optional.of(Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()))));
			assertEquals(Arrays.asList("test_2_2004.12*", "test_2_2005.01*"), index_context_3.getReadableIndexList(Optional.of(Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()))));
			// (see index_context_4 declaration, above)
//			assertEquals(Arrays.asList("test3*"), index_context_4.getReadableIndexList(Optional.of(Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()))));
		
		// Check gets the right timestamp when writing objects into an index
		
			c1.set(2004, 11, 28, 12, 00, 01);
			final ObjectNode obj = ((ObjectNode)BeanTemplateUtils.configureMapper
										(Optional.empty()).readTree("{\"val\":\"test1\"}".getBytes()))
											.put("@timestamp", c1.getTime().getTime());
			
			final String expected3 = new SimpleDateFormat("yyyy.MM").format(new Date());
			
			assertEquals("test1_2004", index_context_2.getWritableIndex(Optional.of(obj)));
			assertEquals("test_2_" + expected3, index_context_3.getWritableIndex(Optional.of(obj)));
			// (see index_context_4 declaration, above)
//			assertEquals("test3", index_context_4.getWritableIndex(Optional.of(obj)));
		}
		
		// Test readable index differences depending on whether the max index size is set or not
		
		{
			final ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext index_context_1 = 
					new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("test1", Optional.empty(), Either.left(true));
			
			assertEquals(Arrays.asList("test1"), index_context_1.getReadableIndexList(Optional.empty()));
			
			final ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext index_context_2 = 
					new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("test2", Optional.of(-1L), Either.left(true));
			
			assertEquals(Arrays.asList("test2"), index_context_2.getReadableIndexList(Optional.empty()));
			
			final ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext index_context_3 = 
					new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("test3", Optional.of(0L), Either.left(true));
			
			assertEquals(Arrays.asList("test3*"), index_context_3.getReadableIndexList(Optional.empty()));
			
			final ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext index_context_4 = 
					new ElasticsearchContext.IndexContext.ReadWriteIndexContext.FixedRwIndexContext("test4", Optional.of(10L), Either.left(true));
			
			assertEquals(Arrays.asList("test4*"), index_context_4.getReadableIndexList(Optional.empty()));
		}		
	}
	
	@Test
	public void test_mixedContext() {
		
		final Calendar c1 = GregorianCalendar.getInstance();
		final Calendar c2 = GregorianCalendar.getInstance();
		final Calendar cnow = GregorianCalendar.getInstance();
		
		{
			final ElasticsearchContext.IndexContext.ReadOnlyIndexContext.MixedRoIndexContext index_context_1 =
					new ElasticsearchContext.IndexContext.ReadOnlyIndexContext.MixedRoIndexContext(
							Arrays.asList("test1_{yyyy}", "test_2_{yyyy.MM}", "test3"),
							Arrays.asList("fixed_{yyyy}", "fixed")
							);
			
			c1.set(2004, 11, 28); c2.set(2005,  0, 2);
			assertEquals(Arrays.asList("test1_2004*", "test1_2005*", "test_2_2004.12*", "test_2_2005.01*", "test3*", "fixed_{yyyy}*", "fixed*"), 
					index_context_1.getReadableIndexList(Optional.of(Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()))));
		}
		
		// Also check that if the indexes are empty it works:
		
		{
			final ElasticsearchContext.IndexContext.ReadOnlyIndexContext.MixedRoIndexContext index_context_2 =
					new ElasticsearchContext.IndexContext.ReadOnlyIndexContext.MixedRoIndexContext(
							Arrays.asList(),
							Arrays.asList("fixed_{yyyy}", "fixed")
							);
			
			c1.set(2004, 11, 28); c2.set(2005,  0, 2);
			assertEquals(Arrays.asList("fixed_{yyyy}*", "fixed*"), 
					index_context_2.getReadableIndexList(Optional.of(Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()))));
		}
		{
			final ElasticsearchContext.IndexContext.ReadOnlyIndexContext.MixedRoIndexContext index_context_2 =
					new ElasticsearchContext.IndexContext.ReadOnlyIndexContext.MixedRoIndexContext(
							Arrays.asList("test1_{yyyy}", "test_2_{yyyy.MM}", "test3"),
							Arrays.asList()
							);
			
			c1.set(2004, 11, 28); c2.set(2005,  0, 2);
			assertEquals(Arrays.asList("test1_2004*", "test1_2005*", "test_2_2004.12*", "test_2_2005.01*", "test3*"), 
					index_context_2.getReadableIndexList(Optional.of(Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()))));
		}
		{
			final ElasticsearchContext.IndexContext.ReadOnlyIndexContext.MixedRoIndexContext index_context_2 =
					new ElasticsearchContext.IndexContext.ReadOnlyIndexContext.MixedRoIndexContext(
							Arrays.asList(),
							Arrays.asList()
							);
			
			c1.set(2004, 11, 28); c2.set(2005,  0, 2);
			assertEquals(Arrays.asList(ElasticsearchContext.NO_INDEX_FOUND), 
					index_context_2.getReadableIndexList(Optional.of(Tuples._2T(c1.getTime().getTime(), c2.getTime().getTime()))));
		}
		
	}
		
	
	@Test
	public void test_contextContainers() {
		
		final MockElasticsearchCrudServiceFactory factory = new MockElasticsearchCrudServiceFactory();
		
		// Some misc coverage:
		{
			final ElasticsearchContext.IndexContext.ReadOnlyIndexContext.FixedRoIndexContext test_ro_index = 
					new ElasticsearchContext.IndexContext.ReadOnlyIndexContext.FixedRoIndexContext(Arrays.asList("test_ro_1", "test_ro_2"));
			
			assertEquals(Arrays.asList("test_ro_1*", "test_ro_2*"), test_ro_index.getReadableIndexList(Optional.empty()));
			
			final ElasticsearchContext.TypeContext.ReadOnlyTypeContext.FixedRoTypeContext test_ro_type = 
					new ElasticsearchContext.TypeContext.ReadOnlyTypeContext.FixedRoTypeContext(Arrays.asList("test_context_1", "test_context_2"));
			
			assertEquals(Arrays.asList("test_context_1", "test_context_2"), test_ro_type.getReadableTypeList());
			
			final ElasticsearchContext.TypeContext.ReadOnlyTypeContext.FixedRoTypeContext test_ro_auto_type = 
					new ElasticsearchContext.TypeContext.ReadOnlyTypeContext.FixedRoTypeContext(Arrays.asList("test_context_1b", "test_context_2b"));
			
			assertEquals(Arrays.asList("test_context_1b", "test_context_2b"), test_ro_auto_type.getReadableTypeList());
			
			final ElasticsearchContext.ReadOnlyContext test = new ElasticsearchContext.ReadOnlyContext(factory.getClient(), test_ro_index, test_ro_type);
					
			assertEquals(test_ro_index, test.indexContext());
			assertEquals(test_ro_type, test.typeContext());
		}
		
		// Check get an invalid entry if the index list is empty
		{
			final ElasticsearchContext.IndexContext.ReadOnlyIndexContext.FixedRoIndexContext index_context_1 = 
					new ElasticsearchContext.IndexContext.ReadOnlyIndexContext.FixedRoIndexContext(Arrays.asList());
			
			assertEquals(Arrays.asList(ElasticsearchContext.NO_INDEX_FOUND), index_context_1.getReadableIndexList(Optional.empty()));
		}
		
	}
	
	// (Other code is covered by TestElasticsearchCrudService - we'll live with that for now)

	// (In particular, the code for testing the max size is living in TestElasticsearchCrudService, since that's where all the code for inserting docs etc lives)
	
}
