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
package com.ikanow.aleph2.v1.document_db.hadoop.assets;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.mapreduce.RecordReader;
import org.bson.BSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.v1.document_db.hadoop.assets.Aleph2V1InputFormat.V1DocumentDbRecordReader;
import com.mongodb.BasicDBObject;

public class TestAleph2V1InputFormat {

	@Test
	public void test_Aleph2V1InputFormat() {

		final Aleph2V1InputFormat to_test = new Aleph2V1InputFormat();

		// (basically just coverage testing - impossible to make this not exception)
		try {
			to_test.getSplits(null);
			fail("Should exception");
		}
		catch (Exception e) {
		}
		
		// (basically just coverage testing)
		try {
			to_test.createRecordReader(null, null);
			fail("Should exception");
		}
		catch (IllegalStateException e) {} // (otherwise fail)		
	}
	
	@Test
	public void test_V1DocumentDbRecordReader_objectConversion() throws IOException, InterruptedException {
		
		@SuppressWarnings("unchecked")
		final RecordReader<Object, BSONObject> mock_record_reader = (RecordReader<Object, BSONObject>) Mockito.mock(RecordReader.class);
		Mockito.when(mock_record_reader.getCurrentKey()).thenReturn("text_test");
		final BasicDBObject test_ret = new BasicDBObject();
		test_ret.put("val_key_text", "val_val_text");
		Mockito.when(mock_record_reader.getCurrentValue()).thenReturn(test_ret);
		
		try (final V1DocumentDbRecordReader reader_under_test = new V1DocumentDbRecordReader(mock_record_reader)) {
			
			final String key = reader_under_test.getCurrentKey();
			assertEquals(String.class, key.getClass());
			assertEquals("text_test", key);
			
			final Tuple2<Long, IBatchRecord> value = reader_under_test.getCurrentValue();
			assertEquals(0L, value._1().longValue()); // (so something breaks in here when/if we put some logic in)
			assertEquals(Optional.empty(), value._2().getContent());
			final JsonNode json_val = value._2().getJson();
			assertTrue("Is object: " + json_val, json_val.isObject());
			assertEquals("val_val_text", json_val.get("val_key_text").asText());
		}
	}

	@Test
	public void test_V1DocumentDbRecordReader_testCoverage() throws IOException, InterruptedException {
		
		@SuppressWarnings("rawtypes")
		final RecordReader mock_shard_record_reader = Mockito.mock(RecordReader.class, new Answer<Void>() {
		      public Void answer(InvocationOnMock invocation) {
		    	  //String fn_name = invocation.getMethod().getName();
		    	  return null;
		      }
		});
		Mockito.when(mock_shard_record_reader.getProgress()).thenReturn((float)1.0);
		Mockito.when(mock_shard_record_reader.nextKeyValue()).thenReturn(true);
		
		@SuppressWarnings("unchecked")
		final V1DocumentDbRecordReader reader_under_test = new V1DocumentDbRecordReader(mock_shard_record_reader);

		// void Functions we don't care about as long as they don't die
		
		reader_under_test.close();
		
		// Functions that return something that we can pass along directly
		
		assertEquals((float)1.0, (float)reader_under_test.getProgress(), 0.00001);
		assertEquals(true, reader_under_test.nextKeyValue());
		
		// (basically just coverage testing)
		try {
			reader_under_test.initialize(null, null);
			//(this one doesn't exception for some reason)
		}
		catch (Exception e) {}		
	}
}
