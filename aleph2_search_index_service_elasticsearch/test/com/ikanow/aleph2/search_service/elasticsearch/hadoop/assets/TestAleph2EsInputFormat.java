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
package com.ikanow.aleph2.search_service.elasticsearch.hadoop.assets;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.search_service.elasticsearch.hadoop.assets.Aleph2EsInputFormat.Aleph2EsRecordReader;

public class TestAleph2EsInputFormat {

	@Test
	public void test_Aleph2EsInputFormat() {

		new Aleph2EsInputFormat(); //code coverage!
		
		final JobContext mock_context = Mockito.mock(JobContext.class);
				
		final Configuration test_config = new Configuration(false);
		
		test_config.set("aleph2.es.resource", "test1,,test ,2");
		
		Mockito.when(mock_context.getConfiguration()).thenReturn(test_config);
		
		final List<InputSplit> res = Aleph2EsInputFormat.getSplits(ctxt -> {
			final InputSplit split1 = Mockito.mock(InputSplit.class);
			final String es_res = ctxt.getConfiguration().get("es.resource.read").toString();
			
			Mockito.when(split1.toString()).thenReturn(es_res);
			return Arrays.asList(split1);
		}, mock_context);
		
		assertEquals(Arrays.asList("test1", "test%20,2"), res.stream().map(fmt -> fmt.toString()).collect(Collectors.toList()));
	}
	
	@Test
	public void test_Aleph2EsRecordReader_objectConversion() throws IOException, InterruptedException {
		
		@SuppressWarnings("rawtypes")
		final RecordReader mock_shard_record_reader = Mockito.mock(RecordReader.class);

		// mock returns Text key, MapWritable value
		Mockito.when(mock_shard_record_reader.getCurrentKey()).thenReturn(new Text("text_test"));
		
		final MapWritable test_out = new MapWritable();
		test_out.put(new Text("val_key_text"), new Text("val_val_text"));
		
		Mockito.when(mock_shard_record_reader.getCurrentValue()).thenReturn(test_out);
		
		final Aleph2EsRecordReader reader_under_test = new Aleph2EsRecordReader(mock_shard_record_reader);
		
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

	@Test
	public void test_Aleph2EsRecordReader_maxRecords() throws IOException, InterruptedException {

		@SuppressWarnings("rawtypes")
		final RecordReader mock_shard_record_reader = Mockito.mock(RecordReader.class);
		Mockito.when(mock_shard_record_reader.nextKeyValue()).thenReturn(true); // (ie will keep going forever)
		Mockito.when(mock_shard_record_reader.getProgress()).thenReturn((float)4.0); // (just return some dummy number so we can check it's working)

		// Test version
		{
			final Configuration config = new Configuration(false);
			config.set(Aleph2EsInputFormat.BE_DEBUG_MAX_SIZE, "10");
			final TaskAttemptContext mock_task = Mockito.mock(TaskAttemptContext.class);
			Mockito.when(mock_task.getConfiguration()).thenReturn(config);
			
			final Aleph2EsRecordReader reader_under_test = new Aleph2EsRecordReader(mock_shard_record_reader);
			
			try {
				 reader_under_test.initialize(null, mock_task);
			}
			catch (Exception e) {} // (the _delegate init call will fail out, that's fine)
			
			int ii = 0;
			for (; ii < 100 && reader_under_test.nextKeyValue(); ++ii) {
				assertTrue("getProgress should be overridden", reader_under_test.getProgress() <= 1.0);
			}
			assertEquals("Should have stopped after 10 iterations", 10, ii);
		}
		// Normal version
		{
			final Configuration config = new Configuration(false);
			final TaskAttemptContext mock_task = Mockito.mock(TaskAttemptContext.class);
			Mockito.when(mock_task.getConfiguration()).thenReturn(config);
			
			final Aleph2EsRecordReader reader_under_test = new Aleph2EsRecordReader(mock_shard_record_reader);
			
			try {
				 reader_under_test.initialize(null, mock_task);
			}
			catch (Exception e) {} // (the _delegate init call will fail out, that's fine)
			
			int ii = 0;
			for (; ii < 100 && reader_under_test.nextKeyValue(); ++ii) {
				assertTrue("getProgress should return the dummy value", reader_under_test.getProgress() == 4.0);
			}
			assertEquals("Should keep going for all 100 iterations", 100, ii);
		}
	}	
	
	@Test
	public void test_Aleph2EsRecordReader_testCoverage() throws IOException, InterruptedException {
		
		@SuppressWarnings("rawtypes")
		final RecordReader mock_shard_record_reader = Mockito.mock(RecordReader.class, new Answer<Void>() {
		      public Void answer(InvocationOnMock invocation) {
		    	  //String fn_name = invocation.getMethod().getName();
		    	  return null;
		      }
		});
		Mockito.when(mock_shard_record_reader.getProgress()).thenReturn((float)1.0);
		Mockito.when(mock_shard_record_reader.nextKeyValue()).thenReturn(true);
		
		final Aleph2EsRecordReader reader_under_test = new Aleph2EsRecordReader(mock_shard_record_reader);

		// void Functions we don't care about as long as they don't die
		
		reader_under_test.close();
		
		// Functions that return something that we can pass along directly
		
		assertEquals((float)1.0, (float)reader_under_test.getProgress(), 0.00001);
		assertEquals(true, reader_under_test.nextKeyValue());
		
		// Things that throw exceptions
		
		try {
			reader_under_test.createKey();
			fail("should have thrown exception");
		}
		catch (Exception e) {}

		try {
			reader_under_test.createValue();			
			fail("should have thrown exception");
		}
		catch (Exception e) {}

		try {
			reader_under_test.setCurrentKey("str", "str");						
			fail("should have thrown exception");
		}
		catch (Exception e) {}
		
		try {
			reader_under_test.setCurrentValue(null, "str");									
			fail("should have thrown exception");
		}
		catch (Exception e) {}
	}
	
}
