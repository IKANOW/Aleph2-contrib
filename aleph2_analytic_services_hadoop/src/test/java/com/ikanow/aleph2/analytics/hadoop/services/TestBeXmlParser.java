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

package com.ikanow.aleph2.analytics.hadoop.services;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import scala.Tuple2;

import com.ikanow.aleph2.analytics.hadoop.data_model.BeFileInputConfigBean;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

/**
 * @author Alex
 *
 */
public class TestBeXmlParser {

	@Test
	public void test_basicXmlParsing() {
		
		// Totally default fields
		{
			final BeFileInputConfigBean.XML xml = 
					BeanTemplateUtils.build(BeFileInputConfigBean.XML.class)
					.done().get();
			
			final BeXmlParser parser = new BeXmlParser(xml);
			
			final String test_string = 
					Stream.of(
							"<test attr1=\"test_attr1\"><field1>test_field1</field1></test>"
							)
							.collect(Collectors.joining("\n"));
			
			final InputStream xml_stream = new ByteArrayInputStream(test_string.getBytes(StandardCharsets.UTF_8));
			
			final Tuple2<Long, IBatchRecord> record = parser.getNextRecord(0, "my_filename", xml_stream);
			assertTrue("EOF'd", null != record);
			assertEquals("{\"field1\":\"test_field1\"}", record._2().getJson().toString());
			final Tuple2<Long, IBatchRecord> record2 = parser.getNextRecord(0, "my_filename", xml_stream);
			assertTrue("!EOF'd", null == record2);
		}
		// Attributes enabled
		{
			final BeFileInputConfigBean.XML xml = 
					BeanTemplateUtils.build(BeFileInputConfigBean.XML.class)
						.with(BeFileInputConfigBean.XML::attribute_prefix, "")
					.done().get();
			
			final BeXmlParser parser = new BeXmlParser(xml);
			
			final String test_string = 
					Stream.of(
							"<test attr1=\"test_attr1\"><field1>test_field1</field1></test>"
							)
							.collect(Collectors.joining("\n"));
			
			final InputStream xml_stream = new ByteArrayInputStream(test_string.getBytes(StandardCharsets.UTF_8));
			
			final Tuple2<Long, IBatchRecord> record = parser.getNextRecord(0, "my_filename", xml_stream);
			assertTrue("EOF'd", null != record);
			assertEquals("{\"attr1\":\"test_attr1\",\"field1\":\"test_field1\"}", record._2().getJson().toString());
		}
		// Nested root
		{
			final BeFileInputConfigBean.XML xml = 
					BeanTemplateUtils.build(BeFileInputConfigBean.XML.class)
						.with(BeFileInputConfigBean.XML::root_fields, Arrays.asList("test"))
						.with(BeFileInputConfigBean.XML::attribute_prefix, "")
					.done().get();
			
			final BeXmlParser parser = new BeXmlParser(xml);
			
			final String test_string = 
					Stream.of(
							"<nested1><nested2><test attr1=\"test_attr1\"><field1>test_field1</field1></test></nested1></nested2>"
							)
							.collect(Collectors.joining("\n"));
			
			final InputStream xml_stream = new ByteArrayInputStream(test_string.getBytes(StandardCharsets.UTF_8));
			
			final Tuple2<Long, IBatchRecord> record = parser.getNextRecord(0, "my_filename", xml_stream);
			assertTrue("EOF'd", null != record);
			assertEquals("{\"attr1\":\"test_attr1\",\"field1\":\"test_field1\"}", record._2().getJson().toString());
		}
		// Multiple fields (different roots)
		{
			final BeFileInputConfigBean.XML xml = 
					BeanTemplateUtils.build(BeFileInputConfigBean.XML.class)
						.with(BeFileInputConfigBean.XML::root_fields, Arrays.asList("test1", "test2"))
						.with(BeFileInputConfigBean.XML::attribute_prefix, "")
					.done().get();
			
			final BeXmlParser parser = new BeXmlParser(xml);
			
			final String test_string = 
					Stream.of(
							"<array>",
							"<test1 attr1=\"test_attr1\"><field1>test_field1</field1></test1>",
							"<test attr1=\"test_attr\"><field1>test_field</field1></test>",
							"<test2 attr1=\"test_attr2\"><field1>test_field2</field1></test2>",
							"</array>"
							)
							.collect(Collectors.joining("\n"));
			
			final InputStream xml_stream = new ByteArrayInputStream(test_string.getBytes(StandardCharsets.UTF_8));

			{
				final Tuple2<Long, IBatchRecord> record = parser.getNextRecord(0, "my_filename", xml_stream);
				assertTrue("EOF'd", null != record);
				assertEquals("{\"attr1\":\"test_attr1\",\"field1\":\"test_field1\"}", record._2().getJson().toString());
			}
			{
				final Tuple2<Long, IBatchRecord> record = parser.getNextRecord(0, "my_filename", xml_stream);
				assertTrue("EOF'd", null != record);
				assertEquals("{\"attr1\":\"test_attr2\",\"field1\":\"test_field2\"}", record._2().getJson().toString());
			}			
			{
				final Tuple2<Long, IBatchRecord> record2 = parser.getNextRecord(0, "my_filename", xml_stream);
				assertTrue("!EOF'd", null == record2);
			}			
		}
		// Multiple fields (different roots) - new line separated version - NOT SUPPORTED
		{
			@SuppressWarnings("unused")
			final String test_string = 
					Stream.of(
							"<test1 attr1=\"test_attr1\"><field1>test_field1</field1></test1>",
							"<test attr1=\"test_attr\"><field1>test_field</field1></test>",
							"<test2 attr1=\"test_attr2\"><field1>test_field2</field1></test2>"
							)
							.collect(Collectors.joining("\n"));
		}
		// Ignore field
		{
			final BeFileInputConfigBean.XML xml = 
					BeanTemplateUtils.build(BeFileInputConfigBean.XML.class)
						.with(BeFileInputConfigBean.XML::preserve_case, false)
						.with(BeFileInputConfigBean.XML::ignore_fields, Arrays.asList("ignore_field"))
					.done().get();
			
			final BeXmlParser parser = new BeXmlParser(xml);
			
			final String test_string = 
					Stream.of(
							"<Test Attr1=\"test_attr1\"><ignore_field><Field1>test_field1</Field1></ignore_field></Test>"
							)
							.collect(Collectors.joining("\n"));
			
			final InputStream xml_stream = new ByteArrayInputStream(test_string.getBytes(StandardCharsets.UTF_8));
			
			final Tuple2<Long, IBatchRecord> record = parser.getNextRecord(0, "my_filename", xml_stream);
			assertTrue("EOF'd", null != record);
			assertEquals("{\"field1\":\"test_field1\"}", record._2().getJson().toString());
			final Tuple2<Long, IBatchRecord> record2 = parser.getNextRecord(0, "my_filename", xml_stream);
			assertTrue("!EOF'd", null == record2);
		}
		// Ignore field, include message
		{
			final BeFileInputConfigBean.XML xml = 
					BeanTemplateUtils.build(BeFileInputConfigBean.XML.class)
						.with(BeFileInputConfigBean.XML::root_fields, Arrays.asList("Test"))
						.with(BeFileInputConfigBean.XML::xml_text_field, "message")
						.with(BeFileInputConfigBean.XML::ignore_fields, Arrays.asList("ignore_field"))
					.done().get();
			
			final BeXmlParser parser = new BeXmlParser(xml);
			
			final String test_string = 
					Stream.of(
							"<list><Test Attr1=\"test_attr1\"><ignore_field><Field1>test_field1</Field1></ignore_field></Test></list>"
							)
							.collect(Collectors.joining("\n"));
			
			final InputStream xml_stream = new ByteArrayInputStream(test_string.getBytes(StandardCharsets.UTF_8));
			
			final Tuple2<Long, IBatchRecord> record = parser.getNextRecord(0, "my_filename", xml_stream);
			assertTrue("EOF'd", null != record);
			assertEquals("{\"Field1\":\"test_field1\",\"message\":\"<Test Attr1=\\\"test_attr1\\\"><ignore_field><Field1>test_field1</Field1></ignore_field></Test>\"}", record._2().getJson().toString());
			final Tuple2<Long, IBatchRecord> record2 = parser.getNextRecord(0, "my_filename", xml_stream);
			assertTrue("!EOF'd", null == record2);
		}
		// _id from fields
		{
			final BeFileInputConfigBean.XML xml = 
					BeanTemplateUtils.build(BeFileInputConfigBean.XML.class)
						.with(BeFileInputConfigBean.XML::primary_key_field, "field1")
						.with(BeFileInputConfigBean.XML::id_field, "url")
						.with(BeFileInputConfigBean.XML::primary_key_prefix, "prefix:")
					.done().get();
			
			final BeXmlParser parser = new BeXmlParser(xml);
			
			final String test_string = 
					Stream.of(
							"<test attr1=\"test_attr1\" xmlns:h=\"DUMMY_URL\"><h:field1>test_field1</h:field1></test>"
							)
							.collect(Collectors.joining("\n"));
			
			final InputStream xml_stream = new ByteArrayInputStream(test_string.getBytes(StandardCharsets.UTF_8));
			
			final Tuple2<Long, IBatchRecord> record = parser.getNextRecord(0, "my_filename", xml_stream);
			assertTrue("EOF'd", null != record);
			assertEquals("{\"field1\":\"test_field1\",\"url\":\"prefix:test_field1\"}", record._2().getJson().toString());
			final Tuple2<Long, IBatchRecord> record2 = parser.getNextRecord(0, "my_filename", xml_stream);
			assertTrue("!EOF'd", null == record2);
		}
		//add _id from content
		{
			final BeFileInputConfigBean.XML xml = 
					BeanTemplateUtils.build(BeFileInputConfigBean.XML.class)
						.with(BeFileInputConfigBean.XML::set_id_from_content, true)
						.with(BeFileInputConfigBean.XML::primary_key_field, "field1")
						.with(BeFileInputConfigBean.XML::primary_key_prefix, "prefix:")
					.done().get();
			
			final BeXmlParser parser = new BeXmlParser(xml);
			
			final String test_string = 
					Stream.of(
							"<test attr1=\"test_attr1\"><field1>test_field1</field1></test>"
							)
							.collect(Collectors.joining("\n"));
			
			final InputStream xml_stream = new ByteArrayInputStream(test_string.getBytes(StandardCharsets.UTF_8));
			
			final Tuple2<Long, IBatchRecord> record = parser.getNextRecord(0, "my_filename", xml_stream);
			assertTrue("EOF'd", null != record);
			assertEquals("{\"field1\":\"test_field1\",\"_id\":\"c7338655-ad4e-38cc-9b1d-2850f38ea673\"}", record._2().getJson().toString());
			final Tuple2<Long, IBatchRecord> record2 = parser.getNextRecord(0, "my_filename", xml_stream);
			assertTrue("!EOF'd", null == record2);
		}
		// Prefix mode
		{
			final BeFileInputConfigBean.XML xml = 
					BeanTemplateUtils.build(BeFileInputConfigBean.XML.class)
						.with(BeFileInputConfigBean.XML::root_fields, Arrays.asList("h:test"))
						.with(BeFileInputConfigBean.XML::primary_key_field, "h:field1")
						.with(BeFileInputConfigBean.XML::id_field, "url")
						.with(BeFileInputConfigBean.XML::primary_key_prefix, "prefix:")
					.done().get();
			
			final BeXmlParser parser = new BeXmlParser(xml);
			
			final String test_string = 
					Stream.of(
							"<h:test attr1=\"test_attr1\" xmlns:h=\"DUMMY_URL\"><h:field1>test_field1</h:field1></h:test>"
							)
							.collect(Collectors.joining("\n"));
			
			final InputStream xml_stream = new ByteArrayInputStream(test_string.getBytes(StandardCharsets.UTF_8));
			
			final Tuple2<Long, IBatchRecord> record = parser.getNextRecord(0, "my_filename", xml_stream);
			assertTrue("EOF'd", null != record);
			assertEquals("{\"field1\":\"test_field1\",\"url\":\"prefix:test_field1\"}", record._2().getJson().toString());
			final Tuple2<Long, IBatchRecord> record2 = parser.getNextRecord(0, "my_filename", xml_stream);
			assertTrue("!EOF'd", null == record2);
		}
	}
}
