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
package com.ikanow.aleph2.v1.document_db.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.v1.document_db.utils.JsonNodeBsonUtils.ObjectNodeWrapper;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

public class TestJsonNodeBsonUtils {

	@Test
	public void test_transform() {
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		new JsonNodeBsonUtils(); //coverage!
		
		assertEquals(NullNode.instance, JsonNodeBsonUtils.transform(new HashSet<String>(), JsonNodeFactory.instance));
		assertEquals(null, JsonNodeBsonUtils.transform(null, JsonNodeFactory.instance));
		assertEquals(mapper.convertValue(true, JsonNode.class), JsonNodeBsonUtils.transform(true, JsonNodeFactory.instance));
		assertEquals(mapper.convertValue("test", JsonNode.class), JsonNodeBsonUtils.transform("test", JsonNodeFactory.instance));
		assertEquals(mapper.convertValue(4, JsonNode.class), JsonNodeBsonUtils.transform(4, JsonNodeFactory.instance));
		assertEquals(mapper.convertValue(4L, JsonNode.class), JsonNodeBsonUtils.transform(4L, JsonNodeFactory.instance));
		assertEquals(mapper.convertValue(new byte[] { (byte)0xFF, (byte)0xFE }, JsonNode.class), JsonNodeBsonUtils.transform(new byte[] { (byte)0xFF, (byte)0xFE }, JsonNodeFactory.instance));
		assertEquals(mapper.convertValue(4.0, JsonNode.class), JsonNodeBsonUtils.transform(4.0, JsonNodeFactory.instance));
		assertEquals(mapper.convertValue(0L, JsonNode.class), JsonNodeBsonUtils.transform(new Date(0L), JsonNodeFactory.instance));
		assertEquals(mapper.convertValue("4c927585d591d31d7b37097a", JsonNode.class), JsonNodeBsonUtils.transform(new ObjectId("4c927585d591d31d7b37097a"), JsonNodeFactory.instance));
		//(had real trouble creating a float node!)
		assertEquals(JsonNodeFactory.instance.numberNode(Float.valueOf((float)4.0)), JsonNodeBsonUtils.transform((float)4.0, JsonNodeFactory.instance));
		
		// will test object writable and array writable below		
	}
	
	@Test
	public void test_mapWritableWrapper() {
		final ObjectMapper mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		
		final BasicDBObject m1 = new BasicDBObject();
		
		m1.put("test1", true);
		
		final BasicDBObject m2 = new BasicDBObject();
		m2.put("nested", m1);
		m2.put("test2", "test2");

		final BasicDBList a1 = new BasicDBList();
		a1.add(4); a1.add(5);
		
		final BasicDBList a2 = new BasicDBList();
		a2.add(m1); a2.add(m1);
		
		m2.put("array", a2);
		m1.put("array", a1);
		
		final JsonNode j2 = JsonNodeBsonUtils.from(m2);
		
		assertEquals(3, j2.size());
		
		// Check j's contents
		assertEquals(Stream.of("nested", "test2", "array").sorted().collect(Collectors.toList()), Optionals.streamOf(j2.fieldNames(), false).sorted().collect(Collectors.toList()));
		assertEquals("test2", j2.get("test2").asText());		
		
		final JsonNode j1 = j2.get("nested");
		assertEquals(2, j1.size());
		final JsonNode j1b = JsonNodeBsonUtils.from(m1);
		assertTrue("entrySet wrong: " + j1b.toString(), "{\"test1\":true,\"array\":[4,5]}".equals(j1b.toString()) ||  "{\"array\":[4,5],\"test1\":true}".equals(j1b.toString())); //(tests entrySet)
		final ArrayNode an = mapper.createArrayNode();
		an.add(mapper.convertValue(4, JsonNode.class)); an.add(mapper.convertValue(5, JsonNode.class));
		assertEquals(Arrays.asList(mapper.convertValue(true, JsonNode.class), an), Optionals.streamOf(((ObjectNode)j1).elements(), false).collect(Collectors.toList()));
		
		// OK, now test adding:
		
		assertEquals(2, j1.size());		
		
		final ObjectNode o1 = (ObjectNode) j1;
		o1.put("added", "added_this");

		final ObjectNodeWrapper o1c = (ObjectNodeWrapper) o1;
		assertFalse(o1c.containsKey("not_present"));
		assertTrue(o1c.containsKey("added"));
		assertTrue(o1c.containsKey("test1"));
		
		
		assertEquals(Stream.of("test1", "array", "added").sorted().collect(Collectors.toList()), Optionals.streamOf(j1.fieldNames(), false).sorted().collect(Collectors.toList()));
		assertEquals(Arrays.asList(mapper.convertValue(true, JsonNode.class), an, mapper.convertValue("added_this", JsonNode.class)), 
				Optionals.streamOf(((ObjectNode)j1).elements(), false).collect(Collectors.toList()));
		assertTrue(j1.toString().contains("added_this"));
		assertTrue(j1.toString().contains("4,5"));
				
		assertEquals(mapper.convertValue("added_this", JsonNode.class), j1.get("added"));
		
		assertEquals(3, j1.size());		
		
		// OK now test removing:

		assertEquals(null, o1.remove("not_present"));
		assertEquals(mapper.convertValue(true, JsonNode.class), o1.remove("test1"));
		assertEquals(2, o1.size());		
		ObjectNode o1b = o1.remove(Arrays.asList("added", "array"));
		assertEquals(0, o1.size());		
		assertEquals(0, o1b.size());

		o1.putAll(JsonNodeBsonUtils.from(m1)); // will be minus one object
		assertEquals(2, o1.size());
		assertTrue(o1c.containsValue(mapper.convertValue(true, JsonNode.class)));
		assertFalse(o1c.containsValue("banana"));
		
		
		final ObjectNodeWrapper o2 = (ObjectNodeWrapper) JsonNodeBsonUtils.from(m2);
		assertFalse(o2.isEmpty());
		assertTrue(o2.containsKey("array"));
		assertFalse(o2.containsValue("array"));
		assertTrue(o2.containsValue(mapper.convertValue("test2", JsonNode.class)));
		assertEquals(TextNode.class, o2.remove("test2").getClass());
		assertEquals(2, o2.size());
		o2.removeAll();
		assertEquals(0, o2.size());		
	}
}
