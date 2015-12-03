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

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBObject;

/** Utilities for creating JsonNodes lazily from MapWritables
 * @author Alex
 *
 */
public class JsonNodeBsonUtils {

	/** Creates a lazy object node from a MapWritable 
	 * @param m
	 * @return
	 */
	public static ObjectNode from(final BSONObject b) {
		return new ObjectNodeWrapper(JsonNodeFactory.instance, b);
	}
	
	/** Utility that goes from various MongoDB artefacts to JsonNode
	 * @param x
	 * @return
	 */
	protected static JsonNode transform(Object x, JsonNodeFactory nc) {
		if (null == x) {
			return nc.nullNode();
		}
		else if (x instanceof ObjectId) {
			return nc.textNode(((ObjectId)x).toString());
		}
		else if (x instanceof Boolean) {
			return nc.booleanNode(((Boolean)x));
		}
		else if (x instanceof String) {
			return nc.textNode(((String)x));
		}
		else if (x instanceof Date) {
			return nc.numberNode(((Date)x).getTime());
		}
		else if (x instanceof Double) {
			return nc.numberNode(((Double)x));
		}
		else if (x instanceof Float) {
			return nc.numberNode(((Float)x));
		}
		else if (x instanceof Long) {
			return nc.numberNode(((Long)x));
		}
		else if (x instanceof Integer) {
			return nc.numberNode(((Integer)x));
		}
		else if (x instanceof byte[]) {
			return nc.binaryNode(((byte[])x));
		}
		else if (x instanceof BasicBSONList) {
			// (don't do this lazily, construct entire thing once requested)
			return new ArrayNodeWrapper(nc, (BasicBSONList)x);
		}
		else if (x instanceof BSONObject) { // recurse! (ish)
			return new ObjectNodeWrapper(nc, (BSONObject) x); 
		}		
		else return nc.nullNode();
	}

	/////////////////////////////////////////////////
	
	// Utility classes
	
	
	/** Lazy map that can have a mix of String/JsonObject and Text/Writable in it
	 * @author Alex
	 */
	public static class LazyTransformingMap implements Map<String, JsonNode> {
		private final static LinkedHashMap<String, JsonNode> _EMPTY_MAP = new LinkedHashMap<>();
		private final static BasicBSONObject _EMPTY_WMAP = new BasicDBObject();
		protected final JsonNodeFactory _nc;
		protected BasicBSONObject _delegate;
		protected LinkedHashMap<String, JsonNode> _new_vals = _EMPTY_MAP;
		LazyTransformingMap(BSONObject delegate, JsonNodeFactory nc) {
			_delegate = (BasicBSONObject) delegate;
			_nc = nc;
		}
		
		@Override
		public int size() {
			return _new_vals.size() + _delegate.size();
		}

		@Override
		public boolean isEmpty() {
			return _new_vals.isEmpty() && _delegate.isEmpty();
		}

		@Override
		public boolean containsKey(Object key) {
			return _new_vals.containsKey(key) || _delegate.containsField((String) key);
		}

		@Override
		public boolean containsValue(Object value) {
			return _new_vals.containsValue(value) || 
					_delegate.values().stream().map(x -> JsonNodeBsonUtils.transform(x, _nc)).anyMatch(x -> x.equals(value))
					;
		}

		@Override
		public JsonNode get(Object key) {
			return (JsonNode) _new_vals.getOrDefault(key, JsonNodeBsonUtils.transform(_delegate.toMap().get((String) key), _nc));
		}

		@Override
		public JsonNode put(String key, JsonNode value) {
			createNewVals();
			JsonNode ret_val = this.remove(key);
			_new_vals.put(key, value);
			return ret_val;
		}

		@Override
		public JsonNode remove(Object key) {
			Object try1 = _new_vals.remove(key);
			
			if (null == try1) {
				try1 = _delegate.removeField((String) key);
				if (null != try1) {
					try1 = JsonNodeBsonUtils.transform(try1, _nc);
				}
			}
			return (JsonNode) try1;
		}

		@Override
		public void putAll(Map<? extends String, ? extends JsonNode> m) {
			createNewVals();
			_new_vals.putAll(m);
		}

		@Override
		public void clear() {
			_new_vals = _EMPTY_MAP;
			_delegate = _EMPTY_WMAP;
		}

		private void createNewVals() {
			if (_EMPTY_MAP == _new_vals) {
				_new_vals = new LinkedHashMap<>();
			}			
		}
		
		@SuppressWarnings("unchecked")
		private void swap() {
			createNewVals();
			_delegate.toMap().forEach((k, v) -> _new_vals.put(k.toString(), transform(v, _nc)));
			_delegate = _EMPTY_WMAP; // (to avoid mutating _delegate unless we have to)
		}
		
		@Override
		public Set<String> keySet() {
			swap();
			return _new_vals.keySet();
		}

		@Override
		public Collection<JsonNode> values() {
			swap();
			return _new_vals.values();
		}

		@Override
		public Set<java.util.Map.Entry<String, JsonNode>> entrySet() {
			swap();
			return _new_vals.entrySet();
		}		
	}
	
	/** Object node mapper for MapWritable
	 * @author Alex
	 */
	public static class ObjectNodeWrapper extends ObjectNode {
		private Map<String, JsonNode> _my_children;
		/** User c'tor
		 * @param nc
		 * @param kids
		 */
		@SuppressWarnings("unchecked")
		public ObjectNodeWrapper(JsonNodeFactory nc, BSONObject delegate) {
			super(nc);
			try {
				// Ugh, children is private
				final Field f = ObjectNode.class.getDeclaredField("_children");
				f.setAccessible(true);
				f.set(this, new LazyTransformingMap(delegate, nc));
				_my_children = (Map<String, JsonNode>) f.get(this);
			}
			catch (Exception e) {}
		}
		
		public boolean containsKey(Object key) {
			return _my_children.containsKey(key);
		}
		public boolean containsValue(Object val) {
			return _my_children.containsValue(val);
		}
		public boolean isEmpty() {
			return _my_children.isEmpty();
		}
	}
	
	/** Array node mapper for ArrayWritable
	 * @author Alex
	 */
	public static class ArrayNodeWrapper extends ArrayNode {

		public ArrayNodeWrapper(JsonNodeFactory nc, BasicBSONList delegate) {
			super(nc);
			try {
				// Ugh, children is private
				final Field f = ArrayNode.class.getDeclaredField("_children");
				f.setAccessible(true);
				f.set(this, com.google.common.collect.Lists.transform(delegate, x -> transform(x, nc)));
			}
			catch (Exception e) {}
		}		
	}
	
}
