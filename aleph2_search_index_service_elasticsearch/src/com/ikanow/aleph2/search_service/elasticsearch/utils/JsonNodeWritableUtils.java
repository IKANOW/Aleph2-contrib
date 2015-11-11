/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.search_service.elasticsearch.utils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Utilities for creating JsonNodes lazily from MapWritables
 * @author Alex
 *
 */
public class JsonNodeWritableUtils {

	/** Creates a lazy object node from a MapWritable 
	 * @param m
	 * @return
	 */
	public static ObjectNode from(final MapWritable m) {
		return new ObjectNodeWrapper(JsonNodeFactory.instance, m);
	}
	
	/** Utility that goes from Writable to JsonNode
	 * @param x
	 * @return
	 */
	protected static JsonNode transform(Object x, JsonNodeFactory nc) {
		if (null == x) {
			return nc.nullNode();
		}
		else if (x instanceof NullWritable) {
			return nc.nullNode();
		}
		else if (x instanceof BooleanWritable) {
			return nc.booleanNode(((BooleanWritable)x).get());
		}
		else if (x instanceof Text) {
			return nc.textNode(((Text)x).toString());
		}
		else if (x instanceof ByteWritable) {
			return nc.binaryNode(new byte[]{ ((ByteWritable)x).get() });
		}
		else if (x instanceof IntWritable) {
			return nc.numberNode(((IntWritable)x).get());
		}
		else if (x instanceof VIntWritable) {
			return nc.numberNode(((VIntWritable)x).get());
		}
		else if (x instanceof LongWritable) {
			return nc.numberNode(((LongWritable)x).get());
		}
		else if (x instanceof VLongWritable) {
			return nc.numberNode(((VLongWritable)x).get());
		}
		else if (x instanceof BytesWritable) {
			return nc.binaryNode(((BytesWritable)x).getBytes());
		}
		else if (x instanceof DoubleWritable) {
			return nc.numberNode(((DoubleWritable)x).get());
		}
		else if (x instanceof FloatWritable) {
			return nc.numberNode(((FloatWritable)x).get());
		}
		else if (x instanceof ArrayWritable) {
			Writable[] xx = ((ArrayWritable)x).get();
			// (don't do this lazily, construct entire thing once requested)
			return new ArrayNodeWrapper(nc, xx);
		}
		else if (x instanceof MapWritable) { // recurse! (ish)
			return new ObjectNodeWrapper(nc, (MapWritable) x); 
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
		private final static Map<Writable, Writable> _EMPTY_WMAP = new MapWritable();
		protected final JsonNodeFactory _nc;
		protected Map<Writable, Writable> _delegate;
		protected LinkedHashMap<String, JsonNode> _new_vals = _EMPTY_MAP;
		LazyTransformingMap(Map<Writable, Writable> delegate, JsonNodeFactory nc) {
			_delegate = delegate;
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
			return _new_vals.containsKey(key) || _delegate.containsKey(new Text((String) key));
		}

		@Override
		public boolean containsValue(Object value) {
			return _new_vals.containsValue(value) || 
					_delegate.values().stream().map(x -> JsonNodeWritableUtils.transform(x, _nc)).anyMatch(x -> x.equals(value))
					;
		}

		@Override
		public JsonNode get(Object key) {
			return (JsonNode) _new_vals.getOrDefault(key, JsonNodeWritableUtils.transform(_delegate.get(new Text((String) key)), _nc));
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
				try1 = _delegate.remove(new Text((String) key));
				if (null != try1) {
					try1 = JsonNodeWritableUtils.transform(try1, _nc);
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
		
		private void swap() {
			createNewVals();
			_delegate.forEach((k, v) -> _new_vals.put(k.toString(), transform(v, _nc)));
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
		/** User c'tor
		 * @param nc
		 * @param kids
		 */
		public ObjectNodeWrapper(JsonNodeFactory nc, Map<Writable, Writable> kids) {
			super(nc, new LazyTransformingMap(kids, nc));
		}
		
		public boolean containsKey(Object key) {
			return _children.containsKey(key);
		}
		public boolean containsValue(Object val) {
			return _children.containsValue(val);
		}
		public boolean isEmpty() {
			return _children.isEmpty();
		}
	}
	
	/** Array node mapper for ArrayWritable
	 * @author Alex
	 */
	public static class ArrayNodeWrapper extends ArrayNode {

		public ArrayNodeWrapper(JsonNodeFactory nc, Writable[] kids) {
			super(nc);
			try {
				// Ugh, children is private
				final Field f = ArrayNode.class.getDeclaredField("_children");
				f.setAccessible(true);
				f.set(this, com.google.common.collect.Lists.transform(Arrays.asList(kids), x -> transform(x, nc)));
			}
			catch (Exception e) {}
		}		
	}
	
}
