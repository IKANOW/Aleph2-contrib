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
import java.util.Map;

import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.list.TransformedList;
import org.apache.commons.collections.map.TransformedMap;
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
	private static JsonNode transform(Object x, JsonNodeFactory nc) {
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
			return new ArrayNodeWrapper(nc, xx);
		}
		else if (x instanceof MapWritable) { // recurse! (ish)
			return new ObjectNodeWrapper(nc, (MapWritable) x); 
		}		
		else return nc.nullNode();
	}

	/////////////////////////////////////////////////
	
	// Utility classes
	
	/** Maps keys
	 * @author Alex
	 */
	public static class WritableToString implements Transformer {
		@Override
		public Object transform(Object arg0) {
			return (null == arg0) ? null : arg0.toString();
		}		
	}
	
	/** Maps values
	 * @author Alex
	 */
	public static class WritableToJsonNode implements Transformer {
		protected final JsonNodeFactory _nc;
		public WritableToJsonNode(JsonNodeFactory nc) {
			_nc = nc;
		}
		
		@Override
		public Object transform(Object arg0) {
			return JsonNodeWritableUtils.transform(arg0, _nc);
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
		@SuppressWarnings("unchecked")
		public ObjectNodeWrapper(JsonNodeFactory nc, Map<Writable, Writable> kids) {
			super(nc, (Map<String, JsonNode>)TransformedMap.decorate(kids, new WritableToString(), new WritableToJsonNode(nc)));
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
				f.set(this, TransformedList.decorate(Arrays.asList(kids), new WritableToJsonNode(nc)));
			}
			catch (Exception e) {}
		}		
	}
	
}
