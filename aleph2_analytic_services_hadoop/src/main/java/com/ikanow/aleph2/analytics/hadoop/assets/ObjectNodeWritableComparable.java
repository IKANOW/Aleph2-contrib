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
package com.ikanow.aleph2.analytics.hadoop.assets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

/** Object node wrapper that is also hadoop Writable
 * @author Alex
 */
public class ObjectNodeWritableComparable implements WritableComparable<Object> {

	protected static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());

	private ObjectNode _object_node;

	/** User c'tor
	 * @param object_node
	 */
	public ObjectNodeWritableComparable(final ObjectNode object_node) {
		_object_node = object_node;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		final Text text = new Text();
		text.set(_object_node.toString());
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		final Text text = new Text();
		text.readFields(in);
		_object_node = (ObjectNode) _mapper.readTree(text.toString()); //(object node by construction)
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override public int compareTo(Object o){
		return toString().compareTo(o.toString());
	}
	
	public static class Comparator extends WritableComparator {
	    @Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	        int i1 = readInt(b1, s1);
	        int i2 = readInt(b2, s2);
	         
	        int comp = (i1 < i2) ? -1 : (i1 == i2) ? 0 : 1;
	        if(0 != comp)
	            return comp;
	         
	        int j1 = readInt(b1, s1+4);
	        int j2 = readInt(b2, s2+4);
	        comp = (j1 < j2) ? -1 : (j1 == j2) ? 0 : 1;
	         
	        return comp;
	    }
	}	
}
