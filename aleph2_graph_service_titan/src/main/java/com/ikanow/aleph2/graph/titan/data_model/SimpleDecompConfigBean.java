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

package com.ikanow.aleph2.graph.titan.data_model;

import java.util.List;

/** Defines a very simple schema for decomposing data objects into vertices and edges (/nodes and links)
 * @author Alex
 */
public class SimpleDecompConfigBean {
	public static class SimpleDecompElementBean {
		public String edge_name() { return edge_name; }
		public List<String> from_fields() { return from_fields; }
		public String from_type() { return from_type; }
		public List<String> to_fields() { return to_fields; }
		public String to_type() { return to_type; }
		public Boolean bidirectional() { return bidirectional; }
		
		private String edge_name;
		private List<String> from_fields;
		private String from_type;
		private List<String> to_fields;
		private String to_type;
		private Boolean bidirectional;
	}
	public List<SimpleDecompElementBean> elements() { return elements; }
	private List<SimpleDecompElementBean> elements;
}
