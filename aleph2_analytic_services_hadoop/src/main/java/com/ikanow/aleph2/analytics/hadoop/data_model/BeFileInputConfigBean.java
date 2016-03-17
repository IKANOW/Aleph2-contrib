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

package com.ikanow.aleph2.analytics.hadoop.data_model;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Allows user to configure how the incoming file is treated
 * @author Alex
 */
public class BeFileInputConfigBean {
	public List<Parser> parsers() { return parsers; }
	
	private List<Parser> parsers;		
		
	// Sub classes
	
	/** Define a set of parsers
	 * @author Alex
	 */
	public static class Parser {
		/** The XML configuration bean
		 * @return
		 */
		public XML xml() { return xml; }
		
		private XML xml;
	};
	
	/** XML configuration bean
	 * @author Alex
	 */
	public static class XML {

		public String file_pattern() { return Optional.ofNullable(file_pattern).orElse("^.*[.]xml"); }
		
		public List<String> root_fields() { return Optional.ofNullable(root_fields).orElse(Collections.emptyList()); }
		public List<String> ignore_fields() { return Optional.ofNullable(ignore_fields).orElse(Collections.emptyList()); }
		public boolean preserve_case() { return Optional.ofNullable(preserve_case).orElse(true); }
		public String primary_key_prefix() {  return Optional.ofNullable(primary_key_prefix).orElse(""); }
		public String primary_key_field() {  return Optional.ofNullable(primary_key_field).orElse(""); }
		public boolean set_id_from_content() { return Optional.ofNullable(set_id_from_content).orElse(false); }
		public String id_field() { return Optional.ofNullable(id_field).orElse("_id"); }
		// These can be null, meaning disable the funcionality
		public String attribute_prefix() { return attribute_prefix; }		
		public String xml_text_field() { return xml_text_field; }
		
		private String file_pattern;
		
		private List<String> root_fields;
		private List<String> ignore_fields;
		private Boolean preserve_case;
		private String attribute_prefix;
		
		// Whether to store the original XML
		private String xml_text_field;
		
		// Allows integrated setting of _id field
		private String primary_key_prefix;
		private String primary_key_field;
		private Boolean set_id_from_content;
		private String id_field;
	}	
}
