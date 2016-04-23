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

import java.io.InputStream;
import java.util.Date;
import java.util.Optional;
import java.util.stream.Stream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.ikanow.aleph2.analytics.hadoop.data_model.BeFileInputConfigBean;
import com.ikanow.aleph2.analytics.hadoop.data_model.IParser;
import com.ikanow.aleph2.core.shared.utils.BatchRecordUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.UuidUtils;

/** XML parser - NOTE LOTS OF CODE C/P FROM
 *  https://github.com/IKANOW/Infinit.e/blob/master/core/infinit.e.harvest.library/src/com/ikanow/infinit/e/harvest/extraction/document/file/XmlToMetadataParser.java
 * @author Alex
 */
public class BeXmlParser implements IParser {
	protected static final Logger logger = LogManager.getLogger();

	protected static final XMLInputFactory _factory = XMLInputFactory.newInstance();
	protected static final XmlMapper _xml_mapper = new XmlMapper();

	protected static final String root_uuid = "A" + Long.toString(new Date().getTime());
	
	protected final BeFileInputConfigBean.XML _xml;
	protected final boolean _prefix_mode; 
	
	public BeXmlParser(final BeFileInputConfigBean.XML xml) {
		_xml = xml;
		
		// If any of the fields contain ":" then it's in "prefix mode", which means the prefixes are used to lookup root and ignore fields (but are still discarded from the JSON)
		_prefix_mode = Stream.concat(xml.root_fields().stream(), xml.ignore_fields().stream()).anyMatch(s -> s.contains(":"));
		
		_factory.setProperty(XMLInputFactory.IS_COALESCING, true);
		_factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);		
	}
	
	protected XMLStreamReader _mutable_reader = null;
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IParser#multipleRecordsPerFile()
	 */
	@Override
	public boolean multipleRecordsPerFile(){
		return true;
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IParser#getNextRecord(long, java.lang.String, java.io.InputStream)
	 */
	@Override
	public Tuple2<Long, IBatchRecord> getNextRecord(long currentFileIndex, String fileName, InputStream inStream) {
		try {
			if (null == _mutable_reader) {
				_mutable_reader = _factory.createXMLStreamReader(inStream);
			}
			
			String mutable_building_json_from = null;			
			boolean mutable_just_ignored = false;
			boolean mutable_hit_identifier = false;
			String mutable_hit_value = null;
			
			final StringBuffer full_text = new StringBuffer();
			final StringBuffer sb = new StringBuffer();
			
			while (_mutable_reader.hasNext()) {
				
				// (note that the newline separate format for XML doesn't work, not clear why)
				int eventCode = _mutable_reader.next();

				switch (eventCode)
				{
				case(XMLStreamReader.START_ELEMENT):
				{				
					final String tag_name = Lambdas.get(() -> {
						final String tmp = _mutable_reader.getLocalName();
						if (_prefix_mode && (null != _mutable_reader.getPrefix())) {
							return _mutable_reader.getPrefix() + ":" + tmp;
						}
						else {
							return tmp;							
						}
					});
				
					if (null == mutable_building_json_from) { //(looking for a start tag)
						if (_xml.root_fields().isEmpty()) {
							mutable_building_json_from = tag_name;
							mutable_just_ignored = false;
						}
						else if (_xml.root_fields().contains(tag_name)) {
							mutable_building_json_from = tag_name;
							mutable_just_ignored = false;
						}
					}
					else {
						if (_xml.ignore_fields().contains(tag_name))
						{
							mutable_just_ignored = true;
						}
						else {
							if (_xml.preserve_case()) {
								sb.append("<").append(_mutable_reader.getLocalName()).append(">");					
							}
							else {
								sb.append("<").append(_mutable_reader.getLocalName().toLowerCase()).append(">");
							}
							mutable_just_ignored = false;
						}						
					}
					if (null != mutable_building_json_from) { //(building an object
						
						// Check for primary key
						mutable_hit_identifier = tag_name.equalsIgnoreCase(_xml.primary_key_field());
						
						if (null != _xml.xml_text_field()) {
							full_text.append("<").append(_mutable_reader.getLocalName());
							for (int ii = 0; ii < _mutable_reader.getAttributeCount(); ++ii) {
								full_text.append(" ");
								full_text.append(_mutable_reader.getAttributeLocalName(ii)).append("=\"").append(_mutable_reader.getAttributeValue(ii)).append('"');
							}
							full_text.append(">");
						}
						
						if (!mutable_just_ignored && (null != _xml.attribute_prefix())) { // otherwise ignore attributes anyway
							int nAttributes = _mutable_reader.getAttributeCount();
							StringBuffer sb2 = new StringBuffer();
							for (int i = 0; i < nAttributes; ++i) {
								sb2.setLength(0);
								sb.append('<');
								
								sb2.append(_xml.attribute_prefix());
								if (_xml.preserve_case()) {
									sb2.append(_mutable_reader.getAttributeLocalName(i));
								}
								else {
									sb2.append(_mutable_reader.getAttributeLocalName(i).toLowerCase());
								}
								sb2.append('>');
								
								sb.append(sb2);
								sb.append("<![CDATA[").append(_mutable_reader.getAttributeValue(i).trim()).append("]]>");
								sb.append("</").append(sb2);
							}
						}
					}
				}
				break;
				
				case (XMLStreamReader.CHARACTERS):
				{
					if (null != mutable_building_json_from) { //(ignore unless building object)
						
						// Grab primary key
						if ((mutable_hit_identifier) && (null == mutable_hit_value)) {
							mutable_hit_value = _mutable_reader.getText().trim();
						}						
						
						if (null != _xml.xml_text_field()) {
							full_text.append(_mutable_reader.getText());
						}
						
						if(_mutable_reader.getText().trim().length()>0 && mutable_just_ignored == false) {
							sb.append("<![CDATA[").append(_mutable_reader.getText().trim()).append("]]>");
						}
					}					
				}
				break;
				
				case (XMLStreamReader.END_ELEMENT):
				{
					if (null != mutable_building_json_from) {
						final String tag_name = Lambdas.get(() -> {
							final String tmp = _mutable_reader.getLocalName();
							if (_prefix_mode && (null != _mutable_reader.getPrefix())) {
								return _mutable_reader.getPrefix() + ":" + tmp;
							}
							else {
								return tmp;							
							}
						});
						
						if (null != _xml.xml_text_field()) {
							if (_xml.preserve_case()) {
								full_text.append("</").append(_mutable_reader.getLocalName()).append(">");
							}
							else {
								full_text.append("</").append(_mutable_reader.getLocalName().toLowerCase()).append(">");						
							}
						}						
						
						if (mutable_building_json_from.equals(tag_name)) {
							
							// Create JSON
							
							ObjectNode mutable_json = null; 
							try {
								//(add misc tags)
								final JsonNode tmp = _xml_mapper.readTree("<A1455574413045>" + sb.toString() + "</A1455574413045>");
								//(originally i thought this went to an array, but apparently not)
								//final ArrayNode json_container = (ArrayNode) tmp;
								//if (json_container.has(0)) {
								// mutable_json = (ObjectNode) json_container.get(0);
								//}
								mutable_json = (ObjectNode)tmp;								
							}
							catch (Exception e) {
								//DEBUG
								//e.printStackTrace();
								
								return null; // (leave the mutable_reader intact since the XML outline appears to be valid)							
							}
							if (null == mutable_json) return null; //(as above)
							
							// Add _id
							final String id_field = _xml.id_field();
							if (!_xml.set_id_from_content()) { 
								if (!Optional.ofNullable(mutable_hit_value).orElse("").isEmpty()) {
									mutable_json.put(id_field, _xml.primary_key_prefix() + mutable_hit_value);
								}
							}
							else { // set if from content
								mutable_json.put(id_field, UuidUtils.get().getContentBasedUuid(sb.toString().getBytes()));
							}
							
							// Add text field if requested
							if (null != _xml.xml_text_field()) {
								mutable_json.put(_xml.xml_text_field(), full_text.toString());
							}							
							return Tuples._2T(0L, new BatchRecordUtils.JsonBatchRecord(mutable_json));
						}
						else if (!_xml.ignore_fields().contains(tag_name)) {
							if (_xml.preserve_case()) {
								sb.append("</").append(_mutable_reader.getLocalName()).append(">");						
							}
							else {
								sb.append("</").append(_mutable_reader.getLocalName().toLowerCase()).append(">");
							}
						}
					} // (end case)
				}
				break;
				} // (end switch)
			}
		}
		catch (Exception e) {
			//(carry on, will return EOF)
			
			//DEBUG
			//e.printStackTrace();
		}
		try {
			if (null != _mutable_reader) {
				_mutable_reader.close();
				_mutable_reader = null;
			}
		}
		catch (XMLStreamException e) {
			//(carry on, will return EOF)
		}
		return null; //(EOF)
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.analytics.hadoop.data_model.IParser#handleThisPath(java.lang.String)
	 */
	@Override
	public boolean handleThisPath(String path) {
		return path.matches(_xml.file_pattern());
	}

}
