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
package com.ikanow.aleph2.analytics.hadoop.services;

import java.io.InputStream;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.analytics.hadoop.assets.BeFileInputReader;
import com.ikanow.aleph2.analytics.hadoop.data_model.IParser;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;

/** Parser for reading in JSON data
 * @author Alex
 */
public class BeJsonParser implements IParser {
	private static final Logger logger = LogManager.getLogger(BeJsonParser.class);

	@Override
	public Tuple2<Long, IBatchRecord> getNextRecord(long currentFileIndex,String fileName,  InputStream inStream) {
		ObjectMapper object_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
		Tuple2<Long, IBatchRecord> t2 = null;
		try {
			JsonNode node = object_mapper.readTree(inStream);
			
			/**/
			System.out.println("JSON ?? " + node.toString());
			
			
			t2 = new Tuple2<Long, IBatchRecord>(currentFileIndex, new BeFileInputReader.BatchRecord(node, null));
		} catch (Exception e) {
			/**/
			System.out.println("JSON PARSER EXC = " + ErrorUtils.getLongForm("{0}", e));
			
			logger.error("JsonParser caught exception",e);
		}
		return t2;
	}

}
