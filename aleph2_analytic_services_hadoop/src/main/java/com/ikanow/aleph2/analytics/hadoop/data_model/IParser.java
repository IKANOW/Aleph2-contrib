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
package com.ikanow.aleph2.analytics.hadoop.data_model;

import java.io.InputStream;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;

public interface IParser {

	/** Get the next record in a list
	 *  Returns null when done
	 * @param currentFileIndex
	 * @param fileName
	 * @param inStream
	 * @return
	 */
	Tuple2<Long, IBatchRecord> getNextRecord(long currentFileIndex,String fileName,  InputStream inStream);
	
	/** Returns true if a file can contain multiple records 
	 * @return
	 */
	default boolean multipleRecordsPerFile(){
		return false;
	}

}
