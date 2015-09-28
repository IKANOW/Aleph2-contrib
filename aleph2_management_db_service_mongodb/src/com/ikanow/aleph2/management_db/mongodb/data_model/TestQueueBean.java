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
package com.ikanow.aleph2.management_db.mongodb.data_model;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;

public class TestQueueBean implements Serializable{
		
	private static final long serialVersionUID = 2744513058711901580L;
	private String _id;
	private JsonNode source;
	private ProcessingTestSpecBean test_params;
	private TestStatus status;
	private String result;
	private String message;
	private Date started_processing_on;
	private Date last_processed_on;

	
	
	public String _id() { return _id; }
	public JsonNode source() { return source; }
	public ProcessingTestSpecBean test_params() { return test_params; }
	public TestStatus status() { return status; }
	public String result() { return result; }
	public String message() { return message; }
	public Date started_processing_on() { return started_processing_on; }
	public Date last_processed_on() { return last_processed_on; }
	
	/**
	 * Enum for the various states the test
	 * can currently be in
	 * 
	 * @author Burch
	 *
	 */
	public enum TestStatus {
		in_progress,
		submitted,
		completed,
		error
	}
}
