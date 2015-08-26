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
	private String status;
	private String result;
	private Date started_processing_on;
	private Date last_processed_on;

	public String _id() { return _id; }
	public JsonNode source() { return source; }
	public ProcessingTestSpecBean test_params() { return test_params; }
	public String status() { return status; }
	public String result() { return result; }
	public Date started_processing_on() { return started_processing_on; }
	public Date last_processed_on() { return last_processed_on; }
}
