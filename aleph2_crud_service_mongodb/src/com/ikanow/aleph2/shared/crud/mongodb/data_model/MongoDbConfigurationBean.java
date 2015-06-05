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
 ******************************************************************************/
package com.ikanow.aleph2.shared.crud.mongodb.data_model;


/** The configuration bean for MongoDB
 * @author acp
 */
public class MongoDbConfigurationBean {

	final public static String PROPERTIES_ROOT = "MongoDbCrudService";
	
	protected MongoDbConfigurationBean() {}
	
	/** User constructor
	 * @param mongodb_connection he connection string that is used to initialize the MongoDB
	 */
	public MongoDbConfigurationBean(final String mongodb_connection) {
		this.mongodb_connection = mongodb_connection;
	}
	/** The connection string that is used to initialize the MongoDB
	 * @return
	 */
	public String mongodb_connection() { return mongodb_connection; }
	
	private String mongodb_connection;
}
