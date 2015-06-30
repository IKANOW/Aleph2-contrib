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
package com.ikanow.aleph2.shared.crud.elasticsearch.data_model;

/** Configuration for the Elasticsearch CRUD service
 * @author Alex
 */
public class ElasticsearchConfigurationBean {

	final public static String PROPERTIES_ROOT = "ElasticsearchCrudService";
	
	protected ElasticsearchConfigurationBean() {}
	
	public ElasticsearchConfigurationBean(final String elasticsearch_connection, final String cluster_name) {
		this.elasticsearch_connection = elasticsearch_connection;
		this.cluster_name = cluster_name;
	}
	/** The connection string that is used to initialize the Elasticsearch client
	 * @return
	 */
	public String elasticsearch_connection() { return elasticsearch_connection; }
	/** The Elasticsearch cluster name to which to connect
	 * @return
	 */
	public String cluster_name() { return cluster_name; }
	
	private String elasticsearch_connection;
	private String cluster_name;
}
