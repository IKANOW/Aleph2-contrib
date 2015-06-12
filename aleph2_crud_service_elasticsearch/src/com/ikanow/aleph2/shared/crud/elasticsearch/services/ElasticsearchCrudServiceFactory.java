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
package com.ikanow.aleph2.shared.crud.elasticsearch.services;

import java.util.Optional;

import org.elasticsearch.client.Client;

import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext;
import com.ikanow.aleph2.shared.crud.elasticsearch.services.ElasticsearchCrudService.CreationPolicy;

/** A factory for returning real or "mock" Elasticsearch CRUD service
 * @author Alex
 */
public class ElasticsearchCrudServiceFactory implements IElasticsearchCrudServiceFactory {

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.elasticsearch.services.IElasticsearchCrudServiceFactory#getClient()
	 */
	public Client getClient() {
		//TODO (ALEPH-14): use the configuration bean to create a real connection
		return null;		
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.elasticsearch.services.IElasticsearchCrudServiceFactory#getElasticsearchCrudService(java.lang.Class, com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext, java.util.Optional, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	public <O> ElasticsearchCrudService<O> getElasticsearchCrudService(final Class<O> bean_clazz,  
			final ElasticsearchContext es_context, 
			final Optional<Boolean> id_ranges_ok, final CreationPolicy creation_policy, 
			final Optional<String> auth_fieldname, final Optional<AuthorizationBean> auth, final Optional<ProjectBean> project) {
		return new ElasticsearchCrudService<O>(bean_clazz, es_context, id_ranges_ok, creation_policy, auth_fieldname, auth, project);
	}
}
