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
package com.ikanow.aleph2.shared.crud.elasticsearch.services;

import java.util.Optional;

import org.elasticsearch.client.Client;

import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext;
import com.ikanow.aleph2.shared.crud.elasticsearch.services.ElasticsearchCrudService.CreationPolicy;

/** A factory for returning real or "mock" Elasticsearch CRUD service
 * @author Alex
 */
public interface IElasticsearchCrudServiceFactory {

	/** Returns the elasticsearch client which backs the CRUD service
	 * @return
	 */
	Client getClient();
	
	/** Returns the CRUD service
	 * @param bean_clazz - h
	 * @param es_context - the elasticsearch context
	 * @param id_ranges_ok - whether the _id field is indexed for this set of indexes/types
	 * @param auth_fieldname - optionally, the fieldname to which auth/project beans are applied
	 * @param auth - optionally, an authorization overlay added to each query
	 * @param project - optionally, a project overlay added to each query
	 * @return
	 */
	<O> ElasticsearchCrudService<O> getElasticsearchCrudService(final Class<O> bean_clazz,  
			final ElasticsearchContext es_context, 
			final Optional<Boolean> id_ranges_ok, final CreationPolicy creation_policy, 
			final Optional<String> auth_fieldname, final Optional<AuthorizationBean> auth, final Optional<ProjectBean> project,
			final Optional<DataSchemaBean.WriteSettings> batch_write_settings);	
}
