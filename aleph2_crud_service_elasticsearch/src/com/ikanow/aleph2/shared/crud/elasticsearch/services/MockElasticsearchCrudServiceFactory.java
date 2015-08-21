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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext;
import com.ikanow.aleph2.shared.crud.elasticsearch.services.ElasticsearchCrudService.CreationPolicy;

/** A factory for returning real or "mock" Elasticsearch CRUD service
 * @author Alex
 */
public class MockElasticsearchCrudServiceFactory implements IElasticsearchCrudServiceFactory {

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.elasticsearch.services.IElasticsearchCrudServiceFactory#getClient()
	 */
	public Client getClient() {
		synchronized (MockElasticsearchCrudServiceFactory.class) {
			if (!_root_node.isSet()) {
				final ImmutableSettings.Builder test_settings = 
						ImmutableSettings.settingsBuilder()
					        .put("cluster.name", "aleph2")
					        .put("node.gateway.type", "none")
					        .put("index.store.type", "memory")
					        .put("index.number_of_replicas", 0)
					        .put("index.number_of_shards", 1)
					        .put("node.http.enabled", false);
										
				_root_node.set(NodeBuilder.nodeBuilder().settings(test_settings).loadConfigSettings(false).node());				
			}
		}
		if (!_client.isSet()) {			
			_client.set(_root_node.get().client());
		}
		return _client.get();
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.elasticsearch.services.IElasticsearchCrudServiceFactory#getElasticsearchCrudService(java.lang.Class, com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext, java.util.Optional, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	public <O> ElasticsearchCrudService<O> getElasticsearchCrudService(final Class<O> bean_clazz,  
			final ElasticsearchContext es_context, 
			final Optional<Boolean> id_ranges_ok, final CreationPolicy creation_policy, 
			final Optional<String> auth_fieldname, final Optional<AuthorizationBean> auth, final Optional<ProjectBean> project,
			final Optional<DataSchemaBean.WriteSettings> batch_write_settings)
	{
		return new ElasticsearchCrudService<O>(bean_clazz, es_context, id_ranges_ok, creation_policy, auth_fieldname, auth, project, batch_write_settings);
	}
	
	private final SetOnce<Client> _client = new SetOnce<>();
	private static final SetOnce<Node> _root_node = new SetOnce<>();
}
