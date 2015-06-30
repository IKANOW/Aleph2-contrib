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
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchConfigurationBean;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext;
import com.ikanow.aleph2.shared.crud.elasticsearch.services.ElasticsearchCrudService.CreationPolicy;

/** A factory for returning real or "mock" Elasticsearch CRUD service
 * @author Alex
 */
public class ElasticsearchCrudServiceFactory implements IElasticsearchCrudServiceFactory {

	protected final ElasticsearchConfigurationBean _config_bean;
	
	/** Guice constructor
	 */
	@Inject
	public ElasticsearchCrudServiceFactory(final ElasticsearchConfigurationBean config_bean) {
		_config_bean = config_bean;
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.shared.crud.elasticsearch.services.IElasticsearchCrudServiceFactory#getClient()
	 */
	public synchronized Client getClient() {
		if (!_client.isSet()) {
			final Builder settings_builder = ImmutableSettings.settingsBuilder();
			final Settings settings = null != _config_bean.cluster_name()
					? settings_builder.put("cluster.name", _config_bean.cluster_name()).build()
					: settings_builder.put("client.transport.ignore_cluster_name", true).build();
			
			_client.set(java.util.stream.Stream.of(_config_bean.elasticsearch_connection().split("\\s*,\\s*"))
									.map(hostport -> {
										final String[] host_port = hostport.split("\\s*:\\s*");
										return Tuples._2T(host_port[0], host_port.length > 1 ? host_port[1] : "9300");
									})
									.reduce(new TransportClient(settings),
											(acc, host_port) -> acc.addTransportAddress(new InetSocketTransportAddress(host_port._1(), Integer.parseInt(host_port._2()))),
											(acc1, acc2) -> acc1) // (not possible)
			);
		}		
		return _client.get();		
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
	private static final SetOnce<Client> _client = new SetOnce<>();
	
}
