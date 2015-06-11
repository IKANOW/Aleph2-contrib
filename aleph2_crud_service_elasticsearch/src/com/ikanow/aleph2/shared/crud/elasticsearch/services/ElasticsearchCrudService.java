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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.elasticsearch.client.Client;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBasicSearchService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;

//TODO ... going to need to get the mapping if any queries involve _id ranges.... and error if not 

//TODO ... handle write mode with no type, ie where it's user defined, with policy for what should happen on conflicts

//TODO .... more thoughts on field list buckets ... options for auto generating .number fields and .raw fields (and nested - that might live in the search index bit though?)

public class ElasticsearchCrudService<O> implements ICrudService<O> {

	public ElasticsearchCrudService(final Class<O> bean_clazz,  
			final Client client, final List<String> indexes, final List<String> types, 
			final Optional<String> auth_fieldname, final Optional<AuthorizationBean> auth, final Optional<ProjectBean> project)
	{
		_state = new State(client, indexes, types);	
		_object_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	}
	protected static class State {
		State(Client client, final List<String> indexes, final List<String> types) {
			this.client = client;
			this.indexes = indexes;
			this.types = types;
			read_only = (1 == types.size()); // no types == _all, >1 type = read-only
		}
		final Client client;
		final List<String> indexes;
		final List<String> types;
		final boolean read_only;
	}
	protected final State _state;
	protected final ObjectMapper _object_mapper;
	
	/////////////////////////////////////////////////////
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getFilteredRepo(java.lang.String, java.util.Optional, java.util.Optional)
	 */
	@Override
	public ICrudService<O> getFilteredRepo(String authorization_fieldname,
			Optional<AuthorizationBean> client_auth,
			Optional<ProjectBean> project_auth) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object, boolean)
	 */
	@Override
	public CompletableFuture<Supplier<Object>> storeObject(O new_object,
			boolean replace_if_present) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObject(java.lang.Object)
	 */
	@Override
	public CompletableFuture<Supplier<Object>> storeObject(O new_object) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<O> new_objects, boolean continue_on_error) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#storeObjects(java.util.List)
	 */
	@Override
	public CompletableFuture<Tuple2<Supplier<List<Object>>, Supplier<Long>>> storeObjects(
			List<O> new_objects) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#optimizeQuery(java.util.List)
	 */
	@Override
	public CompletableFuture<Boolean> optimizeQuery(
			List<String> ordered_field_list) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deregisterOptimizedQuery(java.util.List)
	 */
	@Override
	public boolean deregisterOptimizedQuery(List<String> ordered_field_list) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Optional<O>> getObjectBySpec(
			QueryComponent<O> unique_spec) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Optional<O>> getObjectBySpec(
			QueryComponent<O> unique_spec, List<String> field_list,
			boolean include) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object)
	 */
	@Override
	public CompletableFuture<Optional<O>> getObjectById(Object id) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectById(java.lang.Object, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Optional<O>> getObjectById(Object id,
			List<String> field_list, boolean include) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<O>> getObjectsBySpec(
			QueryComponent<O> spec) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor<O>> getObjectsBySpec(
			QueryComponent<O> spec, List<String> field_list, boolean include) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Long> countObjectsBySpec(QueryComponent<O> spec) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#countObjects()
	 */
	@Override
	public CompletableFuture<Long> countObjects() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectById(java.lang.Object, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public CompletableFuture<Boolean> updateObjectById(Object id,
			UpdateComponent<O> update) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public CompletableFuture<Boolean> updateObjectBySpec(
			QueryComponent<O> unique_spec, Optional<Boolean> upsert,
			UpdateComponent<O> update) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent)
	 */
	@Override
	public CompletableFuture<Long> updateObjectsBySpec(QueryComponent<O> spec,
			Optional<Boolean> upsert, UpdateComponent<O> update) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#updateAndReturnObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent, java.util.Optional, com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent, java.util.Optional, java.util.List, boolean)
	 */
	@Override
	public CompletableFuture<Optional<O>> updateAndReturnObjectBySpec(
			QueryComponent<O> unique_spec, Optional<Boolean> upsert,
			UpdateComponent<O> update, Optional<Boolean> before_updated,
			List<String> field_list, boolean include) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectById(java.lang.Object)
	 */
	@Override
	public CompletableFuture<Boolean> deleteObjectById(Object id) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Boolean> deleteObjectBySpec(
			QueryComponent<O> unique_spec) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteObjectsBySpec(com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent)
	 */
	@Override
	public CompletableFuture<Long> deleteObjectsBySpec(QueryComponent<O> spec) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#deleteDatastore()
	 */
	@Override
	public CompletableFuture<Boolean> deleteDatastore() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getRawCrudService()
	 */
	@Override
	public ICrudService<JsonNode> getRawCrudService() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getSearchService()
	 */
	@Override
	public Optional<IBasicSearchService<O>> getSearchService() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		// TODO Auto-generated method stub
		return null;
	}

}
