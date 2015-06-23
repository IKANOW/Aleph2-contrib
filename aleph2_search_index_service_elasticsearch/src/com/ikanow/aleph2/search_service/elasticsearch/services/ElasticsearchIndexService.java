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
package com.ikanow.aleph2.search_service.elasticsearch.services;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import org.elasticsearch.client.Client;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.ColumnarSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.TemporalSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean.CollidePolicy;
import com.ikanow.aleph2.search_service.elasticsearch.module.ElasticsearchIndexServiceModule;
import com.ikanow.aleph2.search_service.elasticsearch.utils.ElasticsearchIndexUtils;
import com.ikanow.aleph2.shared.crud.elasticsearch.services.IElasticsearchCrudServiceFactory;

import fj.data.Either;

//TODO: all the data services should have a validate(DataSchemaBean xxx) which returns a list of errors so the bucket dev can find out
//in advance of trying to use them...

/** Elasticsearch implementation of the SearchIndexService/TemporalService/ColumnarService
 * @author Alex
 *
 */
public class ElasticsearchIndexService implements ISearchIndexService, ITemporalService, IColumnarService {

	protected final IElasticsearchCrudServiceFactory _crud_factory;
	protected final ElasticsearchIndexServiceConfigBean _config;
	
	protected final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	/** Guice generated constructor
	 * @param crud_factory
	 */
	@Inject
	public ElasticsearchIndexService(
			final IElasticsearchCrudServiceFactory crud_factory,
			final ElasticsearchIndexServiceConfigBean configuration)
	{
		_crud_factory = crud_factory;
		_config = configuration;
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService#getCrudService(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public <O> ICrudService<O> getCrudService(final Class<O> clazz, final DataBucketBean bucket) {
		
		// There's two different cases
		// 1) Multi-bucket - equivalent to the other version of getCrudService
		// 2) Single bucket - a read/write bucket
		
		if ((null != bucket.multi_bucket_children()) && !bucket.multi_bucket_children().isEmpty()) {
			return getCrudService(clazz, bucket.multi_bucket_children());
		}
		
		// OK so it's a legit single bucket ... first question ... does this already exist?
		
		// If it does, do we need to check and potentially update its mapping? 
		
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService#getCrudService(java.lang.Class, java.util.Collection)
	 */
	@Override
	public <O> ICrudService<O> getCrudService(final Class<O> clazz, final Collection<String> buckets) {
		
		// Grab all the buckets 
		
		// TODO Auto-generated method stub
		return null;
	}
	
	//TODO make this static, or at least all its components
	@SuppressWarnings("unused")
	private void createIndexMapping(final DataBucketBean bucket) {
		
		final JsonNode default_mapping = Optional.ofNullable(bucket.data_schema())
												.map(DataSchemaBean::search_index_schema)
												.filter(s -> Optional.ofNullable(s.enabled()).orElse(true))
												.map(DataSchemaBean.SearchIndexSchemaBean::technology_override_schema)
												.map(t -> _mapper.convertValue(t, JsonNode.class))
											.orElse(BeanTemplateUtils.toJson(_config.search_technology_override()));
		
		// Also get JsonNodes for the default field config bit

		final Optional<JsonNode> columnar_defaults =  Optional.ofNullable(bucket.data_schema())
															.map(DataSchemaBean::columnar_schema)
															.filter(s -> Optional.ofNullable(s.enabled()).orElse(true))
															.map(DataSchemaBean.ColumnarSchemaBean::technology_override_schema)
															.map(t -> _mapper.convertValue(t, JsonNode.class));
		
		final JsonNode default_analyzed_field = columnar_defaults
												.map(j -> j.get("default_field_data_analyzed")) 
												.filter(j -> !j.isNull())
											.orElse(BeanTemplateUtils.toJson(_config.columnar_technology_override().default_field_data_analyzed())); // (can't be null by construction)

		final JsonNode default_not_analyzed_field = columnar_defaults
												.map(j -> j.get("default_field_data_notanalyzed")) 
												.filter(j -> !j.isNull())
											.orElse(BeanTemplateUtils.toJson(_config.columnar_technology_override().default_field_data_notanalyzed())); // (can't be null by construction)		
		
		// Get a list of field overrides Either<String,Tuple2<String,String>> for dynamic/real fields
		
		final ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean settings = BeanTemplateUtils.from(default_mapping, 
														ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean.class).get();
		
		final LinkedHashMap<Either<String, Tuple2<String, String>>, JsonNode> 
			field_lookups = ElasticsearchIndexUtils.parseDefaultMapping(default_mapping, 
					(CollidePolicy.new_type == Optional.ofNullable(settings.collide_policy()).orElse(CollidePolicy.new_type))
							? Optional.empty()
							: Optional.ofNullable(settings.type_name_or_prefix())
						);
		
		//TODO: finish this
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(final Class<T> driver_class, final Optional<String> driver_options) {
		if (Client.class.isAssignableFrom(driver_class)) {
			return (Optional<T>) Optional.of(_crud_factory.getClient());
		}
		return Optional.empty();
	}

	/** This service needs to load some additional classes via Guice. Here's the module that defines the bindings
	 * @return
	 */
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new ElasticsearchIndexServiceModule());
	}
	
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		//(done!)
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.ColumnarSchemaBean)
	 */
	@Override
	public List<BasicMessageBean> validateSchema(final ColumnarSchemaBean schema) {
		//TODO
		return Collections.emptyList();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.TemporalSchemaBean)
	 */
	@Override
	public List<BasicMessageBean> validateSchema(final TemporalSchemaBean schema) {
		//TODO
		return Collections.emptyList();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean)
	 */
	@Override
	public List<BasicMessageBean> validateSchema(final SearchIndexSchemaBean schema) {
		//TODO
		return Collections.emptyList();
	}
}
