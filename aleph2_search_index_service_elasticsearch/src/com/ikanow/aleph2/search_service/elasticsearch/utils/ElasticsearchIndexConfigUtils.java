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
package com.ikanow.aleph2.search_service.elasticsearch.utils;

import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.ikanow.aleph2.search_service.elasticsearch.data_model.ElasticsearchIndexServiceConfigBean;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/** Configuration utilities for building a configuration bean from the various config elements
 * @author Alex
 */
public class ElasticsearchIndexConfigUtils {

	/** Builds a config bean using the provided defaults where not overridden by the user
	 * @param global_config - the global configuration
	 * @return - a fully filled out config bean
	 */
	public static ElasticsearchIndexServiceConfigBean buildConfigBean(final Config global_config) {
		
		try {
			final ElasticsearchIndexServiceConfigBean bean = Lambdas.wrap_u(__ -> {  
				return BeanTemplateUtils.from(PropertiesUtils.getSubConfig(global_config, ElasticsearchIndexServiceConfigBean.PROPERTIES_ROOT).orElse(null), 
										ElasticsearchIndexServiceConfigBean.class);
			})
			.andThen(Lambdas.wrap_u(b -> {
				if (null == b.search_technology_override()) {
					final ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean search_override = BeanTemplateUtils.from( 
							ConfigFactory.parseResources(Thread.currentThread().getContextClassLoader(), 
									"com/ikanow/aleph2/search_service/elasticsearch/data_model/default_search_index_override.conf"), 
							ElasticsearchIndexServiceConfigBean.SearchIndexSchemaDefaultBean.class);
					
					return BeanTemplateUtils.clone(b).with(ElasticsearchIndexServiceConfigBean::search_technology_override, search_override).done();
				}
				else return b;
			}))
			.andThen(Lambdas.wrap_u(b -> {
				if (null == b.columnar_technology_override()) {
					final ElasticsearchIndexServiceConfigBean.ColumnarSchemaDefaultBean columnar_override = BeanTemplateUtils.from( 
							ConfigFactory.parseResources(Thread.currentThread().getContextClassLoader(), 
									"com/ikanow/aleph2/search_service/elasticsearch/data_model/default_columnar_override.conf"), 
							ElasticsearchIndexServiceConfigBean.ColumnarSchemaDefaultBean.class);
					
					return BeanTemplateUtils.clone(b).with(ElasticsearchIndexServiceConfigBean::columnar_technology_override, columnar_override).done();
				}
				else return b;
			}))
			.apply(null); // (Supplier doens't support composition for some reason)
			
			return bean;
		}
		catch (Exception e) {
			throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_CONFIG_ERROR,
					ElasticsearchIndexServiceConfigBean.class.toString(),
					global_config.getConfig(ElasticsearchIndexServiceConfigBean.PROPERTIES_ROOT)
					), e);				
		}
	}
}
