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
package com.ikanow.aleph2.shared.crud.elasticsearch.data_model;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestElasticsearchConfigurationBean {

	@Test
	public void test_elasticsearchConfigBean() {
		
		assertEquals("ElasticsearchCrudService", ElasticsearchConfigurationBean.PROPERTIES_ROOT);
		
		final ElasticsearchConfigurationBean test1 = new ElasticsearchConfigurationBean();
		assertEquals(null, test1.elasticsearch_connection());
		assertEquals(null, test1.cluster_name());
		
		final ElasticsearchConfigurationBean test2 = new ElasticsearchConfigurationBean("test2a", "test2b");
		assertEquals("test2a", test2.elasticsearch_connection());
		assertEquals("test2b", test2.cluster_name());
		
	}
}
