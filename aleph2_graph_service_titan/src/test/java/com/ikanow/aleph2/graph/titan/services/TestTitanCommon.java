/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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

package com.ikanow.aleph2.graph.titan.services;

import java.io.File;
import java.util.Collections;
import java.util.Optional;

import org.junit.Before;

import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.GraphSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.thinkaurelius.titan.core.TitanGraph;

/**
 * @author Alex
 *
 */
public class TestTitanCommon {

	static TitanGraph _titan = null;
	static MockTitanGraphService _mock_graph_db_service = null;
	
	@Before
	public void setup() throws InterruptedException {
		if (null != _titan) {
			return;
		}
		//(delete old ES files)
		MockTitanGraphService.TEST_CHANGE_UUID();
		try {
			new File(TitanGraphService.UUID).delete();
		}
		catch (Exception e) {}
		
		_mock_graph_db_service = new MockTitanGraphService();
		_titan = _mock_graph_db_service.getUnderlyingPlatformDriver(TitanGraph.class, Optional.empty()).get();
		final DataBucketBean bucket = BeanTemplateUtils.build(DataBucketBean.class)
				.with(DataBucketBean::full_name, "/test/setup/indexes")
			.done().get();			
		_mock_graph_db_service.onPublishOrUpdate(bucket, Optional.empty(), false, ImmutableSet.of(GraphSchemaBean.name), Collections.emptySet());
	}
}
