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
package com.ikanow.aleph2.v1.document_db.services;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Optional;

import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.v1.document_db.data_model.V1DocDbConfigBean;

public class TestV1DocumentService {

	@SuppressWarnings("rawtypes")
	public static interface InputFormatAccessTest extends IAnalyticsAccessContext<InputFormat> {}
	public static interface StringAccessTest extends IAnalyticsAccessContext<String> {}
	
	@Test
	public void test_V1DocumentService() {
		
		final V1DocumentDbService to_test = new V1DocumentDbService();
		
		assertEquals(Arrays.asList(to_test), to_test.getUnderlyingArtefacts());
		
		final DataSchemaBean.DocumentSchemaBean test_schema = BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class).done().get();
		final DataBucketBean test_bucket = BeanTemplateUtils.build(DataBucketBean.class).done().get();
		assertEquals("Validate returns errors", 2, to_test.validateSchema(test_schema, test_bucket)._2().size());
		
		assertEquals(Optional.empty(), to_test.getUnderlyingPlatformDriver(String.class, Optional.empty()));
		
		assertEquals(Optional.empty(), to_test.getUnderlyingPlatformDriver(StringAccessTest.class, Optional.empty()));
		
		final V1DocDbConfigBean config = new V1DocDbConfigBean("test:27018");
		final V1DocumentDbService to_test_2 = new V1DocumentDbService(config);

		assertTrue("Should return input format access test", to_test_2.getUnderlyingPlatformDriver(InputFormatAccessTest.class, Optional.of("{}")).isPresent());
		
		// code coverage!		
		to_test.youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules();		
		assertEquals(1, V1DocumentDbService.getExtraDependencyModules().size());
	}
}
