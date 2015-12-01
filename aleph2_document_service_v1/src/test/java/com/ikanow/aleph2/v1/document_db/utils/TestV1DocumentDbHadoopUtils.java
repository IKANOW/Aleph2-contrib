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
package com.ikanow.aleph2.v1.document_db.utils;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.Test;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.v1.document_db.data_model.V1DocDbConfigBean;
import com.ikanow.aleph2.v1.document_db.hadoop.assets.Aleph2V1InputFormat;

import fj.data.Either;

public class TestV1DocumentDbHadoopUtils {

	@Test
	public void test_getAccessService() {

		new V1DocumentDbHadoopUtils(); //code coverage!
		
		@SuppressWarnings("rawtypes")
		final IAnalyticsAccessContext<InputFormat> access_context =
				V1DocumentDbHadoopUtils.getInputFormat(null, null); // (doesn't matter what the input is here)
		
		assertEquals(Either.right(Aleph2V1InputFormat.class), access_context.getAccessService());
	}
	
	@Test 
	public void test_getAccessConfig() {
		
		// No filter (no type), max records
		// - all the conditional logic is encapsulated in legacy V1 code 
		{
			final AnalyticThreadJobBean.AnalyticThreadJobInputBean job_input =
					BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobInputBean.class)
						.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::resource_name_or_id, "/aleph2_external/565e076a12c33214b78fd3c2,565e076a12c33214b78fd3c3")
						.with(AnalyticThreadJobBean.AnalyticThreadJobInputBean::config,
								BeanTemplateUtils.build(AnalyticThreadJobBean.AnalyticThreadJobInputConfigBean.class)
									.with(AnalyticThreadJobBean.AnalyticThreadJobInputConfigBean::record_limit_request, 10L)
								.done().get()
								)
					.done().get()
					;
			
			final V1DocDbConfigBean config = new V1DocDbConfigBean("test:27018");
			
			@SuppressWarnings("rawtypes")
			final IAnalyticsAccessContext<InputFormat> access_context =
					V1DocumentDbHadoopUtils.getInputFormat(job_input, config); // (doesn't matter what the input is here)
			
			final Map<String, Object> res = access_context.getAccessConfig().get();

			//DEBUG
			//System.out.println(res);			
			
			assertEquals(10, res.size());
			assertEquals("unknown:/aleph2_external/565e076a12c33214b78fd3c2,565e076a12c33214b78fd3c3", res.get("mongo.job.name"));
			assertEquals("true", res.get("mongo.job.verbose"));
			assertEquals("false", res.get("mongo.job.background"));
			assertEquals("mongodb://test:27018/doc_metadata.metadata", res.get("mongo.input.uri"));
			assertEquals("{ \"communityId\" : { \"$in\" : [ { \"$oid\" : \"565e076a12c33214b78fd3c2\"} , { \"$oid\" : \"565e076a12c33214b78fd3c3\"}]} , \"index\" : { \"$ne\" : \"?DEL?\"}}", res.get("mongo.input.query"));
			assertEquals("", res.get("mongo.input.fields"));
			assertEquals("10", res.get("mongo.input.limit"));
			assertEquals("8", res.get("max.splits"));
			assertEquals("12500", res.get("max.docs.per.split"));
			assertEquals("{}", res.get("infinit.e.source.tags.filter"));

			assertEquals("service_name=Aleph2V1InputFormat options={mongo.input.limit=10, max.docs.per.split=12500, mongo.input.fields=, mongo.input.query={ \"communityId\" : { \"$in\" : [ { \"$oid\" : \"565e076a12c33214b78fd3c2\"} , { \"$oid\" : \"565e076a12c33214b78fd3c3\"}]} , \"index\" : { \"$ne\" : \"?DEL?\"}}, infinit.e.source.tags.filter={}, mongo.job.name=unknown:/aleph2_external/565e076a12c33214b78fd3c2,565e076a12c33214b78fd3c3, max.splits=8, mongo.input.uri=mongodb://test:27018/doc_metadata.metadata}", access_context.describe());
		}
	}
	
}
