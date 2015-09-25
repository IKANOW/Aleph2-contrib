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
package com.ikanow.aleph2.analytics.storm.services;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;

import com.ikanow.aleph2.analytics.storm.data_model.IStormController;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

/** Dummy instance of Storm controller to let us know that we request a remote version but it could not be initialized
 * @author Alex
 */
public class NoStormController implements IStormController {

	@Override
	public CompletableFuture<BasicMessageBean> submitJob(String job_name,
			String input_jar_location, StormTopology topology, Map<String, Object> config_override) {
		return null;
	}

	@Override
	public CompletableFuture<BasicMessageBean> stopJob(String job_name) {
		return null;
	}

	@Override
	public TopologyInfo getJobStats(String job_name) throws Exception {
		return null;
	}

	@Override
	public List<String> getJobNamesForBucket(String bucket_path) {
		return null;
	}

}
