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
package com.ikanow.aleph2.analytics.storm.services;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import com.ikanow.aleph2.analytics.storm.data_model.IStormController;
import com.ikanow.aleph2.analytics.storm.utils.StormControllerUtil;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;

import fj.data.Either;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;

/**
 * Hands submitting storm jobs to a remote cluster.
 * 
 * @author Burch
 *
 */
public class RemoteStormController implements IStormController  {
	private static final Logger logger = LogManager.getLogger();
	private static final String DEFAULT_STORM_CONFIG = "defaults.yaml";	
	final private Map<String, Object> remote_config;
	private Client client;
	
	public static final String NIMBUS_SEEDS = "nimbus.seeds";
	//(Config.NIMBUS_SEEDS with the correct version of Storm - this is an interim workaround)
	
	/**
	 * Initialize the remote client.  Need the nimbus host:port and the transport plugin.
	 * Additionally, the nimbus client will attempt to find the storm.yaml or defaults.yaml
	 * config file on the classpath.  If this is bundled with storm-core it'll use the defaults
	 * in there.  If this is put out on a server you have to deploy a version there (or get the
	 * bundled storm-core/defaults.yaml on the classpath).
	 * 
	 * Need these params minimally:
	 * nimbus.host
	 * nimbus.thrift.port
	 * storm.thrift.transport
	 * storm.meta.serialization.delegate
	 * 
	 * @param config
	 */
	public RemoteStormController(Map<String, Object> config) {
		remote_config = config;
		logger.info("Connecting to remote storm: " + remote_config.toString() );
		client = NimbusClient.getConfiguredClient(remote_config).getClient();
	}
	
	/**
	 * Initialize the remote client.  Need the nimbus host:port and the transport plugin.
	 * Additionally, the nimbus client will attempt to find the storm.yaml or defaults.yaml
	 * config file on the classpath.  If this is bundled with storm-core it'll use the defaults
	 * in there.  If this is put out on a server you have to deploy a version there (or get the
	 * bundled storm-core/defaults.yaml on the classpath).
	 * 
	 * @param nimbus_host
	 * @param nimbus_thrift_port
	 * @param storm_thrift_transport_plugin typically "backtype.storm.security.auth.SimpleTransportPlugin"
	 */
	public RemoteStormController(Either<String, List<String>> nimbus_seeds, int nimbus_thrift_port, String storm_thrift_transport_plugin) {
		Map<String,Object> temp_config = new HashMap<String, Object>();
		nimbus_seeds.either(
				host -> temp_config.put(Config.NIMBUS_HOST, host), //HDP 2.2
				seeds -> temp_config.put(NIMBUS_SEEDS, seeds) // HDP 2.3
				);		
		temp_config.put(Config.NIMBUS_THRIFT_PORT, nimbus_thrift_port);
		temp_config.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, storm_thrift_transport_plugin);	
		temp_config.put(Config.STORM_META_SERIALIZATION_DELEGATE, "todo"); //TODO need to find the correct file for this, throws an error in the logs currently and loads a default
		remote_config = getConfigWithDefaults(temp_config);
		logger.info("Connecting to remote storm: " + remote_config.toString() );
		client = NimbusClient.getConfiguredClient(remote_config).getClient();
	}

	/**
	 * Submits a job to the remote storm cluster.  Sends the input jar to the server and then
	 * submits the supplied topology w/ the given job_name.
	 * 
	 */
	@Override
	public CompletableFuture<BasicMessageBean> submitJob(String job_name, String input_jar_location, StormTopology topology, Map<String, Object> config_override)
	{
		final CompletableFuture<BasicMessageBean> future = new CompletableFuture<BasicMessageBean>();
		logger.info("Submitting job: " + job_name + " jar: " + input_jar_location);
		logger.info("submitting jar");		
		
		final String remote_jar_location = StormSubmitter.submitJar(remote_config, input_jar_location);
//		final String json_conf = JSONValue.toJSONString(ImmutableMap.builder()
//					.putAll(remote_config)
//					.putAll(config_override)
//				.build());
		final String json_conf = JSONValue.toJSONString(mergeMaps(Arrays.asList(remote_config, config_override)));
		logger.info("submitting topology");
		try {
			synchronized (client) {
				client.submitTopology(job_name, remote_jar_location, json_conf, topology);
			}
			//verify job was assigned some executors
			final TopologyInfo info = getJobStats(job_name);
			logger.info("submitted job received: " + info.get_executors_size() + " executors");
			if ( info.get_executors_size() == 0 ) {
				logger.info("received 0 executors, killing job, reporting failure");
				//no executors were available for this job, stop the job, throw an error
				stopJob(job_name);
				
				future.complete(ErrorUtils.buildErrorMessage(this, "submitJob", "No executors were assigned to this job, typically this is because too many jobs are currently running, kill some other jobs and resubmit."));
				return future;					
			}
		} catch (Exception ex ) {
			logger.info( ErrorUtils.getLongForm("Error submitting job: " + job_name + ": {0}", ex));
			return FutureUtils.returnError(ex);
		}
		
		future.complete(ErrorUtils.buildSuccessMessage(this, "submitJob", "Submitted job successfully: " + job_name));
		return future;
	}

	/**
	 * Attempts to stop the given job_name, if the job is not found it will throw an exception.
	 * 
	 */
	@Override
	public CompletableFuture<BasicMessageBean> stopJob(String job_name) {
		logger.info("Stopping job: " + job_name);
		CompletableFuture<BasicMessageBean> future = new CompletableFuture<BasicMessageBean>();
		
		try {
			String actual_job_name = getJobTopologySummaryFromJobPrefix(job_name).get_name();
			if ( actual_job_name != null ) { 
				synchronized (client) {
					client.killTopology(actual_job_name);
				}
			}
		} catch (Exception ex) {
			//let die for now, usually happens when top doesn't exist
			logger.info( ErrorUtils.getLongForm("Error stopping job: " + job_name + "  this is typical with storm because the job may not exist that we try to kill {0}", ex));
			return FutureUtils.returnError(ex);
		}
		
		future.complete(ErrorUtils.buildSuccessMessage(this, "stopJob", "Stopped job successfully"));
		return future;
	}

	/**
	 * Grabs the topology info for the given job name.  First needs to get the
	 * job id from the job_name, then lookup the stats.
	 * 
	 */
	@Override
	public TopologyInfo getJobStats(String job_name) throws Exception {
		logger.info("Looking for stats for job: " + job_name);		
		String job_id = getJobTopologySummaryFromJobPrefix(job_name).get_id();
		logger.info("Looking for stats with id: " + job_id);
		if ( job_id != null ) {
			synchronized (client) {
				return client.getTopologyInfo(job_id);
			}
		}
		return null;
	}
	
	/** Gets a list of (aleph2-side) names for a given bucket
	 * @param bucket_path
	 * @return
	 */
	@Override 
	public List<String> getJobNamesForBucket(String bucket_path) {		
		return StormControllerUtil.getJobNamesForBucket(bucket_path, 
									Lambdas.wrap_u(() -> {
										synchronized (client) {
											return client.getClusterInfo();
										}
									}).get()
				);		
	}
	
	/**
	 * Tries to find a running topology using the job_prefix.  Tries to match on job_name.
	 * 
	 * @param job_prefix
	 * @return
	 * @throws TException
	 */
	private TopologySummary getJobTopologySummaryFromJobPrefix(String job_prefix) throws TException {
		ClusterSummary cluster_summary = Lambdas.wrap_u(() -> {
			synchronized (client) {
				return client.getClusterInfo();
			}
		}).get();
		Iterator<TopologySummary> iter = cluster_summary.get_topologies_iterator();
		 while ( iter.hasNext() ) {
			 TopologySummary summary = iter.next();
			 //WARNING: this just matches on prefix (because we don't know the unique jobid attached to the end of the job_name anymore)
			 //this means you can false positive if you have 2 jobs one a prefix of the other e.g. job_a and job_a_1
			 if ( summary.get_name().startsWith(job_prefix)) {
				 logger.info("found a matching job with name: " + summary);
				 return summary;
			 }
		 }	
		 logger.info("did not find existing job with prefix");
		 return null;
	}
	
	/**
	 * Merges any supplied config with the defaults.yaml found in storm-core.jar.  The latest versions
	 * of store-core expect to have more config values than we need to supply so we just send all the defaults.
	 * 
	 * @param input_conf
	 * @return
	 */
	private Map<String, Object> getConfigWithDefaults(final Map<String,Object> input_conf) {
		final InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(DEFAULT_STORM_CONFIG);
		Yaml yaml = new Yaml(new SafeConstructor());
        @SuppressWarnings("unchecked")
		Map<String, Object> default_storm = (Map<String,Object>) yaml.load(new InputStreamReader(is));
        return mergeMaps(Arrays.asList(default_storm, input_conf));
	}
	
	/**
	 * Merges any number of maps, later maps will overwrite any previous keys e.g. if maps[0].key1=5 and maps[2].key1=12
	 * the final map will have key1=12 because it'll overwrite the prior value.
	 * 
	 * Note: This does not use the google library ImmutableMap because they do not support
	 * null values and the defaults.yaml this is commonly used to load up has many null values.
	 * 
	 * @param maps
	 * @return
	 */
	private Map<String, Object> mergeMaps(final Collection<Map<String,Object>> maps) {		
		final Map<String,Object> map_to_return = new HashMap<String, Object>();
		maps.stream().forEach(map -> map_to_return.putAll(map));		
		return map_to_return;
	}
}
