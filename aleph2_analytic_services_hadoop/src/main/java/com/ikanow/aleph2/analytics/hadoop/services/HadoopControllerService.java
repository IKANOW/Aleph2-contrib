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
package com.ikanow.aleph2.analytics.hadoop.services;

import java.io.File;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.analytics.hadoop.data_model.BasicHadoopConfigBean;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.SetOnce;

/** Starts and stops hadoop jobs
 * @author alex
 */
public class HadoopControllerService {

	/** Launches the specified Hadoop job
	 * @param job_name - the job name (in practice derived from the bucketigur
	 * @param bucket - bucket, for context
	 * @param jobs - the entire list of jobs for this technology, for context
	 * @param job - the actual job to launch
	 * @param config - the hadoop config corresponding to the job's processing params
	 * @param context
	 * @param local_mode_override - if true then runs locally
	 * @return a message indicating success/failure
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public BasicMessageBean startJob(
			final String job_name, 
			final DataBucketBean bucket, 
			final Collection<AnalyticThreadJobBean> jobs, final AnalyticThreadJobBean job,
			final BasicHadoopConfigBean config, 
			final IAnalyticsContext context, 
			boolean local_mode_override)
	{
		try {
			final ObjectMapper json_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
			final GlobalPropertiesBean globals = context.getServiceContext().getGlobalProperties();
			
			final  Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services = Optional.empty();
			//TODO: need to get additional services depending on inputs
			final String context_signature = context.getAnalyticsContextSignature(Optional.of(bucket), services);
			final Configuration base_config = getConfig(globals, context_signature);
	
			//TODO: setup InputFormat/OutputFormat classes, using job
		
			Job task = Job.getInstance(base_config, job_name);
			
			//TODO: need to keep track of input classes 
			
			//TODO: also want to chain config so you get the last object's config
			//(or should we have an explicit param for that?)
			
			final SetOnce<Void> mappers_present = new SetOnce<>();
			Optionals.ofNullable(config.mappers()).stream()
				.filter(mapper -> Optional.ofNullable(mapper.enabled()).orElse(true))
				.forEach(mapper -> {
					mappers_present.set(null);
					final Configuration mapper_config = getConfig(globals, context_signature);
					if (null != mapper.config_json()) {
						mapper_config.set(BasicHadoopConfigBean.CONFIG_JSON_MAPPER, json_mapper.convertValue(mapper.config_json(), String.class));
					}
					if (null != mapper.config_string()) {
						mapper_config.set(BasicHadoopConfigBean.CONFIG_STRING_MAPPER, mapper.config_string());
					}
					//<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
					//TODO: what should the input class type be (Object.class currently)...
					// ... if we want to handle video then might need to be tuple2 of JsonNode and stream
					//TODO: 
//					ChainMapper.addMapper(task, Class.forName(mapper.entry_point()), 
//							String.class, Object.class, outputKeyClass, outputValueClass, mapper_config);
				});
			
			if (!mappers_present.isSet()) {
				return ErrorUtils.buildErrorMessage(this, "startJob", "Found no mappers bucket={0} job={1} config={2}",
						bucket.full_name(),
						BeanTemplateUtils.toJson(job).toString(),
						BeanTemplateUtils.toJson(config)
						);				
			}
			
			//(Do combiner last of all since will often just use the reducer as the combiner)
			
			final SetOnce<Void> combiner_present = new SetOnce<>();
			Optional.ofNullable(config.combiner())
				.filter(combiner -> Optional.ofNullable(combiner.enabled()).orElse(true))
				.ifPresent(Lambdas.wrap_consumer_u(combiner -> {
					combiner_present.set(null);
					//TODO: handle the "use as reducer" case based on 
					if (null != combiner.config_json()) {
						base_config.set(BasicHadoopConfigBean.CONFIG_JSON_COMBINER, json_mapper.convertValue(combiner.config_json(), String.class));
					}
					if (null != combiner.config_string()) {
						base_config.set(BasicHadoopConfigBean.CONFIG_STRING_COMBINER, combiner.config_string());
					}
					
					task.setCombinerClass((Class<? extends Reducer>)(Class<?>)Class.forName(combiner.entry_point()));
				}));

			
			//TODO: combiner (hmm can't have a different config for the combiner .. will have to use the param for that)
			
			//TODO: reducer (hmm can't have a different config for the reducer .. will have to use the param for that)
			
			return null;
		}
		catch (Throwable t) {
			return ErrorUtils.buildErrorMessage(this, "startJob", ErrorUtils.getLongForm("{0}", t));
		}
	}
	
	public BasicMessageBean stopJob(final String job_name) {
		return null;
	}
	
	public Tuple2<Integer, BasicMessageBean> checkJob(final String job_name) {
		return null;
	}

	//////////////////////////////////////////////////////////////////

	// TODO have a validate in here
	
	//////////////////////////////////////////////////////////////////
	
	// UTILITIES
	
	/** Get the general configuration object before job specific variables
	 * @param context
	 * @return
	 */
	protected Configuration getConfig(final GlobalPropertiesBean globals, final String context_signature){
		final Configuration configuration = new Configuration(false);
		
		if (new File(globals.local_yarn_config_dir()).exists()) {
			configuration.addResource(new Path(globals.local_yarn_config_dir() +"/core-site.xml"));
			configuration.addResource(new Path(globals.local_yarn_config_dir() +"/yarn-site.xml"));
			configuration.addResource(new Path(globals.local_yarn_config_dir() +"/hdfs-site.xml"));
			configuration.addResource(new Path(globals.local_yarn_config_dir() +"/hadoop-site.xml"));
			configuration.addResource(new Path(globals.local_yarn_config_dir() +"/mapred-site.xml"));
		}
		// These are not added by Hortonworks, so add them manually
		configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");									
		configuration.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
		// Some other config defaults:
		// (not sure if these are actually applied, or derived from the defaults - for some reason they don't appear in CDH's client config)
		configuration.set("mapred.reduce.tasks.speculative.execution", "false");
		
		// The most basic Aleph2 setting:
		configuration.set(BasicHadoopConfigBean.CONTEXT_SIGNATURE, context_signature);
		
		return configuration;
	}
	
}
