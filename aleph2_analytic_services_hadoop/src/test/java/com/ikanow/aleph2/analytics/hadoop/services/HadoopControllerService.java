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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.InputFormat;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.analytics.hadoop.assets.Aleph2MultipleInputFormatBuilder;
import com.ikanow.aleph2.analytics.hadoop.data_model.BasicHadoopConfigBean;
import com.ikanow.aleph2.analytics.hadoop.utils.HadoopErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
//import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils.BasicMessageException;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;

import fj.Unit;
import fj.data.Validation;

/** Starts and stops hadoop jobs
 *  CURRENTLY ON ICE WHILE I GET THE SIMPLER ENRICHMENT MODULE BASED SERVICE WORKING
 * @author alex
 */
public class HadoopControllerService {
	private static final ObjectMapper _json_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	
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
	public BasicMessageBean startJob(
			final String job_name, 
			final DataBucketBean bucket, 
			final Collection<AnalyticThreadJobBean> jobs, final AnalyticThreadJobBean job,
			final BasicHadoopConfigBean config, 
			final IAnalyticsContext context, 
			final boolean local_mode_override
			)
	{
		return buildJob(job_name, bucket, jobs, job, config, context, local_mode_override, false)
					.validation(
							fail -> fail
							,
							task -> {
								//TODO launch task and return success
								return null;
							}
							);
	}
	public BasicMessageBean stopJob(final String job_name) {
		return null;
	}
	
	public Tuple2<Integer, BasicMessageBean> checkJob(final String job_name) {
		return null;
	}

	//////////////////////////////////////////////////////////////////

	// TODO have a validate in here that just calls the buildJob but without actually doing anything
	
	//////////////////////////////////////////////////////////////////
	
	// JOB BUILDING UTILITIES
	
	public interface IHadoopInputContext extends IAnalyticsAccessContext<InputFormat<?, ?>> {}	
	
	//TODO split out into build input/processing/output
	
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
	public static Validation<BasicMessageBean, Job> buildJob(
			final String job_name, 
			final DataBucketBean bucket, 
			final Collection<AnalyticThreadJobBean> jobs, final AnalyticThreadJobBean job,
			final BasicHadoopConfigBean config, 
			final IAnalyticsContext context, 
			final boolean local_mode_override, final boolean evaluate_only
			)
	{
		try {
			final GlobalPropertiesBean globals = context.getServiceContext().getGlobalProperties();
			// (don't think we need this ironically, always get from the context)
			//final IStorageService storage_service = context.getServiceContext().getStorageService();
			
			final Aleph2MultipleInputFormatBuilder input_format_builder = new Aleph2MultipleInputFormatBuilder();
			
			final  Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services = Optional.empty();
			//TODO: need to get additional services depending on inputs
			final String context_signature = context.getAnalyticsContextSignature(Optional.of(bucket), services);
			final Configuration base_config = getConfig(globals, context_signature);

			//TODO: distributed cache for required JAR files (use context) .. don't actually do if evaluate_only
			
			Job task = Job.getInstance(base_config, job_name);

			// Input Format
			
			Optionals.ofNullable(job.inputs()).stream()
				.filter(input -> Optional.ofNullable(input.enabled()).orElse(true))
				.forEach(input -> {
					//TODO (ALEPH-12): need to look out for self-join cases since that means the output format is more complex (can't delete self..)
					
					// Basically: is it an external input or an internal one
					// TODO (ALEPH-12) later on support other data services
					
					if (null == input.resource_name_or_id()) {
						throw new BasicMessageException(ErrorUtils.buildErrorMessage(HadoopControllerService.class.getName(), "startJob", HadoopErrorUtils.MISSING_REQUIRED_FIELD, "resource_name_or_id", bucket.full_name()));
					}
					if (input.resource_name_or_id().startsWith("/")) { // it's an external bucket
						//TODO (ALEPH-12): need to check have accesss to this...
						//TODO (ALEPH-12) could also be a local file system, worry about security etc etc
						
						// OK then we're going to point at a data service so check that exists ... if it's not specified it will be treated as the HDFS
						final String dataservice_name = Optional.ofNullable(input.data_service()).orElse("storage_service");
						final String[] dataservice_name_array = dataservice_name.split(":");
						final String data_service = dataservice_name_array[0];
						final Optional<String> name = Optional.of(dataservice_name_array.length).filter(len -> len > 1).map(__ -> dataservice_name_array[1]);
						final String key = BucketUtils.getUniqueSignature(input.resource_name_or_id(), Optional.of(dataservice_name));
						
						if (name.isPresent()) {
							throw new BasicMessageException(ErrorUtils.buildErrorMessage(HadoopControllerService.class.getName(), "startJob", 
									ErrorUtils.NOT_YET_IMPLEMENTED, ErrorUtils.get("non-default service: {0}:{1}", data_service, name.get())));							
						}
						
						if (data_service.equals("storage_service")) { // (special case since we know we're using HDFS for the storage service...)
							final Optional<List<Path>> base_paths = Optional.of(context.getInputPaths(Optional.of(bucket), job, input))
																			.map(x -> x.stream().map(s -> new Path(s)).collect(Collectors.toList()));
							base_paths.orElseThrow(() -> 
									new BasicMessageException(ErrorUtils.buildErrorMessage(HadoopControllerService.class.getName(), "startJob", 
											ErrorUtils.NOT_YET_IMPLEMENTED, ErrorUtils.get("No base paths: bucket = {0} input = {1}", bucket.full_name(), BeanTemplateUtils.toJson(input).toString()))));
							
							final Class<? extends InputFormat> input_format_clazz = null; //TODO (ALEPH-12): get from context or exception out?
							input_format_builder.addInput(key, task, input_format_clazz, Collections.emptyMap(), base_paths);
						}
						else {		
							context.getServiceInput(IHadoopInputContext.class, Optional.of(bucket), job, input)
								.map(input_format -> { // See if the service has an actual implementation they want to give me
									final Class<InputFormat<?, ?>> input_format_clazz = input_format.getAccessService().right().value();										
									final Map<String, Object> input_format_config = input_format.getAccessConfig().orElse(Collections.emptyMap());
									
									return Tuples._2T(input_format_clazz, input_format_config);
								})
								.map(Optional::of)
								.orElseGet(() -> { // They don't, so go to the backstop generic CRUD access
									final Optional<IAnalyticsAccessContext.GenericCrudAccessContext> input_format_backup = 
											context.getServiceInput(IAnalyticsAccessContext.GenericCrudAccessContext.class, Optional.of(bucket), job, input);
											;
									
									return input_format_backup
												.map(input_format -> {
													
													final Class<InputFormat<?, ?>> input_format_clazz = null; //TODO (ALEPH-12): build class using this" input_format.getAccessService().left().value();
													final Map<String, Object> input_format_config = input_format.getAccessConfig().orElse(Collections.emptyMap());
													return Tuples._2T(input_format_clazz, input_format_config);
												})
												.map(Optional::of)
											.orElseGet(Optional::empty); // drops down to the throw)
								})
								.map(t2 -> { // We've gotten an input format here
									input_format_builder.addInput(key, task, t2._1(), t2._2(), Optional.empty());
									return Unit.unit(); // (don't actually care about the result as long as it's non null)		
								})
								.orElseThrow(() -> 
									new BasicMessageException(ErrorUtils.buildErrorMessage(HadoopControllerService.class.getName(), "startJob", 
											ErrorUtils.NOT_YET_IMPLEMENTED, ErrorUtils.get("missing data service: {0}", data_service)))
								)
								;
						}
					}
					else { // it's an internal dependency
						//TODO (ALEPH-12): If it's a streaming dependency then will need to batch the streams (or just fail?)
						
						// So assuming it's a batch dependency, we'll grab the directory as a FileInputFormat 
					
						final String key = BucketUtils.getUniqueSignature(bucket.full_name(), Optional.of(input.resource_name_or_id()));
						final Optional<List<Path>> base_paths = Optional.of(context.getInputPaths(Optional.of(bucket), job, input))
																	.map(x -> x.stream().map(s -> new Path(s)).collect(Collectors.toList()));
						final Class<? extends InputFormat> input_format_clazz = null; //TODO
						input_format_builder.addInput(key, task, input_format_clazz, Collections.emptyMap(), base_paths);
					}
					
					//For reference, the various things to use
//					input.config();
//					input.config().new_data_only();
//					input.config().self_join();
//					input.config().size_batch_kb();
//					input.config().size_batch_records();
//					input.config().timed_batch_ms();
//					input.config().time_max();
//					input.config().time_min();
//					input.data_service();
//					input.filter(); // (this is a JSON object that makes sense to the data service)
//					input.resource_name_or_id();					
				});
			;
			
			//TODO: need to keep track of input classes 
			
			//TODO: also want to chain config so you get the last object's config
			//(or should we have an explicit param for that?)
			
			final SetOnce<Void> mappers_present = new SetOnce<>();
			Optionals.ofNullable(config.mappers()).stream()
				.filter(mapper -> Optional.ofNullable(mapper.enabled()).orElse(true))
				.forEach(mapper -> {
					mappers_present.set(null);
					final Configuration mapper_config = getConfig(globals, context_signature);
					fillInConfiguration(mapper_config, mapper, Tuples._2T(BasicHadoopConfigBean.CONFIG_JSON_MAPPER, BasicHadoopConfigBean.CONFIG_STRING_MAPPER));
					//<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
					//TODO: what should the input class type be (Object.class currently)...
					// ... if we want to handle video then might need to be tuple2 of JsonNode and stream
					//TODO: 
//					ChainMapper.addMapper(task, Class.forName(mapper.entry_point()), 
//							String.class, Object.class, outputKeyClass, outputValueClass, mapper_config);
				});
			
			if (!mappers_present.isSet()) {
				return Validation.fail(ErrorUtils.buildErrorMessage(HadoopControllerService.class.getName(), "startJob", HadoopErrorUtils.NO_MAPPERS,
							bucket.full_name(),
							BeanTemplateUtils.toJson(job).toString(),
							BeanTemplateUtils.toJson(config)
							));				
			}
			
			//TODO: need to push all the right stuff into the distributed cache...
			
			//(Do combiner last of all since will often just use the reducer as the combiner)
			
			final SetOnce<Void> combiner_present = new SetOnce<>();
			Optional.ofNullable(config.combiner())
				.filter(combiner -> Optional.ofNullable(combiner.enabled()).orElse(true))
				.ifPresent(Lambdas.wrap_consumer_u(combiner -> {
					combiner_present.set(null);
					//TODO: handle the "use as reducer" case based on entry point
					fillInConfiguration(base_config, combiner, Tuples._2T(BasicHadoopConfigBean.CONFIG_JSON_COMBINER, BasicHadoopConfigBean.CONFIG_STRING_COMBINER));
					task.setCombinerClass((Class<? extends Reducer>)(Class<?>)Class.forName(combiner.entry_point()));
				}));

			
			//TODO: combiner (hmm can't have a different config for the combiner .. will have to use the param for that)
			
			//TODO: reducer (hmm can't have a different config for the reducer .. will have to use the param for that)
			
			//TODO: set up OutputFormat ... note delete the data if the "do something" flag is set
			
			return null;
		}
		catch (BasicMessageException b) {
			return Validation.fail(b.getMessageBean());
		}
		catch (Throwable t) {
			return Validation.fail(ErrorUtils.buildErrorMessage(HadoopControllerService.class.getName(), "startJob", ErrorUtils.getLongForm("{0}", t)));
		}
	}
	
	//////////////////////////////////////////////////////////////////
	
	// LOW LEVEL UTILITIES
	
	protected static void fillInConfiguration(final Configuration config, final BasicHadoopConfigBean.Step step, final Tuple2<String, String> jsonfield_stringfield) {
		if (null != step.config_json()) {
			config.set(jsonfield_stringfield._1(), _json_mapper.convertValue(step.config_json(), String.class));
		}
		if (null != step.config_string()) {
			config.set(jsonfield_stringfield._2(), step.config_string());
		}
		if (null != step.internal_config()) {
			step.internal_config().entrySet().forEach(kv -> config.set(kv.getKey(), kv.getValue()));
		}		
	}
	
	/** Get the general configuration object before job specific variables
	 * @param _context
	 * @return
	 */
	protected static Configuration getConfig(final GlobalPropertiesBean globals, final String context_signature){
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
