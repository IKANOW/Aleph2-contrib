/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.analytics.hadoop.services;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Inject;
import com.ikanow.aleph2.analytics.hadoop.assets.Aleph2MultiInputFormatBuilder;
import com.ikanow.aleph2.analytics.hadoop.assets.BatchEnrichmentJob;
import com.ikanow.aleph2.analytics.hadoop.assets.BeFileInputFormat;
import com.ikanow.aleph2.analytics.hadoop.assets.BeFileOutputFormat;
import com.ikanow.aleph2.analytics.hadoop.data_model.BeJobBean;
import com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobService;
import com.ikanow.aleph2.analytics.hadoop.utils.HadoopAnalyticTechnologyUtils;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.UuidUtils;

import fj.data.Validation;

/** Responsible for launching the hadoop job
 * @author jfreydank
 */
public class BeJobLauncher implements IBeJobService{

	private static final Logger logger = LogManager.getLogger(BeJobLauncher.class);

	protected Configuration _configuration;
	protected GlobalPropertiesBean _globals = null;

	protected BeJobLoader _beJobLoader;

	protected String _yarnConfig = null;

	protected BatchEnrichmentContext _batchEnrichmentContext;

	/** User/guice c'tor
	 * @param globals
	 * @param beJobLoader
	 * @param batchEnrichmentContext
	 */
	@Inject
	public BeJobLauncher(GlobalPropertiesBean globals, BeJobLoader beJobLoader, BatchEnrichmentContext batchEnrichmentContext) {
		_globals = globals;	
		this._beJobLoader = beJobLoader;
		this._batchEnrichmentContext = batchEnrichmentContext;
	}
	
	/** 
	 * Override this function with system specific configuration
	 * @return
	 */
	public Configuration getHadoopConfig(){
		if(_configuration == null){
			_configuration = HadoopAnalyticTechnologyUtils.getHadoopConfig(_globals);
		}
		return _configuration;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.analytics.hadoop.services.IBeJobService#runEnhancementJob(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public Validation<String, Job> runEnhancementJob(DataBucketBean bucket){
		
		final Configuration config = getHadoopConfig();
		
		final ClassLoader currentClassloader = Thread.currentThread().getContextClassLoader();
		//(not currently used, but has proven useful in the past)
		
		try {
			final BeJobBean beJob = _beJobLoader.loadBeJob(bucket, "TODO");		

			final String contextSignature = _batchEnrichmentContext.getEnrichmentContextSignature(Optional.of(bucket), Optional.empty()); 
		    config.set(BatchEnrichmentJob.BE_CONTEXT_SIGNATURE, contextSignature);
			
		    final Aleph2MultiInputFormatBuilder inputBuilder = new Aleph2MultiInputFormatBuilder();

		    // All the hadoop paths, can put into a single format
		    
			final List<String> all_hdfs_paths = 
					Optionals.ofNullable(_batchEnrichmentContext.getJob().inputs()).stream()
						.flatMap(input -> _batchEnrichmentContext.getAnalyticsContext().getInputPaths(Optional.of(bucket), _batchEnrichmentContext.getJob(), input).stream())
						.collect(Collectors.toList());
					;
		    
			if (!all_hdfs_paths.isEmpty()) {
			    final Job inputJob = Job.getInstance(config);
			    inputJob.setInputFormatClass(BeFileInputFormat.class);				
				all_hdfs_paths.stream().forEach(Lambdas.wrap_consumer_u(path -> FileInputFormat.addInputPath(inputJob, new Path(beJob.getBucketInputPath()))));
				inputBuilder.addInput(UuidUtils.get().getRandomUuid(), inputJob);
			}
					
			// (ALEPH-12): other input format types
			
		    // Now do everything else
		    
			final String jobName = BucketUtils.getUniqueSignature(bucket.full_name(), Optional.ofNullable(_batchEnrichmentContext.getJob().name()));
			
		    // do not set anything into config past this line (can set job.getConfiguration() elements though - that is what the builder does)
		    Job job = Job.getInstance(config, jobName);
		    job.setJarByClass(BatchEnrichmentJob.class);

		    //TODO (ALEPH-12): set the classpath...

		    // (generic mapper - the actual code is run using the classes in the shared libraries)
		    job.setMapperClass(BatchEnrichmentJob.BatchEnrichmentMapper.class);
		    
		    //TODO: ALEPH-12 handle reducer scenarios
		    job.setNumReduceTasks(0);
		    //job.setReducerClass(BatchEnrichmentJob.BatchEnrichmentReducer.class);

		    // Input format:
		    inputBuilder.build(job);

			// Output format (doesn't really do anything, all the actual output code is performed by the mapper via the enrichment context)
		    job.setOutputFormatClass(BeFileOutputFormat.class);
			
			launch(job);
			return Validation.success(job);
			
		} 
		catch (Throwable t) {
			logger.error("Caught Exception",t);
			return Validation.fail(ErrorUtils.getLongForm("{0}", t));
		} 
		finally {
			Thread.currentThread().setContextClassLoader(currentClassloader);
		}
	     		
	}
	
	/** Launches the job
	 * @param job
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void launch(Job job) throws ClassNotFoundException, IOException, InterruptedException{
		job.submit();		
	}

}

