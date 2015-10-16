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
import java.io.File;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.io.Files;
import com.google.inject.Inject;
import com.ikanow.aleph2.analytics.hadoop.assets.Aleph2MultiInputFormatBuilder;
import com.ikanow.aleph2.analytics.hadoop.assets.BatchEnrichmentJob;
import com.ikanow.aleph2.analytics.hadoop.assets.BeFileInputFormat;
import com.ikanow.aleph2.analytics.hadoop.assets.BeFileOutputFormat;
import com.ikanow.aleph2.analytics.hadoop.data_model.IBeJobService;
import com.ikanow.aleph2.analytics.hadoop.utils.HadoopTechnologyUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.UuidUtils;

import fj.data.Validation;

/** Responsible for launching the hadoop job
 * @author jfreydank
 */
public class BeJobLauncher implements IBeJobService{

	//TODO (ALEPH-12): sort out test spec
	
	private static final Logger logger = LogManager.getLogger(BeJobLauncher.class);

	protected Configuration _configuration;
	protected GlobalPropertiesBean _globals = null;

	protected String _yarnConfig = null;

	protected BatchEnrichmentContext _batchEnrichmentContext;

	/** User/guice c'tor
	 * @param globals
	 * @param beJobLoader
	 * @param batchEnrichmentContext
	 */
	@Inject
	public BeJobLauncher(GlobalPropertiesBean globals, BatchEnrichmentContext batchEnrichmentContext) {
		_globals = globals;	
		this._batchEnrichmentContext = batchEnrichmentContext;
	}
	
	/** 
	 * Override this function with system specific configuration
	 * @return
	 */
	public Configuration getHadoopConfig(){
		if(_configuration == null){
			_configuration = HadoopTechnologyUtils.getHadoopConfig(_globals);
		}
		return _configuration;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.analytics.hadoop.services.IBeJobService#runEnhancementJob(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public Validation<String, Job> runEnhancementJob(final DataBucketBean bucket, final Optional<ProcessingTestSpecBean> testSpec){
		
		final Configuration config = getHadoopConfig();
		
		final ClassLoader currentClassloader = Thread.currentThread().getContextClassLoader();
		//(not currently used, but has proven useful in the past)
		
		try {
			final String contextSignature = _batchEnrichmentContext.getEnrichmentContextSignature(Optional.of(bucket), Optional.empty()); 
		    config.set(BatchEnrichmentJob.BE_CONTEXT_SIGNATURE, contextSignature);
			
		    final Optional<Long> debug_max = 
		    		testSpec.flatMap(testSpecVals -> 
		    							Optional.ofNullable(testSpecVals.requested_num_objects()));
		    
		    //then gets applied to all the inputs:
		    debug_max.ifPresent(val -> config.set(BatchEnrichmentJob.BE_DEBUG_MAX_SIZE, val.toString()));
		    
		    final Aleph2MultiInputFormatBuilder inputBuilder = new Aleph2MultiInputFormatBuilder();

		    // Create a separate InputFormat for every input (makes testing life easier)
		    
			Optional.ofNullable(_batchEnrichmentContext.getJob().inputs())
						.orElse(Collections.emptyList())
					.stream()
					.forEach(Lambdas.wrap_consumer_u(input -> {
						final List<String> paths = _batchEnrichmentContext.getAnalyticsContext().getInputPaths(Optional.of(bucket), _batchEnrichmentContext.getJob(), input);
					    final Job inputJob = Job.getInstance(config);
					    inputJob.setInputFormatClass(BeFileInputFormat.class);				
						paths.stream().forEach(Lambdas.wrap_consumer_u(path -> FileInputFormat.addInputPath(inputJob, new Path(path))));
						inputBuilder.addInput(UuidUtils.get().getRandomUuid(), inputJob);
					}));
					;
					
			// (ALEPH-12): other input format types
			
		    // Now do everything else
		    
			final String jobName = BucketUtils.getUniqueSignature(bucket.full_name(), Optional.ofNullable(_batchEnrichmentContext.getJob().name()));
			
		    // do not set anything into config past this line (can set job.getConfiguration() elements though - that is what the builder does)
		    Job job = Job.getInstance(config, jobName);
		    job.setJarByClass(BatchEnrichmentJob.class);

		    // Set the classpath
		    
		    cacheJars(job, bucket, _batchEnrichmentContext.getAnalyticsContext());
		    
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
	
	/** Cache the system and user classpaths
	 * @param job
	 * @param context
	 * @throws IOException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws IllegalArgumentException 
	 */
	protected static void cacheJars(final Job job, final DataBucketBean bucket, final IAnalyticsContext context) throws IllegalArgumentException, InterruptedException, ExecutionException, IOException {
	    final FileContext fc = context.getServiceContext().getStorageService().getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
	    final String rootPath = context.getServiceContext().getStorageService().getRootPath();
	    
	    // Aleph2 libraries: need to cache them
	    context
	    	.getAnalyticsContextLibraries(Optional.empty())
	    	.stream()
	    	.map(f -> new File(f))
	    	.map(f -> Tuples._2T(f, new Path(rootPath + "/" + f.getName())))
	    	.map(Lambdas.wrap_u(f_p -> {
	    		final FileStatus fs = Lambdas.get(() -> {
	    			//TODO (ALEPH-12): need to clear out the cache intermittently
		    		try {
		    			return fc.getFileStatus(f_p._2());
		    		}
		    		catch (Exception e) { return null; }
	    		});
	    		if (null == fs) { //cache doesn't exist
	    			// Local version
	    			try (FSDataOutputStream outer = fc.create(f_p._2(), EnumSet.of(CreateFlag.CREATE), // ie should fail if the destination file already exists 
	    					org.apache.hadoop.fs.Options.CreateOpts.createParent()))
	    			{
	    				Files.copy(f_p._1(), outer.getWrappedStream());
	    			}
	    			catch (FileAlreadyExistsException e) {//(carry on - the file is versioned so it can't be out of date)
	    			}

	    			// This doens't work for some reason, get unsupported schema for "file" even though the config is set
//	    			Path srcPath = FileContext.getLocalFSFileContext().makeQualified(new Path(f_p._1().toString()));
//	    			try {
//	    				fc.util().copy(srcPath, f_p._2());
//	    			}
//	    			catch (FileAlreadyExistsException e) {//(carry on - the file is versioned so it can't be out of date)
//	    			}
	    		}
	    		return f_p._2();
	    	}))
	    	.forEach(Lambdas.wrap_consumer_u(path -> job.addFileToClassPath(path)));
	    	;
	    
	    // User libraries: this is slightly easier since one of the 2 keys
	    // is the HDFS path (the other is the _id)
	    context
			.getAnalyticsLibraries(Optional.of(bucket), bucket.analytic_thread().jobs())
			.get()
			.entrySet()
			.stream()
			.map(kv -> kv.getKey())
			.filter(path -> path.startsWith(rootPath))
			.forEach(Lambdas.wrap_consumer_u(path -> job.addFileToClassPath(new Path(path))));
			;	    		
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

