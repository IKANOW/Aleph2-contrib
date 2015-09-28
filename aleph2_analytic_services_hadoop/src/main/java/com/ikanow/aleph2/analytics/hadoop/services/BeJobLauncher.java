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

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Inject;
import com.ikanow.aleph2.analytics.hadoop.assets.BatchEnrichmentJob;
import com.ikanow.aleph2.analytics.hadoop.assets.BeFileInputFormat;
import com.ikanow.aleph2.analytics.hadoop.assets.BeFileOutputFormat;
import com.ikanow.aleph2.analytics.hadoop.data_model.BeJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BucketUtils;

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
	public Configuration getConf(){
		if(_configuration == null){
			this._configuration = new Configuration(false);
		
			if (new File(_globals.local_yarn_config_dir()).exists()) {
				_configuration.addResource(new Path(_globals.local_yarn_config_dir() +"/core-site.xml"));
				_configuration.addResource(new Path(_globals.local_yarn_config_dir() +"/yarn-site.xml"));
				_configuration.addResource(new Path(_globals.local_yarn_config_dir() +"/hdfs-site.xml"));
				_configuration.addResource(new Path(_globals.local_yarn_config_dir() +"/hadoop-site.xml"));
				_configuration.addResource(new Path(_globals.local_yarn_config_dir() +"/mapred-site.xml"));
			}
			// These are not added by Hortonworks, so add them manually
			_configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");									
			_configuration.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
			// Some other config defaults:
			// (not sure if these are actually applied, or derived from the defaults - for some reason they don't appear in CDH's client config)
			_configuration.set("mapred.reduce.tasks.speculative.execution", "false");
		}
		return _configuration;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.analytics.hadoop.services.IBeJobService#runEnhancementJob(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public String runEnhancementJob(DataBucketBean bucket, String configElement){
		
		Configuration config = getConf();
		String jobName = null;
		try {
			
			BeJobBean beJob = _beJobLoader.loadBeJob(bucket, configElement);		

			String contextSignature = _batchEnrichmentContext.getEnrichmentContextSignature(Optional.of(bucket), Optional.empty()); 
		    config.set(BatchEnrichmentJob.BE_CONTEXT_SIGNATURE, contextSignature);
			
			jobName = BucketUtils.getUniqueSignature(bucket.full_name(), Optional.of(configElement));
			
			// set metadata bean to job jik we need to have more config, bean is included in bucket data but needs to be identified
			config.set(BatchEnrichmentJob.BE_META_BEAN_PARAM, configElement);

		    // do not set anything into config past this line
		    Job job = Job.getInstance( config ,jobName);
		    job.setJarByClass(BatchEnrichmentJob.class);

		    //TODO: set the classpath...

		    job.setMapperClass(BatchEnrichmentJob.BatchEnrichmentMapper.class);
		    job.setNumReduceTasks(0);
		    
		    //TODO: ALEPH-12 handle reducer scenarios
		  //  job.setReducerClass(BatchEnrichmentJob.BatchEnrichmentReducer.class);
		    
		    job.setInputFormatClass(BeFileInputFormat.class);

			// Output format:
		    job.setOutputFormatClass(BeFileOutputFormat.class);


		    Path inPath = new Path(beJob.getBucketInputPath());
		    logger.debug("Bucket Input Path:"+inPath.toString());
			FileInputFormat.addInputPath(job, inPath);
			
			launch(job);
			
		} catch (Exception e) {
			logger.error("Caught Exception",e);
		} 
		return jobName;
	     		
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

