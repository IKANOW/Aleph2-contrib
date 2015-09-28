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

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ikanow.aleph2.analytics.hadoop.data_model.BeJobBean;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;


/** Responsible for getting a bucket from the management DB, caching all its library elements, etc
 * @author jfreydank
 */
public class BeJobLoader {
	protected static final Logger logger = LogManager.getLogger(BeJobLoader.class);

	protected final BatchEnrichmentContext _enrichmentContext;
	
	/** Guice/user c'tor
	 * @param serviceContext
	 */
	public BeJobLoader(BatchEnrichmentContext context)
	{
		this._enrichmentContext = context;
	}
	
	/** Copies all the information needed inside Hadoop to construct the requested job
	 * @param bucket
	 * @param config_element
	 * @return
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public BeJobBean loadBeJob(DataBucketBean bucket, String configElement) throws InterruptedException, ExecutionException {
		
		// Get all the shared libraries:
		
		final String baseBucketPath = _enrichmentContext.getServiceContext().getGlobalProperties().local_root_dir()
									+ bucket.full_name();
		
		final Map<String, String> sharedLibraries = _enrichmentContext.getAnalyticsContext().getAnalyticsLibraries(Optional.of(bucket), Arrays.asList(_enrichmentContext.getJob())).get();
		
		//TODO: (ALEPH-12) shouldn't this come from the context?!
		
		final String bucketInPath = baseBucketPath + IStorageService.TO_IMPORT_DATA_SUFFIX;
		
		final BeJobBean beJob = new BeJobBean(bucket, configElement, sharedLibraries, baseBucketPath, bucketInPath);		
		
		return beJob;
	}

}
