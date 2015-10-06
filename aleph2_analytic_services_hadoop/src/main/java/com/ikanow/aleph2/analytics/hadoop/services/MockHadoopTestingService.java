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
package com.ikanow.aleph2.analytics.hadoop.services;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.core.shared.utils.DirUtils;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;

/** Harness service for easily running Hadoop tests
 * @author alex
 */
public class MockHadoopTestingService {

	protected final IServiceContext _service_context;

	protected final HashSet<String> _cleared_dirs = new HashSet<>();
	
	/** User c'tor
	 * @param service_context
	 */
	public MockHadoopTestingService(IServiceContext service_context) {
		_service_context = service_context;
	}
	
	/** Submit a test bucket with exactly one analytic job
	 * @param test_bucket
	 * @param service_context
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public CompletableFuture<BasicMessageBean> testAnalyticModule(final DataBucketBean test_bucket) throws InterruptedException, ExecutionException {

		// (Clear ES index)
		final IDataWriteService<JsonNode> es_index = 
				_service_context.getSearchIndexService().get().getDataService().get().getWritableDataService(JsonNode.class, test_bucket, Optional.empty(), Optional.empty()).get();
		es_index.deleteDatastore().get();

		final Optional<AnalyticThreadJobBean> job = Optionals.of(() -> test_bucket.analytic_thread().jobs().get(0));
		if (!job.isPresent()) {
			throw new RuntimeException("Bucket must have one analytic thread");
		}
		
		//create dummy libary:
		final SharedLibraryBean library = BeanTemplateUtils.build(SharedLibraryBean.class)
		.with(SharedLibraryBean::path_name, "/test/lib")
		.done().get();
								
		// Context		
		final MockAnalyticsContext test_analytics_context = new MockAnalyticsContext(_service_context);
		test_analytics_context.setBucket(test_bucket);
		test_analytics_context.setTechnologyConfig(library);
				
		//PHASE 2: CREATE TOPOLOGY AND SUBMit
		
		final MockHadoopTechnologyService analytic_tech = new MockHadoopTechnologyService();
		return analytic_tech.startAnalyticJob(test_bucket, Arrays.asList(job.get()), job.get(), test_analytics_context);
	}
	
	/** Adds the contents of the InputStream to a file in the bucket's storage service
	 * @param local_stream
	 * @param bucket
	 * @param subservice_suffix
	 * @param date
	 * @throws IOException
	 */
	public void addFileToBucketStorage(final InputStream local_stream, final DataBucketBean bucket, String subservice_suffix, Optional<Date> date) throws IOException {
		final FileContext fileContext = _service_context.getStorageService().getUnderlyingPlatformDriver(FileContext.class,Optional.empty()).get();
		final String bucketPath1 = _service_context.getStorageService().getBucketRootPath() + bucket.full_name();
		
		if (!_cleared_dirs.contains(bucketPath1)) {
			FileUtils.deleteQuietly(new File(bucketPath1)); // (cleanse the dir to start with)
			_cleared_dirs.add(bucketPath1);
		}
		
		final String bucketReadyPath1 = bucketPath1 + subservice_suffix + date.map(d -> DateTimeFormatter.ofPattern("yyyy-MM-dd").format(d.toInstant())).orElse("");
		DirUtils.createDirectory(fileContext,bucketReadyPath1);
		DirUtils.createUTF8File(fileContext,bucketReadyPath1+"/data.json", new StringBuffer(IOUtils.toString(local_stream)));
	}

	/** Adds the contents of the InputStream to a file in the bucket's batch input path
	 * @param local_stream
	 * @param bucket
	 * @throws IOException
	 */
	public void addFileToInputDirectory(final InputStream local_stream, final DataBucketBean bucket) throws IOException {
		addFileToBucketStorage(local_stream, bucket, IStorageService.TO_IMPORT_DATA_SUFFIX, Optional.empty());
	}
	
	/** Adds the contents of the InputStream to a file in the bucket's transient batch job output
	 * @param local_stream
	 * @param bucket
	 * @param job
	 * @throws IOException
	 */
	public void addFileToTransientJobOutput(final InputStream local_stream, final DataBucketBean bucket, String job) throws IOException {
		addFileToBucketStorage(local_stream, bucket, IStorageService.TRANSIENT_DATA_SUFFIX + job, Optional.empty());
	}

	/** Returns the number of files in the designated bucket's storage service
	 * @param bucket
	 * @param subservice_suffix
	 * @param date
	 * @return
	 */
	public int numFilesInBucketStorage(final DataBucketBean bucket, final String subservice_suffix, Optional<Date> date) {
		final String bucketPath1 = _service_context.getStorageService().getBucketRootPath() + bucket.full_name();
		final String bucketReadyPath1 = bucketPath1 + subservice_suffix + date.map(d -> DateTimeFormatter.ofPattern("yyyy-MM-dd").format(d.toInstant())).orElse("");
		return Optional.ofNullable(new File(bucketReadyPath1).listFiles()).orElseGet(() -> new File[0]).length;
	}

	/** Returns the number of files in the designated bucket's batch input path
	 * @param bucket
	 * @return
	 */
	public int numFilesInInputDirectory(final DataBucketBean bucket) {
		return numFilesInBucketStorage(bucket, IStorageService.TO_IMPORT_DATA_SUFFIX, Optional.empty());
	}
	
	/** Returns the number of files in the designated bucket's transient batch job output
	 * @param bucket
	 * @param job
	 * @return
	 */
	public int isFileInTransientJobOutput(final DataBucketBean bucket, final String job) {
		return numFilesInBucketStorage(bucket, IStorageService.TRANSIENT_DATA_SUFFIX + job, Optional.empty());
	}
	
	/** Returns the number of records in the bucket's storage service
	 * @param bucket
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public long getNumRecordsInSearchIndex(final DataBucketBean bucket) throws InterruptedException, ExecutionException {
		final IDataWriteService<JsonNode> es_index = 
				_service_context.getSearchIndexService().get().getDataService().get().getWritableDataService(JsonNode.class, bucket, Optional.empty(), Optional.empty()).get();
		return es_index.countObjects().get().longValue();
	}
}
