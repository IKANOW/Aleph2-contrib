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

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.analytics.storm.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean.StateDirectoryType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.SetOnce;

/** An enrichment context service that just wraps the analytics service
 * @author Alex
 *
 */
public class StreamingEnrichmentContextService implements IEnrichmentModuleContext {
	protected IAnalyticsContext _delegate;
	protected SetOnce<DataBucketBean> _bucket;
	protected SetOnce<AnalyticThreadJobBean> _job;
	
	/** User constructor - in technology
	 * @param analytics_context - the context to wrap
	 * @param bucket - the bucket being processed
	 * @param job - the job being processed
	 */
	public StreamingEnrichmentContextService(final IAnalyticsContext analytics_context, final DataBucketBean bucket, final AnalyticThreadJobBean job)
	{
		_delegate = analytics_context;
		_bucket.set(bucket);
		_job.set(job);
	}
	
	/** User constructor - in module
	 *  All the fields get added by the initializeContext call
	 */
	public StreamingEnrichmentContextService()
	{
		//(nothing to do, see above)
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return _delegate.getUnderlyingArtefacts();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(final Class<T> driver_class, final Optional<String> driver_options) {
		if (IAnalyticsContext.class.isAssignableFrom(driver_class)) {
			return (Optional<T>) Optional.of(_delegate);
		}
		return _delegate.getUnderlyingPlatformDriver(driver_class, driver_options);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getEnrichmentContextSignature(java.util.Optional, java.util.Optional)
	 */
	@Override
	public String getEnrichmentContextSignature(final Optional<DataBucketBean> bucket,
												final Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services)
	{
		return _delegate.getClass().getName() + ":" + _delegate.getAnalyticsContextSignature(bucket, services);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyEntryPoints(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> Collection<Tuple2<T, String>> getTopologyEntryPoints(
												final Class<T> clazz, 
												final Optional<DataBucketBean> bucket)
	{
		// TODO OK this is actually a non trivial case here - wants to be the actual inputs from the actual job
		return null;
	}

	@Override
	public <T> T getTopologyStorageEndpoint(final Class<T> clazz,
											final Optional<DataBucketBean> bucket)
	{
		// TODO OK this is actually a non trivial case here - wants to be the actual output for the actual job
		return null;
	}

	@Override
	public <T> T getTopologyErrorEndpoint(Class<T> clazz, Optional<DataBucketBean> bucket) {
		// TODO OK this is actually a non trivial case here - wants to be the actual output for the actual job
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getNextUnusedId()
	 */
	@Override
	public long getNextUnusedId() {
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_CALL_FROM_WRAPPED_ANALYTICS_CONTEXT_ENRICH, "getNextUnusedId"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#convertToMutable(com.fasterxml.jackson.databind.JsonNode)
	 */
	@Override
	public ObjectNode convertToMutable(final JsonNode original) {
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_CALL_FROM_WRAPPED_ANALYTICS_CONTEXT_ENRICH, "convertToMutable"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emitMutableObject(long, com.fasterxml.jackson.databind.node.ObjectNode, java.util.Optional)
	 */
	@Override
	public void emitMutableObject(final long id, final ObjectNode mutated_json, final Optional<AnnotationBean> annotation) {
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_CALL_FROM_WRAPPED_ANALYTICS_CONTEXT_ENRICH, "emitMutableObject"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emitImmutableObject(long, com.fasterxml.jackson.databind.JsonNode, java.util.Optional, java.util.Optional)
	 */
	@Override
	public void emitImmutableObject(final long id, final JsonNode original_json, final Optional<ObjectNode> mutations, final Optional<AnnotationBean> annotations) {
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_CALL_FROM_WRAPPED_ANALYTICS_CONTEXT_ENRICH, "emitImmutableObject"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#storeErroredObject(long, com.fasterxml.jackson.databind.JsonNode)
	 */
	@Override
	public void storeErroredObject(final long id, final JsonNode original_json) {
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_CALL_FROM_WRAPPED_ANALYTICS_CONTEXT_ENRICH, "storeErroredObject"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getServiceContext()
	 */
	@Override
	public IServiceContext getServiceContext() {
		return _delegate.getServiceContext();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucket()
	 */
	@Override
	public Optional<DataBucketBean> getBucket() {
		return _delegate.getBucket();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getLibraryConfig()
	 */
	@Override
	public SharedLibraryBean getLibraryConfig() {
		return _delegate.getLibraryConfig();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucketStatus(java.util.Optional)
	 */
	@Override
	public Future<DataBucketStatusBean> getBucketStatus(
			final Optional<DataBucketBean> bucket) {
		return _delegate.getBucketStatus(bucket);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#logStatusForBucketOwner(java.util.Optional, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean, boolean)
	 */
	@Override
	public void logStatusForBucketOwner(Optional<DataBucketBean> bucket,
			BasicMessageBean message, boolean roll_up_duplicates) {
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "logStatusForBucketOwner"));
		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#logStatusForBucketOwner(java.util.Optional, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean)
	 */
	@Override
	public void logStatusForBucketOwner(Optional<DataBucketBean> bucket,
			BasicMessageBean message) {
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "logStatusForBucketOwner"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emergencyDisableBucket(java.util.Optional)
	 */
	@Override
	public void emergencyDisableBucket(Optional<DataBucketBean> bucket) {
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "emergencyDisableBucket"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emergencyQuarantineBucket(java.util.Optional, java.lang.String)
	 */
	@Override
	public void emergencyQuarantineBucket(Optional<DataBucketBean> bucket,
			String quarantine_duration) {
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "emergencyQuarantineBucket"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#initializeNewContext(java.lang.String)
	 */
	@Override
	public void initializeNewContext(String signature) {
		try {
			final String[] sig_options = signature.split(":");
			_delegate = (IAnalyticsContext) Class.forName(sig_options[0]).newInstance();		
			_delegate.initializeNewContext(sig_options[1]);
		}
		catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getGlobalEnrichmentModuleObjectStore(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <S> ICrudService<S> getGlobalEnrichmentModuleObjectStore(
			final Class<S> clazz, final Optional<String> collection)
	{
		return _delegate.getGlobalAnalyticTechnologyObjectStore(clazz, collection);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucketObjectStore(java.lang.Class, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	@Override
	public <S> ICrudService<S> getBucketObjectStore(final Class<S> clazz,
			final Optional<DataBucketBean> bucket, final Optional<String> collection,
			final Optional<StateDirectoryType> type)
	{
		Optional<StateDirectoryType> translated_type = Optional.ofNullable(type.orElse(StateDirectoryType.enrichment));
		return _delegate.getBucketObjectStore(clazz, bucket, collection, translated_type);
	}

}
