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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.analytics.hadoop.assets.BeFileInputReader;
import com.ikanow.aleph2.analytics.hadoop.utils.HadoopErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
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
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;

/** The implementation of the batch  enrichment context
 * @author Joern
 */
public class BatchEnrichmentContext implements IEnrichmentModuleContext {
	static final Logger _logger = LogManager.getLogger(BatchEnrichmentContext.class);

	protected final SetOnce<IAnalyticsContext> _delegate = new SetOnce<>();
	protected final SetOnce<AnalyticThreadJobBean> _job = new SetOnce<>();
	protected final SetOnce<SharedLibraryBean> _module = new SetOnce<>();
	
	//(list of records to emit)
	protected final AtomicLong _mutable_1up = new AtomicLong(0);
	protected ArrayList<Tuple2<Long, IBatchRecord>> _mutable_records = new ArrayList<>();
	
	/** User constructor - in technology
	 * @param analytics_context - the context to wrap
	 * @param _bucket - the bucket being processed
	 * @param job - the job being processed
	 */
	public BatchEnrichmentContext(final IAnalyticsContext analytics_context)
	{
		_state_name = State.IN_TECHNOLOGY;
		_delegate.trySet(analytics_context);
	}
	
	/** Copy constructor - to clone and then override the _module
	 * @param enrichment_context
	 */
	public BatchEnrichmentContext(final BatchEnrichmentContext enrichment_context, int batch_size) {		
		_state_name = State.IN_MODULE;		
		_delegate.set(enrichment_context._delegate.get());
		_job.set(enrichment_context._job.get());
		_mutable_records.ensureCapacity(batch_size);
	}
	
	/** User constructor - in module
	 *  All the fields get added by the initializeContext call
	 */
	public BatchEnrichmentContext()
	{
		//(nothing else to do, see above)
		_state_name = State.IN_MODULE;		
	}
	
	///////////////////////////////////////////////////////
	
	// SOME TEST METHODS/ARTEFACTS
	
	public enum State { IN_TECHNOLOGY, IN_MODULE };
	protected final State _state_name; // (duplicate of delegate)	
	
	protected class BucketChain { // (duplicate of delegate)
		protected final SetOnce<DataBucketBean> _bucket = new SetOnce<>();
		@SuppressWarnings("deprecation")
		public void override(DataBucketBean bucket) {
			_bucket.forceSet(bucket);
		}
		public DataBucketBean get() {
			return _bucket.isSet() ? _bucket.get() : _delegate.get().getBucket().get();			
		}
	}
	protected final BucketChain _bucket = new BucketChain();
	
	/** Test function for setting the bucket
	 * @param bucket
	 */
	public void setBucket(final DataBucketBean bucket) {
		_bucket.override(bucket);		
	}
	
	/** Test function for setting the analytic job
	 * @param _bucket
	 */
	@SuppressWarnings("deprecation")
	public void setJob(final AnalyticThreadJobBean job) {
		_job.forceSet(job);		
		setModule(null); //(unset first)
		Optional.ofNullable(job.module_name_or_id()).map(name -> _delegate.get().getLibraryConfigs().get(name)).ifPresent(lib -> setModule(lib));
	}
	
	/** Test function for setting the specific module being processed
	 * @param _bucket
	 */
	@SuppressWarnings("deprecation")
	public void setModule(final SharedLibraryBean module) {
		_module.forceSet(module);		
	}
	
	/** (FOR TESTING) returns the analytics context delegate
	 * @return
	 */
	public IAnalyticsContext getAnalyticsContext() {
		return _delegate.get();
	}
	
	/** Returns the currently active job
	 * @return
	 */
	public AnalyticThreadJobBean getJob() {
		return _job.get();
	}
	
	/** Returns the last batch of outputs
	 * @return
	 */
	public ArrayList<Tuple2<Long, IBatchRecord>> getOutputRecords() {
		return _mutable_records;
	}
	
	/** Clears the current set of output records
	 */
	public void clearOutputRecords() {
		_mutable_records.clear();
	}
	
	///////////////////////////////////////////////////////
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		// All this, and me also!
		return java.util.stream.Stream.concat(Arrays.asList(this).stream(), _delegate.get().getUnderlyingArtefacts().stream()).collect(Collectors.toList());
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		if (IAnalyticsContext.class.isAssignableFrom(driver_class)) {
			return (Optional<T>) Optional.of(_delegate.get());
		}
		return _delegate.get().getUnderlyingPlatformDriver(driver_class, driver_options);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getEnrichmentContextSignature(java.util.Optional, java.util.Optional)
	 */
	@Override
	public String getEnrichmentContextSignature(
			Optional<DataBucketBean> bucket,
			Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
		return this.getClass().getName() + ":" + _job.get().name() + ":" + _delegate.get().getAnalyticsContextSignature(bucket, services);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyEntryPoints(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> Collection<Tuple2<T, String>> getTopologyEntryPoints(
			Class<T> clazz, Optional<DataBucketBean> bucket) {
		throw new RuntimeException(HadoopErrorUtils.BATCH_TOPOLOGIES_NOT_YET_SUPPORTED);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyStorageEndpoint(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> T getTopologyStorageEndpoint(Class<T> clazz,
			Optional<DataBucketBean> bucket) {
		throw new RuntimeException(HadoopErrorUtils.BATCH_TOPOLOGIES_NOT_YET_SUPPORTED);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyErrorEndpoint(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> T getTopologyErrorEndpoint(Class<T> clazz,
			Optional<DataBucketBean> bucket) {
		throw new RuntimeException(HadoopErrorUtils.BATCH_TOPOLOGIES_NOT_YET_SUPPORTED);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getNextUnusedId()
	 */
	@Override
	public long getNextUnusedId() {
		// TODO (ALEPH-12): don't 100% recall the way this was supposed to work
		// but it was something like .. for each batch this would be the index, submitted across all jobs that might
		// be running in parallel .. so then adding a mutation to an object could be managed across multiple concurrent jobs
		// ... since currently it all runs in serial, i don't think it's needed ... probably should generate some locally unique _id though?
		// otherwise someone might try to use it as an "id" in their logic...
		return 0;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#convertToMutable(com.fasterxml.jackson.databind.JsonNode)
	 */
	@Override
	public ObjectNode convertToMutable(JsonNode original) {
		//(this is v simple until we need to manage mutations to the same object across multiple threads_
		return (ObjectNode) original;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emitMutableObject(long, com.fasterxml.jackson.databind.node.ObjectNode, java.util.Optional)
	 */
	@Override
	public void emitMutableObject(long id, ObjectNode mutated_json,
			Optional<AnnotationBean> annotation) {
		if (annotation.isPresent()) {
			throw new RuntimeException(ErrorUtils.get(HadoopErrorUtils.NOT_YET_IMPLEMENTED, "annotations"));			
		}
		_mutable_records.add(Tuples._2T(_mutable_1up.incrementAndGet(), new BeFileInputReader.BatchRecord(mutated_json, null)));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emitImmutableObject(long, com.fasterxml.jackson.databind.JsonNode, java.util.Optional, java.util.Optional)
	 */
	@Override
	public void emitImmutableObject(long id, JsonNode original_json,
			Optional<ObjectNode> mutations, Optional<AnnotationBean> annotations) {
		final JsonNode to_emit = 
				mutations.map(o -> StreamSupport.<Map.Entry<String, JsonNode>>stream(Spliterators.spliteratorUnknownSize(o.fields(), Spliterator.ORDERED), false)
									.reduce(original_json, (acc, kv) -> ((ObjectNode) acc).set(kv.getKey(), kv.getValue()), (val1, val2) -> val2))
									.orElse(original_json);
		
		emitMutableObject(0L, (ObjectNode)to_emit, annotations);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#storeErroredObject(long, com.fasterxml.jackson.databind.JsonNode)
	 */
	@Override
	public void storeErroredObject(long id, JsonNode original_json) {
		throw new RuntimeException(ErrorUtils.get(HadoopErrorUtils.NOT_YET_IMPLEMENTED, "storeErroredObject"));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getServiceContext()
	 */
	@Override
	public IServiceContext getServiceContext() {
		return _delegate.get().getServiceContext();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucket()
	 */
	@Override
	public Optional<DataBucketBean> getBucket() {
		return _delegate.get().getBucket();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getLibraryConfig()
	 */
	@Override
	public Optional<SharedLibraryBean> getModuleConfig() {
		return _module.isSet()
				? Optional.ofNullable(_module.get())
				: Optional.empty()
				;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucketStatus(java.util.Optional)
	 */
	@Override
	public Future<DataBucketStatusBean> getBucketStatus(
			Optional<DataBucketBean> bucket) {
		return _delegate.get().getBucketStatus(bucket);
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

	@Override
	public void initializeNewContext(String signature) {
		try {
			final String[] sig_options = signature.split(":", 3);
			// job_name:delegate:config
			_delegate.trySet((IAnalyticsContext) Class.forName(sig_options[1]).newInstance());		
			_delegate.get().initializeNewContext(sig_options[2]);
			
			// OK now get the job and set it (must exist by construction):
			Optionals.of(() -> 
				_delegate.get().getBucket().get().analytic_thread().jobs()
					.stream().filter(j -> j.name().equals(sig_options[0])).findFirst().get())
					.ifPresent(j -> {
						this.setJob(j);
					});
		}
		catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getGlobalEnrichmentModuleObjectStore(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <S> Optional<ICrudService<S>> getGlobalEnrichmentModuleObjectStore(
			final Class<S> clazz, final Optional<String> collection)
	{
		return _module.isSet()
				? _delegate.get().getLibraryObjectStore(clazz, _module.get().path_name(), collection)
				: Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucketObjectStore(java.lang.Class, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	@Override
	public <S> ICrudService<S> getBucketObjectStore(final Class<S> clazz,
			final Optional<DataBucketBean> bucket, final Optional<String> collection,
			final Optional<StateDirectoryType> type)
	{
		// Translate default to enrichment, and handle bucket store being the module not the analytic technology
		if (type.isPresent() && (StateDirectoryType.library == type.get())) {
			throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_CONFIG_ERROR, "getBucketObjectStore", "library"));
		}
		else {
			Optional<StateDirectoryType> translated_type = Optional.ofNullable(type.orElse(StateDirectoryType.enrichment));
			return _delegate.get().getBucketObjectStore(clazz, bucket, collection, translated_type);
		}
	}

}
