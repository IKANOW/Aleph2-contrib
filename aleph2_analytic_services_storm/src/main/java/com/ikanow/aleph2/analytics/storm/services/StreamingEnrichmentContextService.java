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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.ISpout;
import backtype.storm.spout.SchemeAsMultiScheme;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.analytics.storm.assets.OutputBolt;
import com.ikanow.aleph2.analytics.storm.assets.TransientStreamingOutputBolt;
import com.ikanow.aleph2.analytics.storm.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingTopology;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean.StateDirectoryType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.utils.KafkaUtils;

import fj.data.Either;

/** An enrichment context service that just wraps the analytics service
 * @author Alex
 *
 */
public class StreamingEnrichmentContextService implements IEnrichmentModuleContext {
	protected static final Logger _logger = LogManager.getLogger();	
	
	protected final SetOnce<IAnalyticsContext> _delegate = new SetOnce<>();
	protected final SetOnce<IEnrichmentStreamingTopology> _user_topology = new SetOnce<>();
	protected final SetOnce<AnalyticThreadJobBean> _job = new SetOnce<>();
	protected final SetOnce<SharedLibraryBean> _module = new SetOnce<>();
	
	/** User constructor - in technology
	 * @param analytics_context - the context to wrap
	 * @param bucket - the bucket being processed
	 * @param job - the job being processed
	 */
	public StreamingEnrichmentContextService(final IAnalyticsContext analytics_context)
	{
		_state_name = State.IN_TECHNOLOGY;
		_delegate.trySet(analytics_context);
	}
	
	/** User constructor - in module
	 *  All the fields get added by the initializeContext call
	 */
	public StreamingEnrichmentContextService()
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
	
	/** Test function for setting the user topology
	 * @param bucket
	 */
	@SuppressWarnings("deprecation")
	public void setUserTopology(final IEnrichmentStreamingTopology user_topology) {
		_user_topology.forceSet(user_topology);		
	}

	/** Test function for setting the analytic job
	 * @param bucket
	 */
	@SuppressWarnings("deprecation")
	public void setJob(final AnalyticThreadJobBean job) {
		_job.forceSet(job);
		setModule(null); //unset first)
		Optional.ofNullable(job.module_name_or_id()).map(name -> _delegate.get().getLibraryConfigs().get(name)).ifPresent(lib -> setModule(lib));
	}
	
	/** Test function for setting the specific module being processed
	 *  Auto set by setJob, this overrides it
	 * @param _bucket
	 */
	@SuppressWarnings("deprecation")
	public void setModule(final SharedLibraryBean module) {
		_module.forceSet(module);		
	}
	
	/** (FOR TESTING) returns the analytics context delegate
	 * @return
	 */
	IAnalyticsContext getAnalyticsContext() {
		return _delegate.get();
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
	public <T> Optional<T> getUnderlyingPlatformDriver(final Class<T> driver_class, final Optional<String> driver_options) {
		if (IAnalyticsContext.class.isAssignableFrom(driver_class)) {
			return (Optional<T>) Optional.of(_delegate.get());
		}
		return _delegate.get().getUnderlyingPlatformDriver(driver_class, driver_options);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getEnrichmentContextSignature(java.util.Optional, java.util.Optional)
	 */
	@Override
	public String getEnrichmentContextSignature(final Optional<DataBucketBean> bucket,
												final Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services)
	{
		return this.getClass().getName() + ":" + _job.get().name() + ":" + _delegate.get().getAnalyticsContextSignature(bucket, services);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyEntryPoints(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Collection<Tuple2<T, String>> getTopologyEntryPoints(
												final Class<T> clazz, 
												final Optional<DataBucketBean> bucket)
	{
		if (_state_name == State.IN_TECHNOLOGY) {
			if (!ISpout.class.isAssignableFrom(clazz)) {
				throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_TOPOLOGY_CLASSES, clazz));
			}
			final DataBucketBean my_bucket = bucket.orElseGet(() -> _bucket.get());
			final BrokerHosts hosts = new ZkHosts(KafkaUtils.getZookeperConnectionString());
			final String full_path = (_delegate.get().getServiceContext().getGlobalProperties().distributed_root_dir() + GlobalPropertiesBean.BUCKET_DATA_ROOT_OFFSET + my_bucket.full_name()).replace("//", "/");
			
			return Optionals.ofNullable(_job.get().inputs()).stream()
					.flatMap(input -> _delegate.get().getInputTopics(bucket, _job.get(), input).stream())
					.map(topic_name -> {
						_logger.debug("Created input topic for topology entry point: " + topic_name + " for bucket " + my_bucket.full_name());
						
						final SpoutConfig spout_config = new SpoutConfig(hosts, topic_name, full_path, BucketUtils.getUniqueSignature(my_bucket.full_name(), Optional.of(_job.get().name()))); 
						spout_config.scheme = new SchemeAsMultiScheme(new StringScheme());
						final KafkaSpout kafka_spout = new KafkaSpout(spout_config);
						return Tuples._2T((T) kafka_spout, topic_name);
					})
					.collect(Collectors.toList())
					;
		}
		else {
			throw new RuntimeException(ErrorUtils.TECHNOLOGY_NOT_MODULE);			
		}
		//TODO (ALEPH-12): More sophisticated spout building functionality (eg generic batch->storm checking via CRUD service), handle storage service possibly via Camus?
		
		//TODO (ALEPH-12): if a legit data service is specified then see if that service contains a spout and if so use that, else throw error
		//TODO: (if a legit data service is specified then need to ensure that the service is included in the underlying artefacts)
		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyStorageEndpoint(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> T getTopologyStorageEndpoint(final Class<T> clazz,
											final Optional<DataBucketBean> bucket)
	{
		if (_state_name == State.IN_TECHNOLOGY) {
			final DataBucketBean my_bucket = bucket.orElseGet(() -> _bucket.get());
			if (!Optionals.of(() -> _job.get().output().is_transient()).orElse(false)) { // Final output for this analytic
				//TODO (ALEPH-12): handle child-buckets 
				
				// Just return an aleph2 output bolt:
				return (T) new OutputBolt(my_bucket, this.getEnrichmentContextSignature(bucket, Optional.empty()), _user_topology.get().getClass().getName());			
			}
			else {
				final Optional<String> topic_name = _delegate.get().getOutputTopic(bucket, _job.get());
				
				if (topic_name.isPresent()) {
					final ICoreDistributedServices cds = _delegate.get().getServiceContext().getService(ICoreDistributedServices.class, Optional.empty()).get();			
					cds.createTopic(topic_name.get(), Optional.empty());
					return (T) new TransientStreamingOutputBolt(my_bucket, _job.get(), _delegate.get().getAnalyticsContextSignature(bucket, Optional.empty()), _user_topology.get().getClass().getName(), topic_name.get());
				}
				else { //TODO (ALEPH-12): Write an output bolt for temporary HDFS storage, and another one for both
					throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "batch output from getTopologyStorageEndpoint"));
				}
			}
		}
		else {
			throw new RuntimeException(ErrorUtils.TECHNOLOGY_NOT_MODULE);			
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyErrorEndpoint(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> T getTopologyErrorEndpoint(Class<T> clazz, Optional<DataBucketBean> bucket) {
		if (_state_name == State.IN_TECHNOLOGY) {
			throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "getTopologyErrorEndpoint"));
		}
		else {
			throw new RuntimeException(ErrorUtils.TECHNOLOGY_NOT_MODULE);			
		}
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
		return (ObjectNode) original;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emitMutableObject(long, com.fasterxml.jackson.databind.node.ObjectNode, java.util.Optional)
	 */
	@Override
	public void emitMutableObject(final long id, final ObjectNode mutated_json, final Optional<AnnotationBean> annotation, final Optional<JsonNode> grouping_fields) {		
		_delegate.get().emitObject(_delegate.get().getBucket(), _job.get(), Either.left((JsonNode) mutated_json), annotation);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emitImmutableObject(long, com.fasterxml.jackson.databind.JsonNode, java.util.Optional, java.util.Optional)
	 */
	@Override
	public void emitImmutableObject(final long id, final JsonNode original_json, final Optional<ObjectNode> mutations, final Optional<AnnotationBean> annotations, final Optional<JsonNode> grouping_fields) {
		if (annotations.isPresent()) {
			throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);			
		}
		if (grouping_fields.isPresent()) {
			throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);			
		}
		final JsonNode to_emit = 
				mutations.map(o -> StreamSupport.<Map.Entry<String, JsonNode>>stream(Spliterators.spliteratorUnknownSize(o.fields(), Spliterator.ORDERED), false)
									.reduce(original_json, (acc, kv) -> ((ObjectNode) acc).set(kv.getKey(), kv.getValue()), (val1, val2) -> val2))
									.orElse(original_json);
		
		emitMutableObject(0L, (ObjectNode)to_emit, annotations, Optional.empty());
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
			final Optional<DataBucketBean> bucket) {
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

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#initializeNewContext(java.lang.String)
	 */
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

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#flushBatchOutput(java.util.Optional)
	 */
	@Override
	public CompletableFuture<?> flushBatchOutput(Optional<DataBucketBean> bucket) {
		return _delegate.get().flushBatchOutput(bucket, _job.get());
	}

}
