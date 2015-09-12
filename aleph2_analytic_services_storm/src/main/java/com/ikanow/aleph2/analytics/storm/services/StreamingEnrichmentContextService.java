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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;
import backtype.storm.spout.ISpout;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Tuple;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.analytics.storm.assets.OutputBolt;
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
import com.ikanow.aleph2.data_model.utils.Lambdas;
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
	protected final SetOnce<IAnalyticsContext> _delegate = new SetOnce<>();
	protected final SetOnce<IEnrichmentStreamingTopology> _user_topology = new SetOnce<>();
	protected final SetOnce<DataBucketBean> _bucket = new SetOnce<>();
	protected final SetOnce<AnalyticThreadJobBean> _job = new SetOnce<>();
	
	/** User constructor - in technology
	 * @param analytics_context - the context to wrap
	 * @param bucket - the bucket being processed
	 * @param job - the job being processed
	 */
	public StreamingEnrichmentContextService(final IAnalyticsContext analytics_context, final IEnrichmentStreamingTopology user_topology, final DataBucketBean bucket, final AnalyticThreadJobBean job)
	{
		_delegate.trySet(analytics_context);
		_user_topology.set(user_topology);
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
		return _delegate.get().getUnderlyingArtefacts();
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
		return _delegate.get().getUnderlyingPlatformDriver(driver_class, driver_options);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getEnrichmentContextSignature(java.util.Optional, java.util.Optional)
	 */
	@Override
	public String getEnrichmentContextSignature(final Optional<DataBucketBean> bucket,
												final Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services)
	{
		return _delegate.get().getClass().getName() + ":" + _delegate.get().getAnalyticsContextSignature(bucket, services);
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
		if (!ISpout.class.isAssignableFrom(clazz)) {
			throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_TOPOLOGY_CLASSES, clazz));
		}
		final DataBucketBean my_bucket = bucket.orElseGet(() -> _bucket.get());
		final BrokerHosts hosts = new ZkHosts(KafkaUtils.getZookeperConnectionString());
		final String full_path = (_delegate.get().getServiceContext().getGlobalProperties().distributed_root_dir() + GlobalPropertiesBean.BUCKET_DATA_ROOT_OFFSET + my_bucket.full_name()).replace("//", "/");
		
		final ICoreDistributedServices cds = _delegate.get().getServiceContext().getService(ICoreDistributedServices.class, Optional.empty()).get();
		
		return _job.get().inputs().stream().flatMap(input -> {
			return Optional.of(input)
						.filter(i -> "stream".equalsIgnoreCase(i.data_service()))
						.map(i -> {					
							//Topic naming: 5 cases: 
							// 1) i.resource_name_or_id is a bucket path, ie starts with "/", and then:
							// 1.1) if it ends ":name" then it points to a specific point in the bucket processing
							// 1.2) if it ends ":" or ":$start" then it points to the start of that bucket's processing (ie the output of its harvester) .. which corresponds to the queue name with no sub-channel
							// 1.3) otherwise it points to the end of the bucket's processing (ie immediately before it's output) ... which corresponds to the queue name with the sub-channel "$end"
							// 2) i.resource_name_or_id does not (start with a /), in which case:
							// 2.1) if it's a non-empty string, then it's the name of one the internal jobs (can interpret that as this.full_name + name)  
							// 2.2) if it's "" or null then it's pointing to the output of its own bucket's harvester
							
							final String[] bucket_subchannel = Lambdas.<String, String[]> wrap_u(s -> {
								if (s.startsWith("/")) { //1.*
									if (s.endsWith(":") || s.endsWith(":$start")) {
										return new String[] { s.substring(0, s.length() - 1), "" }; // (1.2)
									}
									else {
										final String[] b_sc = s.split(":");
										if (1 == b_sc.length) {
											return new String[] { b_sc[0], "$end" }; // (1.3)
										}
										else {
											return b_sc; //(1.1)
										}
									}
								}
								else { //2.*
									return new String[] { my_bucket.full_name(), s };
								}
							})
							.apply(i.resource_name_or_id())
							;
							
							final String topic_name = cds.generateTopicName(bucket_subchannel[0], Optional.ofNullable(bucket_subchannel[1]).filter(s -> !s.isEmpty()));
							//TODO: ALEPH-12, register interest in a topic via ZK, which will (somehow!) result in data being streamed from that job (eg via checking ZK every minute)
							//(+ spout config needs to have a different consumer name...)
							cds.createTopic(topic_name, Optional.empty());
							final SpoutConfig spout_config = new SpoutConfig(hosts, topic_name, full_path, BucketUtils.getUniqueSignature(my_bucket.full_name(), Optional.of(_job.get().name()))); 
							spout_config.scheme = new SchemeAsMultiScheme(new StringScheme());
							final KafkaSpout kafka_spout = new KafkaSpout(spout_config);
							return Tuples._2T((T) kafka_spout, topic_name);			
						})
						// convert optional to stream:
						.map(Stream::of).orElseGet(Stream::empty)
						;
		})
		.collect(Collectors.toList());

		//TODO (ALEPH-12): More sophisticated spout building functionality (eg generic batch->storm checking via CRUD service)
		
		//TODO: if a legit data service is specified then see if that service contains a spout and if so use that, else throw error
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
		final DataBucketBean my_bucket = bucket.orElseGet(() -> _bucket.get());
		if (!_job.get().output().is_transient()) { // Final output for this analytic
			// Just return an aleph2 output bolt:
			return (T) new OutputBolt(my_bucket, _delegate.get().getAnalyticsContextSignature(bucket, Optional.empty()), _user_topology.get().getClass().getName());			
		}
		else {
			final ICoreDistributedServices cds = _delegate.get().getServiceContext().getService(ICoreDistributedServices.class, Optional.empty()).get();			
			final String topic_name = cds.generateTopicName(my_bucket.full_name(), Optional.of(my_bucket.streaming_enrichment_topology().name()));
			cds.createTopic(topic_name, Optional.empty());
			
			return (T) new KafkaBolt<String, String>().withTopicSelector(__ -> topic_name).withTupleToKafkaMapper(new TupleToKafkaMapper<String, String>() {
				private static final long serialVersionUID = -1651711778714775009L;
				public String getKeyFromTuple(final Tuple tuple) {
					return null; // (no key, will randomly assign across partition)
				}
				public String getMessageFromTuple(final Tuple tuple) {
					return  _user_topology.get().rebuildObject(tuple, OutputBolt::tupleToLinkedHashMap).toString();
				}
			});
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyErrorEndpoint(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> T getTopologyErrorEndpoint(Class<T> clazz, Optional<DataBucketBean> bucket) {
		throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "getTopologyErrorEndpoint"));
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
		_delegate.get().emitObject(_delegate.get().getBucket(), _job.get(), Either.left((JsonNode) mutated_json));
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
	public SharedLibraryBean getLibraryConfig() {
		return _delegate.get().getLibraryConfig();
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
			final String[] sig_options = signature.split(":");
			_delegate.trySet((IAnalyticsContext) Class.forName(sig_options[0]).newInstance());		
			_delegate.get().initializeNewContext(sig_options[1]);
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
		return _delegate.get().getGlobalAnalyticTechnologyObjectStore(clazz, collection);
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
		return _delegate.get().getBucketObjectStore(clazz, bucket, collection, translated_type);
	}

}
