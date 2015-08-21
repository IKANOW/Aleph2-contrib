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
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.utils.KafkaUtils;

/** An enrichment context service that just wraps the analytics service
 * @author Alex
 *
 */
public class StreamingEnrichmentContextService implements IEnrichmentModuleContext {
	protected IAnalyticsContext _delegate;
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
		_delegate = analytics_context;
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
		final String full_path = (_delegate.getServiceContext().getGlobalProperties().distributed_root_dir() + GlobalPropertiesBean.BUCKET_DATA_ROOT_OFFSET + my_bucket.full_name()).replace("//", "/");
		
		final ICoreDistributedServices cds = _delegate.getServiceContext().getService(ICoreDistributedServices.class, Optional.empty()).get();
		
		return _job.get().inputs().stream().flatMap(input -> {
			return Optional.of(input)
						.filter(i -> "stream".equalsIgnoreCase(i.data_service()))
						.map(i -> {					
							//TODO: Topic naming: 5 cases: 
							// 1) i.resource_name_or_id is a bucket path, ie starts with "/", and then:
							// 1.1) if it ends ":name" then it points to a specific point in the bucket processing
							// 1.2) if it ends ":" then it points to the start of that bucket's processing (ie the output of its harvester)
							// 1.3) otherwise it points to the end of the bucket's processing (ie immediately before it's output)
							// 2) i.resource_name_or_id does not, in which case
							// 2.1) if it's a non-empty string, then it's the name of  
							// 2.2) if it's "" or null then it's pointing to the output of its own bucket's harvester
							
							//TODO: add + ":" + blah if it's not the start of the queue
							
							final String topic_name = KafkaUtils.bucketPathToTopicName(my_bucket.full_name());
							cds.createTopic(topic_name);
							final SpoutConfig spout_config = new SpoutConfig(hosts, topic_name, full_path, my_bucket._id()); 
							spout_config.scheme = new SchemeAsMultiScheme(new StringScheme());
							final KafkaSpout kafka_spout = new KafkaSpout(spout_config);
							return Tuples._2T((T) kafka_spout, topic_name);			
						})
						// convert optional to stream:
						.map(Stream::of).orElseGet(Stream::empty)
						;
		})
		.collect(Collectors.toList());
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyStorageEndpoint(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> T getTopologyStorageEndpoint(final Class<T> clazz,
											final Optional<DataBucketBean> bucket)
	{
		if (!_job.get().output().is_transient()) { // Final output for this analytic
			// Just return an aleph2 output bolt:
			final DataBucketBean my_bucket = bucket.orElseGet(() -> _bucket.get());
			return (T) new OutputBolt(my_bucket, _delegate.getAnalyticsContextSignature(bucket, Optional.empty()), _user_topology.get().getClass().getName());			
		}
		else {
			final ICoreDistributedServices cds = _delegate.getServiceContext().getService(ICoreDistributedServices.class, Optional.empty()).get();			
			
			//TODO: if it's transient then return a kafka bolt using the name
			final String topic_name = "todo";
			cds.createTopic(topic_name);
			
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
		
		//TODO: actually this does need to be supported (see OutputBolt)
		// Also: 
		//every "N" seconds check if anyone has registered interest in my data and stream to an output queue as well if so
		// (this needs to get moved to somewhere more generic .. I think the idea was to use the core output library) 		
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
