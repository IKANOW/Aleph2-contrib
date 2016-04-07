/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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

package com.ikanow.aleph2.graph.titan.services;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean.StateDirectoryType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

import fj.data.Either;
import fj.data.Validation;

/** A wrapper for IEnrichmentContext that allows the user to decompose the outgoing data objects into edges and vertices
 * @author Alex
 *
 */
public class GraphDecompEnrichmentContext implements IEnrichmentModuleContext {

	private List<JsonNode> _mutable_vertices = new LinkedList<>();
	
	////////////////////////////////////////////////////////////////////////////
	
	// GRAPH SPECIFIC INTERFACE
	
	/** Returns all the emitted objects then resets the list ready for the next batch
	 * @return
	 */
	public List<JsonNode> getAndResetVertexList() {
		final List<JsonNode> ret_val = _mutable_vertices;
		_mutable_vertices = new LinkedList<JsonNode>();
		return ret_val;
	}
	
	////////////////////////////////////////////////////////////////////////////
	
	// IEnrichmentModuleContext INTERFACE
	
	/**
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	public Collection<Object> getUnderlyingArtefacts() {
		return _delegate.getUnderlyingArtefacts();
	}
	/**
	 * @param driver_class
	 * @param driver_options
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return _delegate.getUnderlyingPlatformDriver(driver_class,
				driver_options);
	}
	/**
	 * @param bucket
	 * @param services
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getEnrichmentContextSignature(java.util.Optional, java.util.Optional)
	 */
	public String getEnrichmentContextSignature(
			Optional<DataBucketBean> bucket,
			Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
		return _delegate.getEnrichmentContextSignature(bucket, services);
	}
	/**
	 * @param clazz
	 * @param bucket
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyEntryPoints(java.lang.Class, java.util.Optional)
	 */
	public <T> Collection<Tuple2<T, String>> getTopologyEntryPoints(
			Class<T> clazz, Optional<DataBucketBean> bucket) {
		return _delegate.getTopologyEntryPoints(clazz, bucket);
	}
	/**
	 * @param clazz
	 * @param bucket
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyStorageEndpoint(java.lang.Class, java.util.Optional)
	 */
	public <T> T getTopologyStorageEndpoint(Class<T> clazz,
			Optional<DataBucketBean> bucket) {
		return _delegate.getTopologyStorageEndpoint(clazz, bucket);
	}
	/**
	 * @param clazz
	 * @param bucket
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyErrorEndpoint(java.lang.Class, java.util.Optional)
	 */
	public <T> T getTopologyErrorEndpoint(Class<T> clazz,
			Optional<DataBucketBean> bucket) {
		return _delegate.getTopologyErrorEndpoint(clazz, bucket);
	}
	/**
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getNextUnusedId()
	 */
	public long getNextUnusedId() {
		return _delegate.getNextUnusedId();
	}
	/**
	 * @param original
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#convertToMutable(com.fasterxml.jackson.databind.JsonNode)
	 */
	public ObjectNode convertToMutable(JsonNode original) {
		return _delegate.convertToMutable(original);
	}
	/**
	 * @param id
	 * @param mutated_json
	 * @param annotations
	 * @param grouping_key
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emitMutableObject(long, com.fasterxml.jackson.databind.node.ObjectNode, java.util.Optional, java.util.Optional)
	 */
	public Validation<BasicMessageBean, JsonNode> emitMutableObject(long id,
			ObjectNode mutated_json, Optional<AnnotationBean> annotations,
			Optional<JsonNode> grouping_key) {
		return _delegate.emitMutableObject(id, mutated_json, annotations,
				grouping_key);
	}
	/**
	 * @param id
	 * @param original_json
	 * @param mutations
	 * @param annotations
	 * @param grouping_key
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emitImmutableObject(long, com.fasterxml.jackson.databind.JsonNode, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	public Validation<BasicMessageBean, JsonNode> emitImmutableObject(long id,
			JsonNode original_json, Optional<ObjectNode> mutations,
			Optional<AnnotationBean> annotations,
			Optional<JsonNode> grouping_key) {
		return _delegate.emitImmutableObject(id, original_json, mutations,
				annotations, grouping_key);
	}
	/**
	 * @param id
	 * @param original_json
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#storeErroredObject(long, com.fasterxml.jackson.databind.JsonNode)
	 */
	public void storeErroredObject(long id, JsonNode original_json) {
		_delegate.storeErroredObject(id, original_json);
	}
	/**
	 * @param bucket
	 * @param object
	 * @param annotations
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#externalEmit(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, fj.data.Either, java.util.Optional)
	 */
	public Validation<BasicMessageBean, JsonNode> externalEmit(
			DataBucketBean bucket,
			Either<JsonNode, Map<String, Object>> object,
			Optional<AnnotationBean> annotations) {
		return _delegate.externalEmit(bucket, object, annotations);
	}
	/**
	 * @param bucket
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#flushBatchOutput(java.util.Optional)
	 */
	public CompletableFuture<?> flushBatchOutput(Optional<DataBucketBean> bucket) {
		return _delegate.flushBatchOutput(bucket);
	}
	/**
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getServiceContext()
	 */
	public IServiceContext getServiceContext() {
		return _delegate.getServiceContext();
	}
	/**
	 * @param clazz
	 * @param collection
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getGlobalEnrichmentModuleObjectStore(java.lang.Class, java.util.Optional)
	 */
	public <S> Optional<ICrudService<S>> getGlobalEnrichmentModuleObjectStore(
			Class<S> clazz, Optional<String> collection) {
		return _delegate
				.getGlobalEnrichmentModuleObjectStore(clazz, collection);
	}
	/**
	 * @param clazz
	 * @param bucket
	 * @param collection
	 * @param type
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucketObjectStore(java.lang.Class, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	public <S> ICrudService<S> getBucketObjectStore(Class<S> clazz,
			Optional<DataBucketBean> bucket, Optional<String> collection,
			Optional<StateDirectoryType> type) {
		return _delegate.getBucketObjectStore(clazz, bucket, collection, type);
	}
	/**
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucket()
	 */
	public Optional<DataBucketBean> getBucket() {
		return _delegate.getBucket();
	}
	/**
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getModuleConfig()
	 */
	public Optional<SharedLibraryBean> getModuleConfig() {
		return _delegate.getModuleConfig();
	}
	/**
	 * @param bucket
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucketStatus(java.util.Optional)
	 */
	public Future<DataBucketStatusBean> getBucketStatus(
			Optional<DataBucketBean> bucket) {
		return _delegate.getBucketStatus(bucket);
	}
	/**
	 * @param bucket
	 * @return
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getLogger(java.util.Optional)
	 */
	public IBucketLogger getLogger(Optional<DataBucketBean> bucket) {
		return _delegate.getLogger(bucket);
	}
	/**
	 * @param bucket
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emergencyDisableBucket(java.util.Optional)
	 */
	public void emergencyDisableBucket(Optional<DataBucketBean> bucket) {
		_delegate.emergencyDisableBucket(bucket);
	}
	/**
	 * @param bucket
	 * @param quarantine_duration
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emergencyQuarantineBucket(java.util.Optional, java.lang.String)
	 */
	public void emergencyQuarantineBucket(Optional<DataBucketBean> bucket,
			String quarantine_duration) {
		_delegate.emergencyQuarantineBucket(bucket, quarantine_duration);
	}
	/**
	 * @param signature
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#initializeNewContext(java.lang.String)
	 */
	public void initializeNewContext(String signature) {
		_delegate.initializeNewContext(signature);
	}
	protected final IEnrichmentModuleContext _delegate;
	public GraphDecompEnrichmentContext(IEnrichmentModuleContext delegate) {
		_delegate = delegate;
	}
}
