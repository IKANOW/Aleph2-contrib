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
package com.ikanow.aleph2.management_db.mongodb.services;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean.StateDirectoryType;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.MethodNamingHelper;
import com.ikanow.aleph2.data_model.utils.CrudServiceUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.ManagementDbUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.module.MongoDbManagementDbModule;
import com.ikanow.aleph2.management_db.mongodb.utils.MongoDbCollectionUtils;
import com.ikanow.aleph2.shared.crud.mongodb.services.IMongoDbCrudServiceFactory;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.gridfs.GridFS;

/** Implementation of the management DB service using MongoDB (or mock MongoDB) 
 * @author acp
 *
 */
/**
 * @author Alex
 *
 */
public class MongoDbManagementDbService implements IManagementDbService, IExtraDependencyLoader  {

	public static final String STATE_DIRECTORY_STORE = "aleph2_shared.state_directory_store";
	public static final String SHARED_LIBRARY_STORE = "aleph2_shared.library";
	
	public static final String DATA_BUCKET_STORE = "aleph2_data_import.bucket";
	public static final String DATA_BUCKET_STATUS_STORE = "aleph2_data_import.bucket_status";
	public static final String RETRY_STORE_PREFIX = "aleph2_data_import.retry_store_";
	public static final String BUCKET_DELETION_STORE = "aleph2_data_import.bucket_delete_store";	
	public static final String BUCKET_TEST_STORE = "aleph2_data_import.bucket_test_store";	
	
	final public static String BUCKET_STATE_HARVEST_DB_PREFIX = "aleph2_harvest_state";
	final public static String BUCKET_STATE_ENRICH_DB_PREFIX = "aleph2_enrich_state";
	final public static String BUCKET_STATE_ANALYTICS_DB_PREFIX = "aleph2_analytics_state";
	final public static String LIBRARY_STATE_DB_PREFIX = "aleph2_library_state";
	public static final String AUTHENTICATION_STORE = "authentication";
	
	protected final IMongoDbCrudServiceFactory _crud_factory;
	protected final Optional<AuthorizationBean> _auth;
	protected final Optional<ProjectBean> _project;
	protected final MongoDbManagementDbConfigBean _properties;
	
	// Some saved management db services
	// (this is mainly for helping with test code since Fongo sometimes(?) creates new collections instead of returning a new wrapper to an existing ones)
	protected final SetOnce<ICrudService<AssetStateDirectoryBean>> _optimized_state_directory = new SetOnce<>();
	protected final SetOnce<ICrudService<?>> _bucket_deletion_q = new SetOnce<>();
	protected final SetOnce<ICrudService<?>> _bucket_test_q = new SetOnce<>();
	protected final SetOnce<ICrudService<?>> _bucket_retry_q = new SetOnce<>();
	protected final SetOnce<IManagementCrudService<DataBucketBean>> _bucket_crud = new SetOnce<>();
	protected final SetOnce<IManagementCrudService<DataBucketStatusBean>> _bucket_status_crud = new SetOnce<>();
	protected final SetOnce<IManagementCrudService<SharedLibraryBean>> _library_crud = new SetOnce<>();
	
	protected final boolean _read_only;
	
	/** Guice generated constructor
	 * @param crud_factory
	 */
	@Inject
	public MongoDbManagementDbService(
			final IMongoDbCrudServiceFactory crud_factory, 
			final MongoDbManagementDbConfigBean properties,
			final IkanowV1SyncService_Buckets sync_service_buckets,
			final IkanowV1SyncService_LibraryJars sync_service_jars
			)
	{
		_crud_factory = crud_factory;
		_auth = Optional.empty();
		_project = Optional.empty();
		_properties = properties;

		_read_only = false;
		
		//DEBUG
		//System.out.println("Hello world from: " + this.getClass() + ": underlying=" + crud_factory);
	}
	
	/** User constructor for building a cloned version with different auth settings
	 * @param crud_factory 
	 * @param auth_fieldname
	 * @param auth
	 * @param project
	 */
	public MongoDbManagementDbService(IMongoDbCrudServiceFactory crud_factory, 
			Optional<AuthorizationBean> auth, Optional<ProjectBean> project, final MongoDbManagementDbConfigBean properties, final boolean read_only)
	{
		_crud_factory = crud_factory;
		_auth = auth;
		_project = project;		
		_properties = properties;
		_read_only = read_only;
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getFilteredDb(java.lang.String, java.util.Optional, java.util.Optional)
	 */
	public MongoDbManagementDbService getFilteredDb(final Optional<AuthorizationBean> client_auth, final Optional<ProjectBean> project_auth)
	{
		return new MongoDbManagementDbService(_crud_factory, client_auth, project_auth, _properties, _read_only);
	}
	
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getSharedLibraryStore()
	 */
	public IManagementCrudService<SharedLibraryBean> getSharedLibraryStore() {
		synchronized (this) {
			if (!_library_crud.isSet()) {
				_library_crud.set(		
						ManagementDbUtils.wrap(_crud_factory.getMongoDbCrudService(
								SharedLibraryBean.class, String.class, 
								_crud_factory.getMongoDbCollection(MongoDbManagementDbService.SHARED_LIBRARY_STORE), 
								Optional.of(BeanTemplateUtils.from(SharedLibraryBean.class).field(SharedLibraryBean::access_rights)), 
								_auth, _project)).readOnlyVersion(_read_only)
						);
			}
		}
		return this._library_crud.get();
	}

	/** Utility function handling all the functionality required for getPerLibraryState, getBucket*State
	 * @param clazz
	 * @param name
	 * @param collection
	 * @param type
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected <T> ICrudService<T> getAppStateStore(final Class<T> clazz, final String name, final Optional<String> collection, final AssetStateDirectoryBean.StateDirectoryType type) {
		if (!collection.isPresent()) { // return the directory			
			if (!clazz.equals(AssetStateDirectoryBean.class)) {
				throw new RuntimeException("Empty collection => return directory, so clazz type must be AssetStateDirectoryBean");
			}
			return ((ICrudService<T>) getStateDirectory(Optional.of(BeanTemplateUtils.build(DataBucketBean.class).with(DataBucketBean::full_name, name).done().get()),
										Optional.of(type)
										)).readOnlyVersion(true); //(always read only)
		}
		// Otherwise...
		
		final String prefix = Lambdas.get(() -> {
			switch (type) {
				case analytic_thread: return MongoDbManagementDbService.BUCKET_STATE_ANALYTICS_DB_PREFIX;
				case enrichment: return MongoDbManagementDbService.BUCKET_STATE_ENRICH_DB_PREFIX;
				case harvest: return MongoDbManagementDbService.BUCKET_STATE_HARVEST_DB_PREFIX;
				case library: return MongoDbManagementDbService.LIBRARY_STATE_DB_PREFIX;
			}
			return null;
		});
		
		final String collection_name = BucketUtils.getUniqueSignature(name, collection);
		
		final DB db = MongoDbCollectionUtils.findDatabase(
						_crud_factory.getMongoDb("test").getMongo(), 
						prefix, collection_name);

		final DBCollection db_collection = db.getCollection(collection_name);
		final ICrudService<T> state_crud = _crud_factory.getMongoDbCrudService(
				clazz, String.class,
				db_collection,
				Optional.empty(), 
				_auth, _project);
		
		// add a delete interceptor to remove the entry from the directory
		final Map<String, BiFunction<Object, Object[], Object>> interceptors = Lambdas.get(() -> {
			if (!this._read_only) {
				// add entry to directory:		
				final ICrudService<AssetStateDirectoryBean> raw_state_crud_dir = getStateDirectory(Optional.empty(), Optional.empty());
				raw_state_crud_dir.storeObject(new AssetStateDirectoryBean(name, type, collection.get(), db_collection.getFullName()), true); 				
				//^SIDE EFFECT:
				
				return ImmutableMap.<String, BiFunction<Object, Object[], Object>>builder()
						.put("deleteDatastore",
							(Object ret_val, Object[] args) -> {
								raw_state_crud_dir.deleteObjectById(db_collection.getFullName());
								return ret_val;
							})
						.build()
						;
			} // (else not needed because can't deleteDatastore anyway)
			else {
				return Collections.<String, BiFunction<Object, Object[], Object>>emptyMap();
			}
		});
		
		final ICrudService<T> intercepted_crud = CrudServiceUtils.intercept(clazz, state_crud, Optional.empty(), interceptors, Optional.empty());		
		
		return ManagementDbUtils.wrap(intercepted_crud).readOnlyVersion(_read_only);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerLibraryState(java.lang.Class, com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getPerLibraryState(Class<T> clazz,
			SharedLibraryBean library, Optional<String> collection) {
		return getAppStateStore(clazz, library.path_name(), collection, AssetStateDirectoryBean.StateDirectoryType.library);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getDataBucketStore()
	 */
	public IManagementCrudService<DataBucketBean> getDataBucketStore() {
		synchronized (this) {
			if (!_bucket_crud.isSet()) {
				_bucket_crud.set(		
						ManagementDbUtils.wrap(_crud_factory.getMongoDbCrudService(
								DataBucketBean.class, String.class, 
								_crud_factory.getMongoDbCollection(MongoDbManagementDbService.DATA_BUCKET_STORE), 
								Optional.of(BeanTemplateUtils.from(DataBucketBean.class).field(DataBucketBean::access_rights)), 
								_auth, _project)).readOnlyVersion(_read_only)
							);
			}
		}
		return this._bucket_crud.get();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getDataBucketStatusStore()
	 */
	public IManagementCrudService<DataBucketStatusBean> getDataBucketStatusStore() {
		synchronized (this) {
			if (!_bucket_status_crud.isSet()) {
				_bucket_status_crud.set(		
					ManagementDbUtils.wrap(_crud_factory.getMongoDbCrudService(
						DataBucketStatusBean.class, String.class, 
						_crud_factory.getMongoDbCollection(MongoDbManagementDbService.DATA_BUCKET_STATUS_STORE), 
						Optional.empty(), 
						_auth, _project)).readOnlyVersion(_read_only)
						);
			}
		}
		return this._bucket_status_crud.get();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerBucketState(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getBucketHarvestState(final Class<T> clazz,
			final DataBucketBean bucket, final Optional<String> collection) {
		return getAppStateStore(clazz, bucket.full_name(), collection, AssetStateDirectoryBean.StateDirectoryType.harvest);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerBucketState(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getBucketEnrichmentState(final Class<T> clazz,
			final DataBucketBean bucket, final Optional<String> collection) {
		return getAppStateStore(clazz, bucket.full_name(), collection, AssetStateDirectoryBean.StateDirectoryType.enrichment);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerAnalyticThreadState(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getBucketAnalyticThreadState(Class<T> clazz,
			DataBucketBean bucket, Optional<String> collection) {
		return getAppStateStore(clazz, bucket.full_name(), collection, AssetStateDirectoryBean.StateDirectoryType.analytic_thread);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		
		if (DBCollection.class == driver_class) {
			if (!driver_options.isPresent()) {
				throw new RuntimeException("If requesting DB collection, need to specify db_name.coll_name");
			}
			final String[] db_coll = driver_options.get().split("[.]", 2);
			if (2 != db_coll.length) {
				throw new RuntimeException("If requesting DB collection, need to specify db_name.coll_name: " + driver_options.get());
			}
			return (Optional<T>) Optional.of(_crud_factory.getMongoDbCollection(db_coll[0], db_coll[1]));
		}
		if (GridFS.class == driver_class) {
			if (!driver_options.isPresent()) {
				throw new RuntimeException("If requesting GridFS, need to specify db_name.coll_name");
			}
			final String[] db_coll = driver_options.get().split("[.]", 2);
			if (2 != db_coll.length) {
				throw new RuntimeException("If requesting GridFS, need to specify db_name.coll_name: " + driver_options.get());
			}
			return (Optional<T>) Optional.of(new GridFS(_crud_factory.getMongoDb(db_coll[0]), db_coll[1]));
		}
		else if (DB.class == driver_class) {
			if (!driver_options.isPresent()) {
				throw new RuntimeException("If requesting DB, need to specify db_name");
			}
			return (Optional<T>) Optional.of( _crud_factory.getMongoDb(driver_options.get()));
		}
		else if (ICrudService.class == driver_class) {
			if (!driver_options.isPresent()) {
				throw new RuntimeException("If requesting a CRUD service, need to specify db_name.coll_name[/fully.qualified.bean.class]");
			}
			final String[] dbcoll_clazz = driver_options.get().split("[/]", 2);
			final String[] db_coll = dbcoll_clazz[0].split("[.]", 2);
			if (2 != db_coll.length) {
				throw new RuntimeException("If requesting a CRUD service, need to specify db_name.coll_name[/fully.qualified.bean.class]: " + driver_options.get());
			}
			try {
				if (2 != dbcoll_clazz.length) { // returns the JSON version
					return (Optional<T>) Optional.of(_crud_factory.getMongoDbCrudService(
							JsonNode.class, Object.class, _crud_factory.getMongoDbCollection(db_coll[0], db_coll[1]), 
							Optional.empty(), Optional.empty(), Optional.empty()));
				}
				else {
					return (Optional<T>) Optional.of(_crud_factory.getMongoDbCrudService(
							Class.forName(dbcoll_clazz[1]), Object.class, _crud_factory.getMongoDbCollection(db_coll[0], db_coll[1]), 
							Optional.empty(), Optional.empty(), Optional.empty()));
				}
			}
			catch (Exception e) {
				throw new RuntimeException("Requesting CRUD service, specified invalid class name: " + driver_options.get());
			}
		}
		return Optional.empty();
	}

	/** This service needs to load some additional classes via Guice. Here's the module that defines the bindings
	 * @return
	 */
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new MongoDbManagementDbModule());
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader#youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules()
	 */
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// (done see above)		
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> ICrudService<T> getBucketTestQueue(Class<T> test_queue_clazz) {
		synchronized (this) {
			if (!_bucket_test_q.isSet()) {
				_bucket_test_q.set(
						(ICrudService<T>) _crud_factory.getMongoDbCrudService(
								test_queue_clazz, String.class, 
								_crud_factory.getMongoDbCollection(MongoDbManagementDbService.BUCKET_TEST_STORE), 
								Optional.empty(), Optional.empty(), Optional.empty()).readOnlyVersion(_read_only)
						);
			}
		}
		return (ICrudService<T>) _bucket_test_q.get();
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getRetryStore(java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> ICrudService<T> getRetryStore(Class<T> retry_message_clazz) {
		synchronized (this) {
			if (!_bucket_retry_q.isSet()) {
				_bucket_retry_q.set(
						(ICrudService<T>) _crud_factory.getMongoDbCrudService(
								retry_message_clazz, String.class, 
								_crud_factory.getMongoDbCollection(MongoDbManagementDbService.RETRY_STORE_PREFIX + retry_message_clazz.getSimpleName()), 
								Optional.empty(), Optional.empty(), Optional.empty()).readOnlyVersion(_read_only)
						);
			}
		}
		return (ICrudService<T>) _bucket_retry_q.get();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getBucketDeletionQueue(java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> ICrudService<T> getBucketDeletionQueue(Class<T> deletion_queue_clazz) {		
		synchronized (this) {
			if (!_bucket_deletion_q.isSet()) {
				_bucket_deletion_q.set(
						(ICrudService<T>) _crud_factory.getMongoDbCrudService(
							deletion_queue_clazz, String.class, 
							_crud_factory.getMongoDbCollection(MongoDbManagementDbService.BUCKET_DELETION_STORE), 
							Optional.empty(), Optional.empty(), Optional.empty()).readOnlyVersion(_read_only)
						);
			}
		}
		return (ICrudService<T>) _bucket_deletion_q.get();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getStateDirectory(java.util.Optional)
	 */
	@Override
	public ICrudService<AssetStateDirectoryBean> getStateDirectory(final Optional<DataBucketBean> bucket_filter, final Optional<StateDirectoryType> type_filter) {
		final Optional<QueryComponent<AssetStateDirectoryBean>> extra_terms =
				Optional.of(
						bucket_filter
							.map(bf -> CrudUtils.allOf(AssetStateDirectoryBean.class).when(AssetStateDirectoryBean::asset_path, bf.full_name()))
							.orElse(CrudUtils.allOf(AssetStateDirectoryBean.class))
						)
				.map(bfq -> type_filter.map(tf -> bfq.when(AssetStateDirectoryBean::state_type, tf)).orElse(bfq))
				.filter(q -> !q.getAll().isEmpty())
				.map(bfq -> (QueryComponent<AssetStateDirectoryBean>)bfq)
				;		
		
		final ICrudService<AssetStateDirectoryBean> raw_state_crud = Lambdas.get(() -> {
			synchronized (this) {
				if (!_optimized_state_directory.isSet()) {
					final ICrudService<AssetStateDirectoryBean> raw_state_crud_internal = 
							(ICrudService<AssetStateDirectoryBean>)
							_crud_factory.getMongoDbCrudService(
								AssetStateDirectoryBean.class, String.class, 
								_crud_factory.getMongoDbCollection(MongoDbManagementDbService.STATE_DIRECTORY_STORE), 
								Optional.empty(), Optional.empty(), Optional.empty());
							
					final MethodNamingHelper<AssetStateDirectoryBean> h = BeanTemplateUtils.from(AssetStateDirectoryBean.class);
					raw_state_crud_internal.optimizeQuery(Arrays.asList(h.field(AssetStateDirectoryBean::asset_path), h.field(AssetStateDirectoryBean::state_type)));
					raw_state_crud_internal.optimizeQuery(Arrays.asList(h.field(AssetStateDirectoryBean::state_type)));
					_optimized_state_directory.set(raw_state_crud_internal);
				}
			}
			return _optimized_state_directory.get();
		});

		return extra_terms
					.<ICrudService<AssetStateDirectoryBean>>
						map(et -> CrudServiceUtils.intercept(AssetStateDirectoryBean.class, raw_state_crud, extra_terms, Collections.emptyMap(), Optional.empty()))
					.orElse(raw_state_crud)
					.readOnlyVersion(_read_only)
				;
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Arrays.asList(this, _crud_factory);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#readOnlyVersion()
	 */
	@Override
	public IManagementDbService readOnlyVersion() {
		return new MongoDbManagementDbService(_crud_factory, _auth, _project, _properties, true);
	}

	@Override
	public ManagementFuture<Boolean> purgeBucket(DataBucketBean to_purge,
			Optional<Duration> in) {
		throw new RuntimeException("This is implemented in the CoreManagementDbService not here");
	}

	@Override
	public ManagementFuture<Boolean> testBucket(DataBucketBean to_test,
			ProcessingTestSpecBean test_spec) {
		throw new RuntimeException("This is implemented in the CoreManagementDbService not here");
	}

	@Override
	public IManagementDbService getSecuredDb(AuthorizationBean client_auth) {
		// TODO verify if we don't want secured Wrapper here.
		return new MongoDbManagementDbService(_crud_factory, _auth, _project, _properties, true);
	}

}
