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
package com.ikanow.aleph2.management_db.mongodb.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.mongodb.data_model.MongoDbManagementDbConfigBean;
import com.ikanow.aleph2.management_db.mongodb.data_model.PurgeQueueBean;
import com.ikanow.aleph2.management_db.mongodb.data_model.PurgeQueueBean.PurgeStatus;

/** This service looks for changes to IKANOW test db entries and kicks off test jobs
 * @author cmb
 */
public class IkanowV1SyncService_PurgeBuckets {
	private static final Logger _logger = LogManager.getLogger();
	protected static final ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	protected final MongoDbManagementDbConfigBean _config;
	protected final IServiceContext _context;
	protected final Provider<IManagementDbService> _core_management_db;
	protected final Provider<IManagementDbService> _underlying_management_db;
	protected final ICoreDistributedServices _core_distributed_services;
	
	protected final SetOnce<MutexMonitor> _source_purge_mutex_monitor = new SetOnce<MutexMonitor>();
	
	protected final ScheduledExecutorService _mutex_scheduler = Executors.newScheduledThreadPool(1);		
	protected final ScheduledExecutorService _source_purge_scheduler = Executors.newScheduledThreadPool(1);		
	protected SetOnce<ScheduledFuture<?>> _source_purge_monitor_handle = new SetOnce<ScheduledFuture<?>>(); 
	
	protected static int _num_leader_changes = 0; // (just for debugging/testing)
	
	public final static String SOURCE_PURGE_MONITOR_MUTEX = "/app/aleph2/locks/v1/sources_purge";

	/** guice constructor
	 * @param config - the management db configuration, includes whether this service is enabled
	 * @param service_context - the service context providing all the required dependencies
	 */
	@Inject
	public IkanowV1SyncService_PurgeBuckets(final MongoDbManagementDbConfigBean config, final IServiceContext service_context) {		
		_config = config;
		_context = service_context;
		_core_management_db = _context.getServiceProvider(IManagementDbService.class, IManagementDbService.CORE_MANAGEMENT_DB).get();		
		_underlying_management_db = _context.getServiceProvider(IManagementDbService.class, Optional.empty()).get();
		_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		_core_distributed_services = _context.getService(ICoreDistributedServices.class, Optional.empty()).get();
				
		if (Optional.ofNullable(_config.v1_enabled()).orElse(false)) {
			// Launch the synchronization service
			
			// 1) Monitor sources
			
			_source_purge_mutex_monitor.set(new MutexMonitor(SOURCE_PURGE_MONITOR_MUTEX));
			_mutex_scheduler.schedule(_source_purge_mutex_monitor.get(), 250L, TimeUnit.MILLISECONDS);
			_source_purge_monitor_handle.set(_source_purge_scheduler.scheduleWithFixedDelay(new SourcePurgeMonitor(), 10L, 2L, TimeUnit.SECONDS));
				//(give it 10 seconds before starting, let everything else settle down - eg give the bucket choose handler time to register)			
		}
	}
	/** Immediately start (for testing)
	 */
	@SuppressWarnings("deprecation")
	public void start() {
		_logger.info("IkanowV1SyncService_TestBuckets started, running on a schedule (typically every 1s)");
		_source_purge_monitor_handle.get().cancel(true);
		_source_purge_monitor_handle.forceSet(_source_purge_scheduler.scheduleWithFixedDelay(new SourcePurgeMonitor(), 1, 1L, TimeUnit.SECONDS));
	}
	
	/** Stop threads (just for testing I think)
	 */
	public void stop() {
		_source_purge_monitor_handle.get().cancel(true);
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// WORKER THREADS
	
	public class MutexMonitor implements Runnable {
		protected final String _path;
		protected final SetOnce<CuratorFramework> _curator = new SetOnce<CuratorFramework>();
		protected final SetOnce<LeaderLatch> _leader_selector = new SetOnce<LeaderLatch>();
		public MutexMonitor(final String path) {
			_path = path;
		}
		
		@Override
		public void run() {
			if (!_leader_selector.isSet()) {
				_curator.set(_core_distributed_services.getCuratorFramework());				
				try {
					final LeaderLatch Leader_latch = new LeaderLatch(_curator.get(), _path);
					Leader_latch.start();
					_leader_selector.set(Leader_latch);
				}
				catch (Throwable e) {
					_logger.error(ErrorUtils.getLongForm("{0}", e));
				}
				_logger.info("SourcePurgeMonitor: joined the leadership candidate cluster");
			}			
		}
		public boolean isLeader() {
			return _leader_selector.isSet() ? _leader_selector.get().hasLeadership() : false;
		}
	}

	
	public class SourcePurgeMonitor implements Runnable {
		private final SetOnce<ICrudService<PurgeQueueBean>> _v1_db = new SetOnce<ICrudService<PurgeQueueBean>>();
		private boolean _last_state = false;
		
		/* (non-Javadoc)
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			if (!_source_purge_mutex_monitor.get().isLeader()) {
				_last_state = false;
				return;
			}
			if (!_last_state) {
				_logger.info("SourcePurgeMonitor: now the leader");
				_num_leader_changes++;
				_last_state = true;
			}
			if (!_v1_db.isSet()) {
				@SuppressWarnings("unchecked")
				final ICrudService<PurgeQueueBean> v1_config_db = _underlying_management_db.get().getUnderlyingPlatformDriver(ICrudService.class, Optional.of("ingest.v2_purge_q/" + PurgeQueueBean.class.getName())).get();				
				_v1_db.set(v1_config_db);
				
				_v1_db.get().optimizeQuery(Arrays.asList(BeanTemplateUtils.from(PurgeQueueBean.class).field(PurgeQueueBean::status)));
			}
			
			try {
				// Synchronize
				synchronizePurgeSources(
						_core_management_db.get().getDataBucketStore(), 
						_underlying_management_db.get().getDataBucketStatusStore(), 
						_v1_db.get())
						.get();
				//(note this only waits for completions to finish, which should be super fast)
				
				//(note you can have multiple instances of the called code running even though the main thread pool is one,
				// because of the futures - the code handles it so that it doesn't try to start the code multiple times)
			}			
			catch (Throwable t) {
				_logger.error(ErrorUtils.getLongForm("{0}", t));
			}
		}		
	}
	
	////////////////////////////////////////////////////
	////////////////////////////////////////////////////

	// CONTROL LOGIC
	
	/** Top level logic for source synchronization
	 * @param bucket_mgmt
	 * @param source_db
	 */
	protected CompletableFuture<Void> synchronizePurgeSources(
			final IManagementCrudService<DataBucketBean> bucket_mgmt, 
			final IManagementCrudService<DataBucketStatusBean> underlying_bucket_status_mgmt, 
			final ICrudService<PurgeQueueBean> source_purge_db
			)
	{
		//_logger.debug("Starting a sync purge sources cycle");
//		final List<CompletableFuture<?>> new_results = new ArrayList<CompletableFuture<?>>(); // (not used for synchronization - in a single)
		final List<CompletableFuture<?>> purge_results = new ArrayList<CompletableFuture<?>>();		
		
		final CompletableFuture<List<PurgeQueueBean>> future_purge_sources = getAllPurgeSources(source_purge_db);
		
		//check for entries in test db
		return future_purge_sources.thenCompose( purge_sources -> {
			//_logger.debug("Got test sources successfully, looping over any results");
			purge_sources.forEach(Lambdas.wrap_consumer_u(purge_source -> {		
				_logger.debug("Looking at purge source: " + purge_source._id());

				final DataBucketBean to_purge = Lambdas.wrap_u(() -> IkanowV1SyncService_Buckets.getBucketFromV1Source(purge_source.source())).get();
				//always try to purge the source we pulled
				purge_results.add(handlePurgeSource(to_purge, purge_source));				
			}));
//			if (existing_results.isEmpty()) { // Make sure at least that we don't start a new thread until we've got all the tests from the previous sources
//				existing_results.add(future_purge_sources);
//			}
			//_logger.debug("done looping over test sources");
			//combine response of new and old entries, return
			List<CompletableFuture<?>> retval = 
					Arrays.asList(purge_results).stream() // potentially block on existing results but not new tests 'cos that can take ages
					.flatMap(l -> l.stream())
					.collect(Collectors.toList());			
			return CompletableFuture.allOf(retval.toArray(new CompletableFuture[0]));
		});				
	}
	
	/**
	 * Tells the core management db to purge the data for the given bucket.
	 * 
	 * @param to_purge
	 * @param purge_source
	 * @return
	 */
	private CompletableFuture<Boolean> handlePurgeSource(DataBucketBean to_purge,
			PurgeQueueBean purge_source) {
		_logger.info("Purging data bucket: " + to_purge.full_name());
		return _core_management_db.get().purgeBucket(to_purge, Optional.empty());
	}	
	
	/**
	 * Returns back all purge sources that aren't marked as complete or errored,
	 * deletes everything that was pulled back
	 * 
	 * @param source_test_db
	 * @return
	 */
	protected CompletableFuture<List<PurgeQueueBean>> getAllPurgeSources(final ICrudService<PurgeQueueBean> source_test_db) {
		final QueryComponent<PurgeQueueBean> get_query =
				CrudUtils.allOf(PurgeQueueBean.class)
				.whenNot(PurgeQueueBean::status, PurgeStatus.complete)
				.whenNot(PurgeQueueBean::status, PurgeStatus.error); //can be complete | error | in_progress | submitted | {unset/anything else}
		
		final CompletableFuture<List<PurgeQueueBean>> get_command = 
				source_test_db.getObjectsBySpec(get_query)
				.thenApply(c -> StreamSupport.stream(c.spliterator(), false).collect(Collectors.toList()));
		
		return get_command.thenCompose(__ -> {
			return source_test_db.deleteObjectBySpec(get_query);
		})
		.thenApply(__ -> get_command.join()); // (ie return the original command but only once the update has completed)
	}
}
