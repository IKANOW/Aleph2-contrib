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
package com.ikanow.aleph2.shared.crud.elasticsearch.data_model;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.client.Client;
import org.elasticsearch.indices.IndexMissingException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.TimeUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext.IndexContext.ReadWriteIndexContext.TimedRwIndexContext;
import com.ikanow.aleph2.shared.crud.elasticsearch.utils.ElasticsearchContextUtils;
import com.ikanow.aleph2.shared.crud.elasticsearch.utils.ElasticsearchFutureUtils;

import fj.Unit;
import fj.data.Either;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import scala.Tuple2;
import scala.Tuple3;

//TODO: ALEPH-14: Ignore requests for new indexes that are too old? (So we don't bother creating indexes that we'll delete an hour later)

/** Algebraic data type (ADT) encapsulating the state of an elasticsearch crud "service" (which could point at multiple/auto indexes and types)
 * ElasticsearchContext = ReadOnlyContext(ReadOnlyTypeContext, ReadOnlyIndexContext) 
 * 						| ReadWriteContext(ReadWriteTypeContext, ReadWriteIndexContext)
 * 
 * TypeContext = ReadOnlyTypeContext | ReadWriteTypeContext
 * IndexContext = ReadOnlyIndexContext | ReadWriteIndexContext
 * 
 * ReadOnlyIndexContext = FixedRoIndexContext | TimedRoIndexContext
 * ReadWriteIndexContext = FixedRwIndexContext | TimedRwIndexContext
 * 
 * ReadOnlyTypeContext = FixedRoTypeContext | AutoRoTypeContext
 * ReadWriteTypeContext = FixedRwTypeContext | AutoRwTypeContext
 * 
 * @author Alex
 */
public abstract class ElasticsearchContext {
	private ElasticsearchContext(final Client client) {
		_client = client;
	}
	/** Returns the raw system-wide Elasticsearch client
	 * @return an Elasticsearch client
	 */
	public Client client() { return _client; }
	private final Client _client;
	
	/** Returns information about the set of indexes associated with this CRUD service
	 * @return the index context ADT
	 */
	public abstract IndexContext indexContext(); 
	/** Returns information about the set of types associated with this CRUD service
	 * @return the type context ADT
	 */
	public abstract TypeContext typeContext(); 
	
	/** This is pre-prended to index names to form an alias that is always safe to read
	 */
	public static final String READ_PREFIX = "r__";		
	
	/** Returns a random index instead of [] which defaults to "everything"
	 */
	public static final String NO_INDEX_FOUND = "NO_INDEXES_FOUND_e8ed37b4_831e_11e5_8bcf_feff819cdc9f";
	public static final List<String> NO_INDEXES_FOUND = Arrays.asList(NO_INDEX_FOUND);
	
	/** Elasticsearch context that one can read from but not write to (eg because it has multiple indexes)
	 * @author Alex
	 */
	public static class ReadOnlyContext extends ElasticsearchContext {
		public ReadOnlyContext(final Client client, final IndexContext.ReadOnlyIndexContext index_context, final TypeContext.ReadOnlyTypeContext type_context) {
			super(client);
			_index_context = index_context;
			_type_context = type_context;
		}
		@Override
		public IndexContext.ReadOnlyIndexContext indexContext() {
			return _index_context;
		}
		@Override
		public  TypeContext.ReadOnlyTypeContext typeContext() {
			return _type_context;			
		}
		private final IndexContext.ReadOnlyIndexContext _index_context;
		private final TypeContext.ReadOnlyTypeContext _type_context;
	}

	/** Elasticsearch context that one can also write to (ie has a single type - though possibly a variable one, and a single type - though possibly a variable one)
	 * @author Alex
	 */
	public static class ReadWriteContext extends ElasticsearchContext {
		public ReadWriteContext(final Client client, final IndexContext.ReadWriteIndexContext index_context, final TypeContext.ReadWriteTypeContext type_context) {
			super(client);
			_index_context = index_context;
			_type_context = type_context;
			_index_context.client(client);
		}
		@Override
		public IndexContext.ReadWriteIndexContext indexContext() {
			return _index_context;
		}
		@Override
		public  TypeContext.ReadWriteTypeContext typeContext() {
			return _type_context;			
		}
		private final IndexContext.ReadWriteIndexContext _index_context;
		private final TypeContext.ReadWriteTypeContext _type_context;
	}
	
	/** ADT encapsulating information about an index or set of indexes
	 * @author Alex
	 */
	public static abstract class IndexContext {
		private IndexContext() {}
		
		protected Client client(final Client client) {
			return _client.trySet(client);
		}
		protected Client client() { return _client.get(); }
		private final SetOnce<Client> _client = new SetOnce<>();
		
		/** Returns a list of indexes that can be used directly
		 * @param date_range - ignored for FixedRoIndexContext; for TimeBasedIndexContext, can be used to narrow down the indexes searched 
		 * @return a list of indexes that can be passed into Client calls
		 */
		public abstract List<String> getReadableIndexList(final Optional<Tuple2<Long, Long>> date_range);
		
		public String[] getReadableIndexArray(final Optional<Tuple2<Long, Long>> date_range) {
			return getReadableIndexList(date_range).toArray(new String[0]);
		}
		
		/** ADT encapsulating information about a read-only index of set of indexes - see enclosing class for more details
		 * @author Alex
		 */
		public static abstract class ReadOnlyIndexContext extends IndexContext {
			private ReadOnlyIndexContext() {}
			
			/** A simple list of multiple indexes to query over
			 * @author Alex
			 */
			public static class FixedRoIndexContext extends ReadOnlyIndexContext {
				public FixedRoIndexContext(final List<String> indexes) {
					_indexes = indexes;
				}				
				final private List<String> _indexes;
				
				@Override
				public List<String> getReadableIndexList(final Optional<Tuple2<Long, Long>> date_range) {
					final List<String> ret_val = _indexes.stream().map(s -> s + "*").collect(Collectors.toList()); //(need the "*" to support segments)
					return ret_val.isEmpty() ? NO_INDEXES_FOUND : ret_val;
				} 
			}
			/** A simple list of multiple indexes to query over
			 * @author Alex
			 */
			public static class TimedRoIndexContext extends ReadOnlyIndexContext {
				public TimedRoIndexContext(final List<String> indexes) {
					_indexes = indexes;
				}				
				final private List<String> _indexes;
				
				@Override
				public List<String> getReadableIndexList(final Optional<Tuple2<Long, Long>> date_range) {
					
					final List<String> ret_val = Lambdas.get(() -> {
						if (!date_range.isPresent()) { // Convert to wildcards
							//(the extra bits here handle the case where a timed index is passed in without its date suffix (eg "somewhat_badly_formed__UUID")
							return _indexes.stream().map(i -> i.replaceFirst("([^_])_[^_]+$", "$1*")).map(i -> i.endsWith("*") ? i : (i + "*")).collect(Collectors.toList());
						}
						else {
							return _indexes.stream()
										.flatMap(i -> ElasticsearchContextUtils.getIndexesFromDateRange(i, date_range.get()))
										.map(s -> s + '*')
										.collect(Collectors.toList());
						}
					});
					return ret_val.isEmpty() ? NO_INDEXES_FOUND : ret_val;
				} 
			}
			/** Handles a combination of fixed and timed read only indexes
			 * @author Alex
			 */
			public static class MixedRoIndexContext extends ReadOnlyIndexContext {
				public MixedRoIndexContext(final List<String> timed_indexes, final List<String> fixed_indexes) {
					_timed_delegate = new TimedRoIndexContext(timed_indexes);
					_fixed_delegate = new FixedRoIndexContext(fixed_indexes);
				}				
				final TimedRoIndexContext _timed_delegate;				
				final FixedRoIndexContext _fixed_delegate;				
				@Override
				public List<String> getReadableIndexList(
						Optional<Tuple2<Long, Long>> date_range) {
					final List<String> ret_val = 
							Stream.concat(
									_timed_delegate.getReadableIndexList(date_range).stream().filter(s -> !NO_INDEX_FOUND.equals(s))
									, 
									_fixed_delegate.getReadableIndexList(date_range).stream().filter(s -> !NO_INDEX_FOUND.equals(s)))
								.collect(Collectors.toList());
					return ret_val.isEmpty() ? NO_INDEXES_FOUND : ret_val;					
				}				
			}
		}
		/** ADT encapsulating information about a read-write single index - see enclosing class for details
		 * @author Alex
		 */
		public static abstract class ReadWriteIndexContext extends IndexContext {
			private ReadWriteIndexContext(Optional<Long> target_max_index_size_mb, final Either<Boolean, Function<String, Optional<String>>> create_aliases) {
				_target_max_index_size_mb = target_max_index_size_mb.filter(i -> i >= 0); // (negative means no limit)
				_create_aliases = create_aliases;
			}			
			public final Optional<Long> target_max_index_size_mb() { return _target_max_index_size_mb; }
			protected final Optional<Long> _target_max_index_size_mb;
			protected final Either<Boolean, Function<String, Optional<String>>> _create_aliases;
			
			protected final static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());			
			
			// READ-ONLY ALIAS CREATION:
			
			private final ScheduledExecutorService _scheduler = Executors.newScheduledThreadPool(1);			

			// READ-ONLY ALIAS UTILS:

			/** If we're performing expensive checks for aliases
			 * @return
			 */
			protected boolean mayCreateAliasesForThisIndex() {
				return _create_aliases.isRight() || _create_aliases.left().value();
			}
			
			/** Checks to see if the read only version of this alias has been assigned, and assigns it if not
			 *  (At some point, ES should allow this function to be built into the mapping, or discovered in the callback to the store function)
			 * @param index_name
			 */
			protected boolean checkForAliases(final String index_name) {				
				final Optional<String> alias_to_create = 
						_create_aliases.<Optional<String>>either(
								left -> left ? Optional.of(index_name) : Optional.empty()
								, 
								right -> right.apply(index_name));
				
				alias_to_create.ifPresent(base_index -> {
					recursiveAliasCreate(base_index, index_name, 30); //(try for a minute - 2s sleep between each try)
				});				
				return alias_to_create.isPresent();
			}
			
			/** Low level recursive utility for trying repeatedly to create an index until its index exists
			 * @param base_index
			 * @param index_name
			 * @param number_of_tries
			 */
			private void recursiveAliasCreate(final String base_index, final String index_name, int number_of_tries) {
				_scheduler.schedule(() -> {
					ElasticsearchFutureUtils.wrap(this.client().admin().indices().prepareAliases().addAlias(index_name + "*", READ_PREFIX + base_index).execute(),
							__ -> {
								// Index created we're done
								return Unit.unit();
							},
							(e, cf) -> {
								if ((e instanceof IndexMissingException) ||
										((null != e.getCause()) && (e.getCause() instanceof IndexMissingException)))
								{
									if (number_of_tries > 0) {
										recursiveAliasCreate(base_index, index_name, number_of_tries - 1);
									}
								}
							}
							);
				}
				,
				2, TimeUnit.SECONDS // need to give the index time to write or the alias won't exist...
				);												
				
			}
			
			// INDEX SIZING UTILITY:
			
			private static long MB = 1024L*1024L;
			public static String BAR = "_";
			public static final long INDEX_SIZE_CHECK_MS = 10000L; // (Every 10s)			
			public static class MutableState {
				private AtomicBoolean _is_working = new AtomicBoolean(false); 
				private ConcurrentHashMap<String, Tuple3<Long, String, Integer>> _base_index_states = new ConcurrentHashMap<>();
			}
			private final MutableState _mutable_state = new MutableState(); // (WARNING - mutable)
			
			/** Every 10s check the index size and increment the index suffix if too large
			 * @return
			 */
			protected String getIndexSuffix(final String base_index) {
				// Common:
				final long now = new Date().getTime();
				final Tuple3<Long, String, Integer> last_suffix = _mutable_state._base_index_states.computeIfAbsent(base_index, __ -> Tuples._3T(null, "", 0));
				final Long last_checked = last_suffix._1();
				final boolean checked_for_aliases = Lambdas.get(() -> {
					if (mayCreateAliasesForThisIndex()) {
						if (null == last_checked) { // alias checking logic, first time through only... (And then for index splitting, whenever an index is split)
							checkForAliases(base_index);
							if (!_target_max_index_size_mb.isPresent()) { // (update the concurrent hash map here since not going any further)
								_mutable_state._base_index_states.put(base_index, Tuples._3T(now, "", 0)); // (current time + defaults - 2/3 not used)
							}
							return true;
						}
					}
					return false;
				});
				
				if (!_target_max_index_size_mb.isPresent()) { // if not splitting contexts into multiple indexes
					return base_index;
				}
				else synchronized (this) { // (just need this sync point so that multiple threads first time through don't fall through to default)					
					return _target_max_index_size_mb
							.filter(__ -> { // only check first time or every 10s
								if ((null == last_checked) || ((now - last_checked) >= INDEX_SIZE_CHECK_MS)) {
									_mutable_state._base_index_states.put(base_index, Tuples._3T(now, last_suffix._2(), last_suffix._3()));
									return true;
								}
								else return false;
							})
							// (if it's processing a previous request, keep going with the current suffix)
							.filter(__ -> _mutable_state._is_working.compareAndSet(false, true)) 
							.map(m -> {	
								final CompletableFuture<Tuple3<Long, String, Integer>> future = ElasticsearchFutureUtils.wrap(
										this.client().admin().indices().prepareStats()
						                    .clear()
						                    .setIndices(base_index + "*")
						                    .setStore(true)
						                    .execute()								
										,
										stats -> {
											final int suffix_index = Lambdas.get(() -> {
												final IndexStats index_stats = stats.getIndex(getName(base_index, last_suffix));
												
												final Predicate<IndexStats> shard_too_big = i_stats -> 
													Arrays.stream(i_stats.getShards()).map(shard -> shard.getStats().getStore()).anyMatch(x -> x.getSizeInBytes() >= (m*MB));
												
												if ((null != index_stats) && shard_too_big.test(index_stats))
												{
													int max_index = 1;
													// find a new index to use:									
													for (; ; max_index++) {
														final IndexStats candidate_index_stats = stats.getIndex(base_index + BAR + max_index);
														
														if (null == candidate_index_stats) break;
														else if (!shard_too_big.test(candidate_index_stats)) break; // (found one we can use!)
													}
													return max_index;
												}
												else {
													return last_suffix._3();
												}
											});			
											if (mayCreateAliasesForThisIndex() && !checked_for_aliases && (suffix_index != last_suffix._3())) {
												checkForAliases(base_index);
											}
											final Tuple3<Long, String, Integer> new_suffix = Tuples._3T(now, BAR + suffix_index, suffix_index);
											_mutable_state._base_index_states.put(base_index, new_suffix);
											return new_suffix;
										})
										// Just stick a future at the end of the chain to ensure the mutable state is always fixed
										.thenApply(x -> {
											_mutable_state._is_working.set(false);
											return x;
										})
										.exceptionally(t -> {
											_mutable_state._is_working.set(false);
											return last_suffix;
										});
										;
								
								if (null == last_checked) { // first time through, wait for the code to complete
									try {
										final Tuple3<Long, String, Integer> new_suffix = future.join();
										return getName(base_index, new_suffix);
									}
									catch (Exception e) { // pass through to default on error
										return null;
									}
								}
								else return null; // pass through to default
							})
							.orElseGet(() -> getName(base_index, last_suffix))
							;
					
				} //(end sync/if checking index sizes)
			}//(end getIndexSuffix)
			
			//(util function)
			private static String getName(final String index, final Tuple3<Long, String, Integer> suffix_meta) {
				return index + 
						((0 != suffix_meta._3())
								? suffix_meta._2()
										: "");
			}
			
			/** Gets the index to write to
			 * @param writable_object - only used in time-based indexes, if optional then "now" is used
			 * @return an index that can be used for "client" writes
			 */
			public abstract String getWritableIndex(Optional<JsonNode> writable_object);
			
			/** Least interesting case ever! Single index
			 * @author Alex
			 */
			public static class FixedRwIndexContext extends ReadWriteIndexContext {
				/** Creates a fixed name index
				 * @param index - the base index name
				 * @param target_max_index_size_mb - the target max index size
				 * @param create_aliases - if true tries to maintain a read-only alias across the different indexes comprising this context
				 */
				public FixedRwIndexContext(final String index, final Optional<Long> target_max_index_size_mb, final Either<Boolean, Function<String, Optional<String>>> create_aliases) {
					super(target_max_index_size_mb, create_aliases);
					_index = index;
				}
				/** Creates a fixed name index (will create a read only index)
				 * @param index - the base index name
				 * @param target_max_index_size_mb - the target max index size
				 */
				protected final String _index;
				
				/* (non-Javadoc)
				 * @see com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext.IndexContext#getReadableIndexList(java.util.Optional)
				 */
				@Override
				public List<String> getReadableIndexList(Optional<Tuple2<Long, Long>> date_range) {
					return Arrays.asList(_index + _target_max_index_size_mb.map(__ -> "*").orElse(""));
				}

				/* (non-Javadoc)
				 * @see com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext.IndexContext.ReadWriteIndexContext#getWritableIndex(java.util.Optional)
				 */
				@Override
				public String getWritableIndex(Optional<JsonNode> writable_object) {
					return this.getIndexSuffix(_index);
				}
			}
			/** Least interesting case ever! Secondary buffer version - doesn't create indexes
			 * @author Alex
			 */
			public static class FixedRwIndexSecondaryContext extends FixedRwIndexContext {
				public FixedRwIndexSecondaryContext(final String index, Optional<Long> target_max_index_size_mb) {
					super(index, target_max_index_size_mb, Either.left(false));
				}
			}
			
			/** Just one index but it is time-based ie contains _{TIME_SIGNATURE}
			 * @author Alex
			 */
			public static class TimedRwIndexContext extends ReadWriteIndexContext {
				/** Created a time-based index context
				 * @param index - index name including pattern
				 * @param time_field - the field to use, will just use "now" if left blank
				 * @param target_max_index_size_mb - the target max index size
				 * @param create_aliases - if true tries to maintain a read-only alias across the different indexes comprising this context
				 */
				public TimedRwIndexContext(final String index, final Optional<String> time_field, final Optional<Long> target_max_index_size_mb, final Either<Boolean, Function<String, Optional<String>>> create_aliases) {
					super(target_max_index_size_mb, create_aliases);
					_index = index;
					_time_field = time_field;
					_index_split = ElasticsearchContextUtils.splitTimeBasedIndex(_index);
					_formatter = ThreadLocal.withInitial(() -> new SimpleDateFormat(_index_split._2()));
					
				}
				/** Created a time-based index context (will create a read-only alias across all indexes in the context)
				 * @param index - index name including pattern
				 * @param time_field - the field to use, will just use "now" if left blank
				 * @param target_max_index_size_mb - the target max index size
				 */
				final String _index;
				final Optional<String> _time_field;
				final Tuple2<String, String> _index_split;
				final ThreadLocal<SimpleDateFormat> _formatter;
				
				public Optional<String> timeField() {
					return _time_field;
				}
				
				/* (non-Javadoc)
				 * @see com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext.IndexContext#getReadableIndexList(java.util.Optional)
				 */
				@Override
				public List<String> getReadableIndexList(final Optional<Tuple2<Long, Long>> date_range) {
					if (!date_range.isPresent()) { // Convert to wildcards
						//(the extra bits here handle the case where a timed index is passed in without its date suffix (eg "somewhat_badly_formed__UUID")
						final String s1 = _index.replaceFirst("([^_])_[^_]+$", "$1*");
						return Arrays.asList(s1.endsWith("*") ? s1 : (s1 + "*"));
					}
					else { //(always returns >1 index by construction)
						return ElasticsearchContextUtils.getIndexesFromDateRange(_index, date_range.get()).map(s -> s + '*').collect(Collectors.toList());
					}
				}
				/* (non-Javadoc)
				 * @see com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext.IndexContext.ReadWriteIndexContext#getWritableIndex(java.util.Optional)
				 */
				@Override
				public String getWritableIndex(final Optional<JsonNode> writable_object) {
					final Date d = _time_field
											.filter(__ -> writable_object.isPresent())
											.map(t -> writable_object.get().get(t))
											.map(jsonl -> {
												if (jsonl.isLong()) return new Date(jsonl.asLong());
												else if (jsonl.isTextual()) return TimeUtils.parseIsoString(jsonl.asText()).validation(__ -> null, success -> success);
												else return null; // return nulls fall through to...)
											})
										.orElseGet(Date::new); // (else just use "now")
							
					final String formatted_date = _formatter.get().format(d);

					return getIndexSuffix(ElasticsearchContextUtils.reconstructTimedBasedSplitIndex(_index_split._1(), formatted_date));
				}
			}
		}
		/** Just one index but it is time-based ie contains _{TIME_SIGNATURE} - secondary buffer version
		 * @author Alex
		 */
		public static class TimedRwIndexSecondaryContext extends TimedRwIndexContext {
			public TimedRwIndexSecondaryContext(final String index, final Optional<String> time_field, Optional<Long> target_max_index_size_mb) {
				super(index, time_field, target_max_index_size_mb, Either.left(false));
			}
		}
	}

	/** ADT encapsulating information about a type or set of types within an index or set of indexes
	 * @author Alex
	 */
	public static abstract class TypeContext {
		private TypeContext() {}
		
		/** Get a list of types that can be passed into an ES read operation
		 *  Note that where possible this should be bypassed since it can be problematic with auto-type indexes
		 * @return a list of types (possibly empty - means can't filter on types)
		 */
		public abstract List<String> getReadableTypeList();
		
		public String[] getReadableTypeArray() {
			return getReadableTypeList().toArray(new String[0]);
		}
		
		public static abstract class ReadOnlyTypeContext extends TypeContext {
			private ReadOnlyTypeContext() {}
			
			/** Simple case - one or more types to query over
			 * @author Alex
			 */
			public static class FixedRoTypeContext extends ReadOnlyTypeContext {
				public FixedRoTypeContext(final List<String> types) {
					_types = types; 
				}
				private List<String> _types;
				@Override
				public List<String> getReadableTypeList() {
					return Collections.unmodifiableList(_types);
				}
			}
			
			/** More complex case - auto type is turned on. Since types don't support wildcards except for "everything" (ie don't specify index)
			 *  we'll let people pass in the complete set (ie obtained from a mapping), or none, and then just not use it unless we have to
			 * @author Alex
			 */
			public static class AutoRoTypeContext extends ReadOnlyTypeContext {
				
				/** Construct an auto read-only type context
				 * @param known_types - to restrict the set of indexes passed to a query it is necessary to get the current set of types (via the mapping), otherwise leave blank and don't apply to the query
				 * @param fixed_type fields - 
				 */
				public AutoRoTypeContext(final Optional<List<String>> known_types) {
					_known_types = known_types.orElse(Collections.emptyList()); 
				}
				private List<String> _known_types;
				@Override
				public List<String> getReadableTypeList() {
					return Collections.unmodifiableList(_known_types);
				}
			}
		}
		public static abstract class ReadWriteTypeContext extends TypeContext {
			private ReadWriteTypeContext() {}
			
			/** Returns the type into which to write. Note that for AutoRwTypeContext objects this may not
			 * work with the raw client since it maybe conflict against the existing mapping. The CRUD service automatically detects and works around this
			 * if you know which type an object should map to (eg via some util code, or based on cloning an existing object) then use FixedRwTypeContext instead.
			 * @return
			 */
			public abstract String getWriteType();
			
			/** Returns a set of fields that can incur "auto type generation" (because their type is fixed). Parsing errors on these types will always result in the record being dropped  
			 * @return a set of fields that can incur "auto type generation" (because their type is fixed). Parsing errors on these types will always result in the record being dropped
			 */
			public Set<String> fixed_type_fields() {
				return Collections.emptySet();
			}
			
			/** Simple case - a single type into which to write 
			 * @author Alex
			 */
			public static class FixedRwTypeContext extends ReadWriteTypeContext {
				public FixedRwTypeContext(final String type) {
					_type = type; 
				}
				private String _type;
				@Override
				public List<String> getReadableTypeList() {
					return Arrays.asList(_type);
				}
				@Override
				public String getWriteType() {
					return _type;
				}
			}
			
			/** More complex case - auto type is turned on.
			 * @author Alex
			 */
			public static class AutoRwTypeContext extends ReadWriteTypeContext {
				public static final String DEFAULT_PREFIX = "type_";
				
				/** Construct an auto read-write type context
				 * @param known_types - to restrict the set of indexes passed to a query it is necessary to get the current set of types (via the mapping), otherwise leave blank and don't apply to the query
				 */
				public AutoRwTypeContext(final Optional<List<String>> known_types, final Optional<String> prefix, final Set<String> fixed_type_fields) {
					_known_types = known_types.orElse(Collections.emptyList()); 
					_prefix = prefix.orElse(DEFAULT_PREFIX);
					_fixed_type_fields = Collections.unmodifiableSet(fixed_type_fields);
				}
				private List<String> _known_types;
				private String _prefix;
				private Set<String> _fixed_type_fields;

				/* (non-Javadoc)
				 * @see com.ikanow.aleph2.shared.crud.elasticsearch.data_model.ElasticsearchContext.TypeContext.ReadWriteTypeContext#fixed_type_fields()
				 */
				@Override
				public Set<String> fixed_type_fields() {
					return _fixed_type_fields;
				}				
				
				@Override
				public List<String> getReadableTypeList() {
					return Collections.unmodifiableList(_known_types);
				}
				public String getPrefix() {
					return _prefix;
				}
				@Override
				public String getWriteType() {
					// Just return a placeholder, handle conflicts inside Crud Service
					return _prefix + Integer.toString(1);
				}
			}
		}
	}
}
