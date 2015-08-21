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
import java.util.stream.Collectors;

import org.elasticsearch.client.Client;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.shared.crud.elasticsearch.utils.ElasticsearchContextUtils;

import scala.Tuple2;

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
					return Collections.unmodifiableList(_indexes);
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
					if (!date_range.isPresent()) { // Convert to wildcards
						return _indexes.stream().map(i -> i.replaceFirst("_[^_]+$", "_*")).collect(Collectors.toList());
					}
					else {
						return _indexes.stream()
									.flatMap(i -> ElasticsearchContextUtils.getIndexesFromDateRange(i, date_range.get()))
									.map(s -> s + '*')
									.collect(Collectors.toList());
					}
				} 
			}
		}
		/** ADT encapsulating information about a read-write single index - see enclosing class for details
		 * @author Alex
		 */
		public static abstract class ReadWriteIndexContext extends IndexContext {
			private ReadWriteIndexContext() {}
			
			/** Gets the index to write to
			 * @param writable_object - only used in time-based indexes, if optional then "now" is used
			 * @return an index that can be used for "client" writes
			 */
			public abstract String getWritableIndex(Optional<JsonNode> writable_object);
			
			/** Least interesting case ever! Single index
			 * @author Alex
			 */
			public static class FixedRwIndexContext extends ReadWriteIndexContext {
				public FixedRwIndexContext(final String index) {
					_index = index;
				}
				final String _index;
				
				@Override
				public List<String> getReadableIndexList(Optional<Tuple2<Long, Long>> date_range) {
					return Arrays.asList(_index);
				}

				@Override
				public String getWritableIndex(Optional<JsonNode> writable_object) {
					return _index;
				}
			}
			
			/** Just one index but it is time-based ie contains _{TIME_SIGNATURE}
			 * @author Alex
			 */
			public static class TimedRwIndexContext extends ReadWriteIndexContext {
				/** Created a time-based index context
				 * @param index - index name including pattern
				 * @param time_field - the field to use, will just use "now" if left blank
				 */
				public TimedRwIndexContext(final String index, final Optional<String> time_field) {
					_index = index;
					_time_field = time_field;
					_index_split = ElasticsearchContextUtils.splitTimeBasedIndex(_index);
					_formatter = ThreadLocal.withInitial(() -> new SimpleDateFormat(_index_split._2()));
					
				}
				final String _index;
				final Optional<String> _time_field;
				final Tuple2<String, String> _index_split;
				final ThreadLocal<SimpleDateFormat> _formatter;
				
				public Optional<String> timeField() {
					return _time_field;
				}
				
				@Override
				public List<String> getReadableIndexList(final Optional<Tuple2<Long, Long>> date_range) {
					if (!date_range.isPresent()) { // Convert to wildcards
						return Arrays.asList(_index.replaceFirst("_[^_]+$", "_*"));
					}
					else {
						return ElasticsearchContextUtils.getIndexesFromDateRange(_index, date_range.get()).map(s -> s + '*').collect(Collectors.toList());
					}
				}
				@Override
				public String getWritableIndex(final Optional<JsonNode> writable_object) {
					final Date d = _time_field
											.filter(__ -> writable_object.isPresent())
											.map(t -> writable_object.get().get(t))
											.filter(j -> j.isLong())
											.map(j -> new Date(j.asLong()))
										.orElseGet(() -> new Date()); // (else just use "now")
							
					final String formatted_date = _formatter.get().format(d);

					return ElasticsearchContextUtils.reconstructTimedBasedSplitIndex(_index_split._1(), formatted_date);
				}
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
				public AutoRwTypeContext(final Optional<List<String>> known_types, final Optional<String> prefix) {
					_known_types = known_types.orElse(Collections.emptyList()); 
					_prefix = prefix.orElse(DEFAULT_PREFIX);
				}
				private List<String> _known_types;
				private String _prefix;
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
