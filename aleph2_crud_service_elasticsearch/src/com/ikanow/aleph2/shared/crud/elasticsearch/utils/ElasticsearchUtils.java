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
package com.ikanow.aleph2.shared.crud.elasticsearch.utils;

import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.sort.SortOrder;

import scala.Tuple2;

import com.google.common.collect.LinkedHashMultimap;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.TimeUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.CrudUtils.MultiQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.Operator;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;

/** Utilities for converting from generic CRUD commands to Elasticsearch
 * @author acp
 */
public class ElasticsearchUtils {

	final public static EnumSet<Operator> _RANGE_OP = EnumSet.of(Operator.range_open_open, Operator.range_open_closed, Operator.range_closed_closed, Operator.range_closed_open);
	
	//TOOD: put map comparison in here?
	
	//TODO: put code to check if the mapping supports _id ranges in here?
	
	//TODO: hmm need to pass a list of nested fields in and then do queries on those fields differently?
	
	////////////////////////////////////////////////////////////////////////////////////////
	
	// CREATE QUERY
	
	/** Top-level entry point to convert a generic Aleph2 CRUD component into an Elasticsearch filter ("complex" _id queries not supported)
	 * @param query_in the generic query component
	 * @param id_ranges_ok - true if the _id is indexed, enables range queries on _ids, false - if not, only all/single-term queries supported
	 * @return a tuple2, first element is the query, second element contains the meta ("$skip", "$limit")
	 */
	public static <T> Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> convertToElasticsearchFilter(final QueryComponent<T> query_in) {
		return convertToElasticsearchFilter(query_in, false);
	}
	
	/** Top-level entry point to convert a generic Aleph2 CRUD component into an Elasticsearch filter
	 * @param query_in the generic query component
	 * @param id_ranges_ok - true if the _id is indexed, enables range queries on _ids, false - if not, only all/single-term queries supported
	 * @return a tuple2, first element is the query, second element contains the meta ("$skip", "$limit")
	 */
	@SuppressWarnings("unchecked")
	public static <T> Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> convertToElasticsearchFilter(final QueryComponent<T> query_in, boolean id_ranges_ok) {
		
		final Function<List<FilterBuilder>, FilterBuilder> andVsOr = getMultiOperator(query_in.getOp());
		
		final FilterBuilder query_out = Patterns.match(query_in)
				.<FilterBuilder>andReturn()
					.when((Class<SingleQueryComponent<T>>)(Class<?>)SingleQueryComponent.class, q -> convertToElasticsearchFilter_single(andVsOr, q, id_ranges_ok))
					.when((Class<MultiQueryComponent<T>>)(Class<?>)MultiQueryComponent.class, q -> convertToElasticsearchFilter_multi(andVsOr, q, id_ranges_ok))
					.otherwise(() -> FilterBuilders.matchAllFilter());
		
		// Meta commands

		final UnaryOperator<SearchRequestBuilder> op = srb -> {
			Optional.of(srb)
				.map(s -> (null != query_in.getLimit()) ? s.setSize(query_in.getLimit().intValue()) : s)
				.map(s -> Optionals.ofNullable(query_in.getOrderBy()).stream().reduce(s,
							(acc, field_order) -> acc.addSort(field_order._1(), (field_order._2() < 0) ? SortOrder.DESC : SortOrder.ASC),
							(acc1, acc2) -> { throw new RuntimeException("Dev: remove the parallel() from this stream!"); }
						))
				.get();
			return srb;
		};
		
		return Tuples._2T(query_out, op);		
	}

	//////////////////////////////////////////////////////////////////////
	
	// UTILS
	
	/** Defaults to true unless o is non null and boolean and false!
	 * @param o - the object under test
	 * @return true unless o is non null and boolean and false!
	 */
	private static boolean objToBool(Object o) {
		return Optional.of(o).map(o_exists -> (o_exists instanceof Boolean) ? (Boolean)o_exists : false ).orElse(true);
	}
	
	/** Creates the MongoDB clause from the QueryComponent inner object
	 * @param field - the field used in the clause
	 * @param operator_args - an operator enum and a pair of objects whose context depends on the operator
	 * @return the MongoDB clause
	 */
	protected static FilterBuilder operatorToFilter(final String field, final Tuple2<Operator, Tuple2<Object, Object>> operator_args, boolean id_ranges_ok) {
		
		return Patterns.match(operator_args).<FilterBuilder>andReturn()

				//(es - handle _ids/_types differently)
				.when(op_args -> field.equals(JsonUtils._ID) && Operator.exists == op_args._1(), op_args -> { throw new RuntimeException(ErrorUtils.EXISTS_ON_IDS); })				
				.when(op_args -> field.equals("_type") && Operator.exists == op_args._1(), op_args -> { throw new RuntimeException(ErrorUtils.EXISTS_ON_TYPES); })				
				
				.when(op_args -> Operator.exists == op_args._1(), op_args -> {
					final FilterBuilder exists = FilterBuilders.existsFilter(field);
					return objToBool(op_args._2()._1()) ? exists : FilterBuilders.notFilter(exists);
				})

				//(es - handle _ids/_types differently)
				.when(op_args -> field.equals(JsonUtils._ID) && (Operator.any_of == op_args._1()), op_args -> 
					FilterBuilders.idsFilter().addIds(StreamSupport.stream(((Iterable<?>)op_args._2()._1()).spliterator(), false).map(x -> x.toString()).collect(Collectors.toList()).toArray(new String[0])))										
				.when(op_args -> field.equals(JsonUtils._ID) && (Operator.all_of == op_args._1()), __ -> { throw new RuntimeException(ErrorUtils.ALL_OF_ON_IDS); }) 				
				.when(op_args -> field.equals("_type") && (Operator.any_of == op_args._1()), __ -> { throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED, "any_of/_type")); })
				.when(op_args -> field.equals("_type") && (Operator.all_of == op_args._1()), __ -> { throw new RuntimeException(ErrorUtils.ALL_OF_ON_TYPES); }) 				
				
				.when(op_args -> (Operator.any_of == op_args._1()), op_args -> FilterBuilders.termsFilter(field, (Iterable<?>)op_args._2()._1()).execution("or"))
				.when(op_args -> (Operator.all_of == op_args._1()), op_args -> FilterBuilders.termsFilter(field, (Iterable<?>)op_args._2()._1()).execution("and")) 

				//(es - handle _ids/_types differently)
				.when(op_args -> field.equals(JsonUtils._ID) && (Operator.equals == op_args._1()) && (null != op_args._2()._2()), op_args -> FilterBuilders.notFilter(FilterBuilders.idsFilter().addIds(op_args._2()._2().toString())) )
				.when(op_args -> field.equals(JsonUtils._ID) && (Operator.equals == op_args._1()), op_args -> FilterBuilders.idsFilter().addIds(op_args._2()._1().toString()) )				
				.when(op_args -> field.equals("_type") && (Operator.equals == op_args._1()) && (null != op_args._2()._2()), op_args -> FilterBuilders.notFilter(FilterBuilders.typeFilter(op_args._2()._2().toString())) )
				.when(op_args -> field.equals("_type") && (Operator.equals == op_args._1()), op_args -> FilterBuilders.typeFilter(op_args._2()._1().toString()) )				
				
				.when(op_args -> (Operator.equals == op_args._1()) && (null != op_args._2()._2()), op_args -> FilterBuilders.notFilter(FilterBuilders.termFilter(field, op_args._2()._2())) )
				.when(op_args -> (Operator.equals == op_args._1()), op_args -> FilterBuilders.termFilter(field, op_args._2()._1()) )
										
				// unless id_ranges_ok, exception out here:
				.when(op_args -> field.equals(JsonUtils._ID) && !id_ranges_ok && _RANGE_OP.contains(op_args._1()), __ -> {
					throw new RuntimeException(ErrorUtils.NO_ID_RANGES_UNLESS_IDS_INDEXED);
				})
				.when(op_args -> field.equals("_type"),  __ -> { throw new RuntimeException(ErrorUtils.RANGES_ON_TYPES); })
				
				.when(op_args ->_RANGE_OP.contains(op_args._1()), op_args -> {					
					return Optional.of(FilterBuilders.rangeFilter(field))
								.map(f -> Optional.ofNullable(op_args._2()._1()).map(b -> 
												f.from(b).includeLower(EnumSet.of(Operator.range_closed_closed, Operator.range_closed_open).contains(op_args._1())))
											.orElse(f))
								.map(f -> Optional.ofNullable(op_args._2()._2()).map(b -> 
												f.to(b).includeUpper  (EnumSet.of(Operator.range_open_closed, Operator.range_closed_closed).contains(op_args._1())))
											.orElse(f))
								.get();
				})
				.otherwise(op_args -> FilterBuilders.matchAllFilter());
	}

	/** Runs the query in isolation to check if it needs _id to be indexed in order to work
	 * @param query_in - the query to test
	 * @return - basically, whether to check the index/types' mapping(s) to see if an _id query is supported
	 */
	public static <T> boolean queryContainsIdRanges(final QueryComponent<T> query_in) {
		try {
			convertToElasticsearchFilter(query_in, false);
			return true; // didn't throw so we're good
		}
		catch (RuntimeException re) {
			if (re.getMessage().equals(ErrorUtils.NO_ID_RANGES_UNLESS_IDS_INDEXED)) {
				return true;
			}
			throw re; // (just pass the error upwards)
		}
	}
	
	/** Handy util function
	 * @param l
	 * @param getter
	 * @return
	 */
	private static FilterBuilder emptyOr(final List<FilterBuilder> l, final Supplier<FilterBuilder> getter) {
		return l.isEmpty() ? FilterBuilders.matchAllFilter() : getter.get();
	}
	
	/** Top-level "is this query ANDing terms or ORing them"
	 * @param op_in - the operator enum
	 * @return - a function to combine a list of filter builders using the designated operator
	 */
	protected static Function<List<FilterBuilder>, FilterBuilder> getMultiOperator(final Operator op_in) {		
		return Patterns.match(op_in).<Function<List<FilterBuilder>, FilterBuilder>>andReturn()
				.when(op -> Operator.any_of == op, __ -> l -> emptyOr(l, () -> FilterBuilders.orFilter(l.toArray(new FilterBuilder[0]))))
				.otherwise(__ -> l -> emptyOr(l, () -> FilterBuilders.andFilter(l.toArray(new FilterBuilder[0])))); //(ie and)
	}
	
	/** Creates a big and/or list of the list of "multi query components"
	 * @param andVsOr - top level and/or operator applicator
	 * @param query_in - a multi query
	 * @return the Elasticsearch filter object (no meta - that is added above)
	 */
	@SuppressWarnings("unchecked")
	protected static <T> FilterBuilder convertToElasticsearchFilter_multi(final Function<List<FilterBuilder>, FilterBuilder> andVsOr, final MultiQueryComponent<T> query_in, boolean id_ranges_ok) {
		
		return andVsOr.apply(query_in.getElements().stream()
				.<FilterBuilder>map(entry -> 
					(FilterBuilder)Patterns.match(entry).<FilterBuilder>andReturn()
						//(^not sure why all this extra cast is needed here, ecj works fine but oraclej complains)
							.when(SingleQueryComponent.class, 
									e -> convertToElasticsearchFilter_single(getMultiOperator(e.getOp()), e, id_ranges_ok))
							.when(MultiQueryComponent.class, 
									e -> convertToElasticsearchFilter_multi(getMultiOperator(((MultiQueryComponent<?>)e).getOp()), (MultiQueryComponent<?>)e, id_ranges_ok))
									//(^not sure why the extra cast is needed here, ecj works fine but oraclej complains)
							.otherwise(e -> { throw new RuntimeException("Internal Logic Error: type: " + e.getClass()); })
				)
				.collect(Collectors.toList()))
				;
	}	
	
	/** Creates a big $and/$or list of the list of fields in the single query component
	 * @param andVsOr - top level and/or operator applicator
	 * @param query_in - a single query (ie set of fields)
	 * @return the MongoDB query object (no meta - that is added above)
	 */
	protected static <T> FilterBuilder convertToElasticsearchFilter_single(final Function<List<FilterBuilder>, FilterBuilder> andVsOr, final SingleQueryComponent<T> query_in, boolean id_ranges_ok) {
		final LinkedHashMultimap<String, Tuple2<Operator, Tuple2<Object, Object>>> fields = query_in.getAll();
		
		// The actual query:

		return fields.isEmpty()
			? FilterBuilders.matchAllFilter()
			: andVsOr.apply(
				fields.asMap().entrySet().stream()
							.<Tuple2<String, Tuple2<Operator, Tuple2<Object, Object>>>>
								flatMap(entry -> entry.getValue().stream().map( val -> Tuples._2T(entry.getKey(), val) ) )
							.map(entry -> operatorToFilter(entry._1(), entry._2(), id_ranges_ok))
							.collect(Collectors.toList())
							);
	}
		
	/** If there's an obvious date range restriction on this query then return it so it can be applied to make queries more efficient
	 * @param spec
	 * @param maybe_time_field
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <O> Optional<Tuple2<Long, Long>> interpretObviousDateRange(final QueryComponent<O> spec, final Optional<String> maybe_time_field) {
		
		return maybe_time_field.flatMap(tf -> {
			
			return Patterns.match(spec).<Optional<Tuple2<Long, Long>>>andReturn()
				.when(MultiQueryComponent.class, mq -> (Operator.all_of == mq.getOp()) || (1 == mq.getElements().size()), mq -> {
					return ((List<QueryComponent<O>>)mq.getElements())
							.stream().limit(10).map(o -> interpretObviousDateRange(o, maybe_time_field))
							.findAny()
							.flatMap(o->o)
							;
				})
				.when(SingleQueryComponent.class, sq -> (Operator.all_of == sq.getOp()) || (1 == sq.getAll().size()), sq -> {
					final SingleQueryComponent<O> ssq = (SingleQueryComponent<O>)sq;
					return ssq.getAll().get(tf).stream()
						.filter(op_args -> _RANGE_OP.contains(op_args._1())) // must be a range query
						.filter(op_args -> null != op_args._2()._1()) // can't be unbounded below
						.map(op_args -> 
								Tuples._2T(
									toLongDate(op_args._2()._1())
									, 
									Optional.ofNullable(op_args._2()._2()).map(o -> toLongDate(o)).orElse(new Date().getTime()))
						)
						.filter(t2 -> (null != t2._1()) && (null != t2._2()))
						.findAny()
						;
				})
				.otherwise(() -> Optional.empty())
				;
		})
		;
	}
	
	/** Quick util to return a date from long/data/iso_string
	 * @param obj
	 * @return
	 */
	protected static Long toLongDate(Object obj) {
		return Patterns.match(obj).<Long>andReturn()
				.when(Long.class, l -> l)
				.when(Date.class, d -> d.getTime())
				.when(String.class, s -> TimeUtils.parseIsoString(s).validation(err->null, d->d.getTime()))
				.otherwise(__ -> null)
				;
	}
}
