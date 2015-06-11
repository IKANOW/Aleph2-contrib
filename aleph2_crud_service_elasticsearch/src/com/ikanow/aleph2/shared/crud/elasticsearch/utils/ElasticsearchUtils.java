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
package com.ikanow.aleph2.shared.crud.elasticsearch.utils;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.sort.SortOrder;

import scala.Tuple2;

import com.google.common.collect.LinkedHashMultimap;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.CrudUtils.MultiQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.Operator;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;

/** Utilities for converting from generic CRUD commands to Elasticsearch
 * @author acp
 */
public class ElasticsearchUtils {

	////////////////////////////////////////////////////////////////////////////////////////
	
	// CREATE QUERY
	
	/** Top-level entry point to convert a generic Aleph2 CRUD component into a function MongoDB query
	 * @param query_in the generic query component
	 * @return a tuple2, first element is the query, second element contains the meta ("$skip", "$limit")
	 */
	@SuppressWarnings("unchecked")
	public static <T> Tuple2<FilterBuilder, UnaryOperator<SearchRequestBuilder>> convertToElasticsearchFilter(final QueryComponent<T> query_in) {
		
		final Function<List<FilterBuilder>, FilterBuilder> andVsOr = getMultiOperator(query_in.getOp());
		
		final FilterBuilder query_out = Patterns.match(query_in)
				.<FilterBuilder>andReturn()
					.when((Class<SingleQueryComponent<T>>)(Class<?>)SingleQueryComponent.class, q -> convertToElasticsearchFilter_single(andVsOr, q))
					.when((Class<MultiQueryComponent<T>>)(Class<?>)MultiQueryComponent.class, q -> convertToElasticsearchFilter_multi(andVsOr, q))
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
	protected static FilterBuilder operatorToFilter(final String field, final Tuple2<Operator, Tuple2<Object, Object>> operator_args) {
		
		return Patterns.match(operator_args).<FilterBuilder>andReturn()
				
				.when(op_args -> Operator.exists == op_args._1(), op_args -> {
					final FilterBuilder exists = FilterBuilders.existsFilter(field);
					return objToBool(op_args._2()._1()) ? exists : FilterBuilders.notFilter(exists);
				})
				
				.when(op_args -> (Operator.any_of == op_args._1()), op_args -> FilterBuilders.termsFilter(field, (Iterable<?>)op_args._2()._1()).execution("or"))
				.when(op_args -> (Operator.all_of == op_args._1()), op_args -> FilterBuilders.termsFilter(field, (Iterable<?>)op_args._2()._1()).execution("and")) 

				.when(op_args -> (Operator.equals == op_args._1()) && (null != op_args._2()._2()), op_args -> FilterBuilders.notFilter(FilterBuilders.termFilter(field, op_args._2()._2())) )
				.when(op_args -> (Operator.equals == op_args._1()), op_args -> FilterBuilders.termFilter(field, op_args._2()._1()) )
										
				.when(op_args -> EnumSet.of(Operator.range_open_open, Operator.range_open_closed, Operator.range_closed_closed, Operator.range_closed_open).contains(op_args._1()), op_args -> {					
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
				.otherwise(__ -> l -> emptyOr(l, () -> FilterBuilders.orFilter(l.toArray(new FilterBuilder[0])))); //(ie and)
	}
	
	/** Creates a big and/or list of the list of "multi query components"
	 * @param andVsOr - top level and/or operator applicator
	 * @param query_in - a multi query
	 * @return the Elasticsearch filter object (no meta - that is added above)
	 */
	protected static <T> FilterBuilder convertToElasticsearchFilter_multi(final Function<List<FilterBuilder>, FilterBuilder> andVsOr, final MultiQueryComponent<T> query_in) {
		
		return andVsOr.apply(query_in.getElements().stream().map(entry -> 
								convertToElasticsearchFilter_single(getMultiOperator(entry.getOp()), entry)).collect(Collectors.toList()));
	}	
	
	/** Creates a big $and/$or list of the list of fields in the single query component
	 * @param andVsOr - top level and/or operator applicator
	 * @param query_in - a single query (ie set of fields)
	 * @return the MongoDB query object (no meta - that is added above)
	 */
	protected static <T> FilterBuilder convertToElasticsearchFilter_single(final Function<List<FilterBuilder>, FilterBuilder> andVsOr, final SingleQueryComponent<T> query_in) {
		final LinkedHashMultimap<String, Tuple2<Operator, Tuple2<Object, Object>>> fields = query_in.getAll();
		
		// The actual query:

		return fields.isEmpty()
			? FilterBuilders.matchAllFilter()
			: andVsOr.apply(
				fields.asMap().entrySet().stream()
							.<Tuple2<String, Tuple2<Operator, Tuple2<Object, Object>>>>
								flatMap(entry -> entry.getValue().stream().map( val -> Tuples._2T(entry.getKey(), val) ) )
							.map(entry -> operatorToFilter(entry._1(), entry._2()))
							.collect(Collectors.toList())
							);
	}
}
