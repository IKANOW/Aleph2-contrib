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
package com.ikanow.aleph2.shared_services.crud.mongodb.utils;

import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collector.Characteristics;

import org.checkerframework.checker.nullness.qual.NonNull;

import scala.Tuple2;

import com.google.common.collect.LinkedHashMultimap;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.CrudUtils.MultiQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.Operator;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/** Utilities for converting from generic CRUD commands to MongoDB
 * @author acp
 */
public class MongoDbUtils {

	////////////////////////////////////////////////////////////////////////////////////////
	
	// CREATE QUERY
	
	/** Top-level entry point to convert a generic Aleph2 CRUD component into a function MongoDB query
	 * @param query_in the generic query component
	 * @return a tuple2, first element is the query, second element contains the meta ("$skip", "$limit")
	 */
	@SuppressWarnings("unchecked")
	@NonNull
	public static <T> Tuple2<DBObject,DBObject> convertToMongoQuery(@NonNull QueryComponent<T> query_in) {
		
		final String andVsOr = getOperatorName(query_in.getOp());
		
		final DBObject query_out = Patterns.match(query_in)
				.<DBObject>andReturn()
				.when((Class<SingleQueryComponent<T>>)(Class<?>)SingleQueryComponent.class, q -> convertToMongoQuery_single(andVsOr, q))
				.when((Class<MultiQueryComponent<T>>)(Class<?>)MultiQueryComponent.class, q -> convertToMongoQuery_multi(andVsOr, q))
				.otherwise(() -> (DBObject)new BasicDBObject());
		
		// Meta commands
		
		final BasicDBObject meta = new BasicDBObject();
		
		if (null != query_in.getLimit()) meta.put("$limit", query_in.getLimit());
		final BasicDBObject sort = Patterns.match(query_in.getOrderBy())
									.<BasicDBObject>andReturn()
									.when(l -> l == null, l -> null)
									.otherwise(l -> {
												BasicDBObject s = new BasicDBObject();
												l.stream().forEach(field_order -> s.put(field_order._1(), field_order._2()));
												return s;
											});
		if (null != sort) meta.put("$sort", sort);
		
		return Tuples._2T(query_out, meta);		
	}

	//////////////////////////////////////////////////////////////////////
	
	// UTILS
	
	/** Creates the MongoDB clause from the QueryComponent inner object
	 * @param field - the field used in the clause
	 * @param operator_args - an operator enum and a pair of objects whose context depends on the operator
	 * @return the MongoDB clause
	 */
	@NonNull
	protected static BasicDBObject operatorToMongoKey(@NonNull String field, @NonNull Tuple2<Operator, Tuple2<Object, Object>> operator_args) {
		return Patterns.match(operator_args).<BasicDBObject>andReturn()
				.when(op_args -> Operator.exists == op_args._1(), op_args -> new BasicDBObject(field, new BasicDBObject("$exists", op_args._2()._1())) )
				
				.when(op_args -> (Operator.any_of == op_args._1()), op_args -> new BasicDBObject(field, new BasicDBObject("$in", op_args._2()._1())) )
				.when(op_args -> (Operator.all_of == op_args._1()), op_args -> new BasicDBObject(field, new BasicDBObject("$all", op_args._2()._1())) )
				
				.when(op_args -> (Operator.equals == op_args._1()) && (null != op_args._2()._2()), op_args -> new BasicDBObject(field, new BasicDBObject("$ne", op_args._2()._2())) )
				.when(op_args -> (Operator.equals == op_args._1()), op_args -> new BasicDBObject(field, op_args._2()._1()) )
				
				.when(op_args -> Operator.range_open_open == op_args._1(), op_args -> {
					QueryBuilder qb = QueryBuilder.start(field);
					if (null != op_args._2()._1()) qb = qb.greaterThan(op_args._2()._1());
					if (null != op_args._2()._2()) qb = qb.lessThan(op_args._2()._2());
					return (BasicDBObject) qb.get(); 
				})
				.when(op_args -> Operator.range_open_closed == op_args._1(), op_args -> {
					QueryBuilder qb = QueryBuilder.start(field);
					if (null != op_args._2()._1()) qb = qb.greaterThan(op_args._2()._1());
					if (null != op_args._2()._2()) qb = qb.lessThanEquals(op_args._2()._2());
					return (BasicDBObject) qb.get(); 
				})
				.when(op_args -> Operator.range_closed_closed == op_args._1(), op_args -> {
					QueryBuilder qb = QueryBuilder.start(field);
					if (null != op_args._2()._1()) qb = qb.greaterThanEquals(op_args._2()._1());
					if (null != op_args._2()._2()) qb = qb.lessThanEquals(op_args._2()._2());
					return (BasicDBObject) qb.get(); 
				})
				.when(op_args -> Operator.range_closed_open == op_args._1(), op_args -> {
					QueryBuilder qb = QueryBuilder.start(field);
					if (null != op_args._2()._1()) qb = qb.greaterThanEquals(op_args._2()._1());
					if (null != op_args._2()._2()) qb = qb.lessThan(op_args._2()._2());
					return (BasicDBObject) qb.get(); 
				})
				.otherwise(op_args -> new BasicDBObject());
	}

	/** Top-level "is this query ANDing terms or ORing them"
	 * @param op_in - the operator enum
	 * @return - the mongodb operator
	 */
	@NonNull
	protected static String getOperatorName(@NonNull Operator op_in) {		
		return Patterns.match(op_in).<String>andReturn()
				.when(op -> Operator.any_of == op, op -> "$or")
				.when(op -> Operator.all_of == op, op -> "$and")
				.otherwise(op -> "$and");
	}
	
	/** Creates a big $and/$or list of the list of "multi query components"
	 * @param andVsOr - top level MongoDB operator
	 * @param query_in - a multi query
	 * @return the MongoDB query object (no meta - that is added above)
	 */
	@NonNull
	protected static <T> DBObject convertToMongoQuery_multi(@NonNull String andVsOr, @NonNull MultiQueryComponent<T> query_in) {
		
		return Patterns.match(query_in.getElements())
				.<DBObject>andReturn()
				.when(f -> f.isEmpty(), f -> new BasicDBObject())
				.otherwise(f -> f.stream().collect(
					Collector.of( 
						BasicDBList::new,
						(acc, entry) -> {
							acc.add(convertToMongoQuery_single(getOperatorName(entry.getOp()), entry));
						},
						(a, b) -> { a.addAll(b); return a; },
						acc -> (DBObject)new BasicDBObject(andVsOr, acc),
						Characteristics.UNORDERED))); 
	}	
	
	/** Creates a big $and/$or list of the list of fields in the single query component
	 * @param andVsOr - top level MongoDB operator
	 * @param query_in - a single query (ie set of fields)
	 * @return the MongoDB query object (no meta - that is added above)
	 */
	@NonNull
	protected static <T> DBObject convertToMongoQuery_single(@NonNull String andVsOr, @NonNull SingleQueryComponent<T> query_in) {
		final LinkedHashMultimap<String, Tuple2<Operator, Tuple2<Object, Object>>> fields = query_in.getAll();
		
		// The actual query:

		return Patterns.match(fields).<DBObject>andReturn()
				.when(f -> f.isEmpty(), f -> new BasicDBObject())
				.otherwise(f -> f.asMap().entrySet().stream()
					.<Tuple2<String, Tuple2<Operator, Tuple2<Object, Object>>>>
						flatMap(entry -> entry.getValue().stream().map( val -> Tuples._2T(entry.getKey(), val) ) )
					.collect(	
						Collector.of(
							BasicDBObject::new,
							(acc, entry) -> {
								Patterns.match(acc.get(andVsOr)).andAct()
									.when(l -> (null == l), l -> {
										BasicDBList dbl = new BasicDBList();
										dbl.add(operatorToMongoKey(entry._1(), entry._2()));
										acc.put(andVsOr, dbl);
									})
									.when(BasicDBList.class, l -> l.add(operatorToMongoKey(entry._1(), entry._2())))
									.otherwise(() -> {});
							},
							(a, b) -> { a.putAll(b.toMap()); return a; },
							Characteristics.UNORDERED)));		
	}
	////////////////////////////////////////////////////////////////////////////////////////
	
	// CREATE UPDATE
	
	/** Create a MongoDB ipdate object
	 * @param set overwrites any fields
	 * @param add increments numbers or adds to sets/lists
	 * @param remove decrements numbers of removes from sets/lists
	 * @return
	 */
	public static <O> DBObject createUpdateObject(Optional<O> set, Optional<O> add, Optional<O> remove) {

		final BasicDBObject update_object = new BasicDBObject();
		
		// Set is the easy one:
		update_object.put("$set", 
				CrudUtils.allOf(set).getAll().entries().stream().collect(
						Collector.of(
								BasicDBObject::new,
								((acc, kv) -> acc.put(kv.getKey(), kv.getValue()._1())),
								(a, b) -> { a.putAll(b.toMap()); return a; },
								Characteristics.UNORDERED))); 
		
		// For add:
		// value - numeric ... $inc
		// value - other $push
		// list $push: $each
		// set $addToSet: $each
		
		// For remove
		// whenNotExists - $unset
		// value - $pull
		// list - $pullAll
		// empty list - $pop
		
		//TODO: $bit, $mul, $, $slice ($push), $min, $max 
		
		//TODO
		return update_object;
	}

}
