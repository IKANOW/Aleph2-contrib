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
package com.ikanow.aleph2.shared.crud.mongodb.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Collector.Characteristics;

import org.mongojack.internal.MongoJackModule;
import org.mongojack.internal.object.BsonObjectGenerator;

import scala.Tuple2;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.BeanTemplate;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateOperator;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.CrudUtils.MultiQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.Operator;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
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
	public static <T> Tuple2<DBObject,DBObject> convertToMongoQuery(final QueryComponent<T> query_in) {
		
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
	protected static BasicDBObject operatorToMongoKey(final String field, final Tuple2<Operator, Tuple2<Object, Object>> operator_args) {
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
	protected static String getOperatorName(final Operator op_in) {		
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
	protected static <T> DBObject convertToMongoQuery_multi(final String andVsOr, final MultiQueryComponent<T> query_in) {
		
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
	protected static <T> DBObject convertToMongoQuery_single(final String andVsOr, final SingleQueryComponent<T> query_in) {
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

	/** Create a DB object from a bean template
	 * @param bean_template
	 * @return
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonGenerationException 
	 */
	public static DBObject convertBeanTemplate(BeanTemplate<Object> bean_template, ObjectMapper object_mapper) {
		try {
			final BsonObjectGenerator generator = new BsonObjectGenerator();
	        object_mapper.writeValue(generator, bean_template.get());
	        return generator.getDBObject();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	/** Create a DB object from a JsonNode
	 * @param bean_template
	 * @return
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonGenerationException 
	 */
	public static DBObject convertJsonBean(JsonNode json, ObjectMapper object_mapper) {
		try {
			final BsonObjectGenerator generator = new BsonObjectGenerator();
	        object_mapper.writeTree(generator, json);
	        return generator.getDBObject();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	/** Create a MongoDB update object
	 * @param update - the generic specification
	 * @param add increments numbers or adds to sets/lists
	 * @param remove decrements numbers of removes from sets/lists
	 * @return the mongodb object
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonGenerationException 
	 */
	@SuppressWarnings("unchecked")
	public static <O> DBObject createUpdateObject(final UpdateComponent<O> update) {
		final ObjectMapper object_mapper = MongoJackModule.configure(BeanTemplateUtils.configureMapper(Optional.empty()));
		
		return update.getAll().entries().stream()
					.map(kv -> Patterns.match(kv.getValue()._2())
								.<Map.Entry<String, Tuple2<UpdateOperator, Object>>>andReturn()
								// Special case, handle bean template
								.when(e -> null == e, __ -> kv)
								.when(JsonNode.class, j -> 
									Maps.immutableEntry(kv.getKey(),  Tuples._2T(kv.getValue()._1(), convertJsonBean(j, object_mapper))))
								.when(BeanTemplate.class, e -> 
									Maps.immutableEntry(kv.getKey(),  Tuples._2T(kv.getValue()._1(), convertBeanTemplate(e, object_mapper))))
								// Special case, handle list of bean templates
								.when(Collection.class, l -> !l.isEmpty() && (l.iterator().next() instanceof JsonNode),
										l -> Maps.immutableEntry(kv.getKey(),  Tuples._2T(kv.getValue()._1(), 
												l.stream().map(j -> convertJsonBean((JsonNode)j, object_mapper))
															.collect(Collectors.toList()))))								
								.when(Collection.class, l -> !l.isEmpty() && (l.iterator().next() instanceof BeanTemplate),
										l -> Maps.immutableEntry(kv.getKey(),  Tuples._2T(kv.getValue()._1(), 
												l.stream().map(e -> convertBeanTemplate((BeanTemplate<Object>)e, object_mapper))
															.collect(Collectors.toList()))))								
								.otherwise(() -> kv))
					.collect(
						Collector.of(
							BasicDBObject::new,
							(acc, kv) -> {
								Patterns.match(kv.getValue()._2()).andAct()
									// Delete operator, bunch of things have to happen for safety
									.when(o -> ((UpdateOperator.unset == kv.getValue()._1()) && kv.getKey().isEmpty() && (null == kv.getValue()._2())), 
											o -> acc.put("$unset", null))
									//Increment
									.when(Number.class, n -> (UpdateOperator.increment == kv.getValue()._1()), 
											n -> nestedPut(acc, "$inc", kv.getKey(), n))
									// Set
									.when(o -> (UpdateOperator.set == kv.getValue()._1()), 
											o -> nestedPut(acc, "$set", kv.getKey(), o))
									// Unset
									.when(o -> (UpdateOperator.unset == kv.getValue()._1()), 
											o -> nestedPut(acc, "$unset", kv.getKey(), 1))
									// Add items/item to list
									.when(Collection.class, c -> (UpdateOperator.add == kv.getValue()._1()), 
											c -> nestedPut(acc, "$push", kv.getKey(), new BasicDBObject("$each", c)))
									.when(o -> (UpdateOperator.add == kv.getValue()._1()), 
											o -> nestedPut(acc, "$push", kv.getKey(), o))
									// Add item/items to set
									.when(Collection.class, c -> (UpdateOperator.add_deduplicate == kv.getValue()._1()), 
											c -> nestedPut(acc, "$addToSet", kv.getKey(), new BasicDBObject("$each", c)))
									.when(o -> (UpdateOperator.add_deduplicate == kv.getValue()._1()), 
											o -> nestedPut(acc, "$addToSet", kv.getKey(), o))
									// Remove items from list by query
									.when(QueryComponent.class, q -> (UpdateOperator.remove == kv.getValue()._1()), 
											q -> nestedPut(acc, "$pull", kv.getKey(), convertToMongoQuery(q)._1()))
									// Remove items/item from list
									.when(Collection.class, c -> (UpdateOperator.remove == kv.getValue()._1()), 
											c -> nestedPut(acc, "$pullAll", kv.getKey(), c))
									.when(o -> (UpdateOperator.remove == kv.getValue()._1()), 
											o -> nestedPut(acc, "$pullAll", kv.getKey(), Arrays.asList(o)))
									.otherwise(() -> {}); // (do nothing)
							},
							(a, b) -> { a.putAll(b.toMap()); return a; },
							Characteristics.UNORDERED)); 
	}

	/** Inserts an object into field1.field2, creating objects along the way
	 * @param mutable the mutable object into which the the nested field is inserted
	 * @param parent the top level fieldname
	 * @param nested the nested fieldname 
	 * @param to_insert the object to insert
	 */
	protected static void nestedPut(final BasicDBObject mutable, final String parent, final String nested, final Object to_insert) {
		final DBObject dbo = (DBObject) mutable.get(parent);
		if (null != dbo) {
			dbo.put(nested, to_insert);
		}
		else {
			BasicDBObject new_dbo = new BasicDBObject();
			new_dbo.put(nested, to_insert);
			mutable.put(parent, new_dbo);
		}
	}
}
