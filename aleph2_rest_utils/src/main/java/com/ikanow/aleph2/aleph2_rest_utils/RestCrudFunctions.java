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
package com.ikanow.aleph2.aleph2_rest_utils;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.server.ContainerRequest;

import scala.Tuple2;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;

import fj.data.Either;

/**
 * @author Burch
 *
 */
public class RestCrudFunctions {
	private static Logger _logger = LogManager.getLogger();
	private static final String COUNT_FIELD_NAME = "count";
	private static final String DELETE_SUCCESS_FIELD_NAME = "delete_success";
	
	public enum FunctionType {
		QUERY,
		COUNT
	}
	
	public static <T> Response readFunction(IServiceContext service_context, FunctionType function_type, String service_type, String access_level, String service_identifier, Optional<String> bucket_full_names, 
			Optional<String> query_json, Optional<String> query_id, Optional<Long> limit) {				
		_logger.error("Handling READ request");
		//parse out the url params
		
		final Either<String,Tuple2<ICrudService<T>, Class<T>>> crud_service_either = RestUtils.getCrudService(service_context, service_type, access_level, service_identifier, bucket_full_names);
		if ( crud_service_either.isLeft() )
    		return Response.status(Status.BAD_REQUEST).entity(crud_service_either.left().value()).build();
		
		try {
        	switch ( function_type ) {
			case COUNT:
				return handleCountRequest(query_json, crud_service_either.right().value()._1, crud_service_either.right().value()._2);
			case QUERY:
				return handleQueryRequest(query_json, query_id, crud_service_either.right().value()._1, crud_service_either.right().value()._2, limit);
			default:
				return Response.status(Status.BAD_REQUEST).entity("Unknown GET function type (how did you do this?): " + function_type).build();	        	
        	}
    	} catch ( Exception ex ) {
    		return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error: {0}", ex)).build();
    	}	
	}
	
	public static <T> Response createFunction(IServiceContext service_context, String service_type, String access_level, String service_identifier, Optional<String> bucket_full_names, 
			Optional<String> json) {						
		_logger.error("Handling CREATE request");
		//parse out the url params
		final Either<String,Tuple2<ICrudService<T>, Class<T>>> crud_service_either = RestUtils.getCrudService(service_context, service_type, access_level, service_identifier, bucket_full_names);

		if ( crud_service_either.isLeft() )
    		return Response.status(Status.BAD_REQUEST).entity(crud_service_either.left().value()).build();			
		try {
			return handleCreateRequest(json, crud_service_either.right().value()._1, crud_service_either.right().value()._2);	        		        	
    	} catch ( Exception ex ) {
    		return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error: {0}", ex)).build();
    	}	
	}
	
	public static <T> Response updateFunction(IServiceContext service_context, String service_type, String access_level, String service_identifier, Optional<String> bucket_full_names, 
			Optional<String> json) {
		_logger.error("Handling UPDATE request");
		//parse out the url params
		final Either<String,Tuple2<ICrudService<T>, Class<T>>> crud_service_either = RestUtils.getCrudService(service_context, service_type, access_level, service_identifier, bucket_full_names);
		if ( crud_service_either.isLeft() )
    		return Response.status(Status.BAD_REQUEST).entity(crud_service_either.left().value()).build();
		
		try {
			return handleUpdateRequest(json, crud_service_either.right().value()._1, crud_service_either.right().value()._2);	        		        	
    	} catch ( Exception ex ) {
    		return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error: {0}", ex)).build();
    	}   		
    	
	}
	
	public static <T> Response deleteFunction(IServiceContext service_context, String service_type, String access_level, String service_identifier, Optional<String> bucket_full_names, 
			Optional<String> query_json, Optional<String> query_id) {		
		_logger.error("Handling DELETE request");
		//parse out the url params
		final Either<String,Tuple2<ICrudService<T>, Class<T>>> crud_service_either = RestUtils.getCrudService(service_context, service_type, access_level, service_identifier, bucket_full_names);
		if ( crud_service_either.isLeft() )
    		return Response.status(Status.BAD_REQUEST).entity(crud_service_either.left().value()).build();
		
		try {
			return handleDeleteRequest(query_json, query_id, crud_service_either.right().value()._1, crud_service_either.right().value()._2);	        		        	
    	} catch ( Exception ex ) {
    		return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error: {0}", ex)).build();
    	}
	}
	
	public static <T> Function<ContainerRequestContext, Response> getMultipartCreateFunction(final IServiceContext service_context) {
		
		
		return (containerRequestContext)->{
			_logger.error("Handling Multipart CREATE request");
			
			ContainerRequest cr = (ContainerRequest) containerRequestContext;
		    cr.bufferEntity();
		    FormDataMultiPart multiPart = cr.readEntity(FormDataMultiPart.class);
		   
			return Response.ok( multiPart.getField("a").getValue()).build();
			//parse out the url params
//			final Either<String,Tuple2<ICrudService<T>, Class<T>>> crud_service_either = RestUtils.parseRequest(containerRequestContext, service_context);
//			if ( crud_service_either.isLeft() )
//        		return Response.status(Status.BAD_REQUEST).entity(crud_service_either.left().value()).build();			
//			try {
//				return handleCreateRequest(containerRequestContext, crud_service_either.right().value()._1, crud_service_either.right().value()._2);	        		        	
//        	} catch ( Exception ex ) {
//        		return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error: {0}", ex)).build();
//        	}
		};
	}
	
//	public static <T> Function<ContainerRequestContext, Response> getCreateFunction(IServiceContext service_context) {			
//		return (containerRequestContext)->{
//			_logger.error("Handling CREATE request");
//			//parse out the url params
//			final Either<String,Tuple2<ICrudService<T>, Class<T>>> crud_service_either = RestUtils.parseRequest(containerRequestContext, service_context);
//			if ( crud_service_either.isLeft() )
//        		return Response.status(Status.BAD_REQUEST).entity(crud_service_either.left().value()).build();			
//			try {
//				return handleCreateRequest(containerRequestContext, crud_service_either.right().value()._1, crud_service_either.right().value()._2);	        		        	
//        	} catch ( Exception ex ) {
//        		return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error: {0}", ex)).build();
//        	}
//		};
//	}
//	
//	public static <T> Function<ContainerRequestContext, Response> getUpdateFunction(IServiceContext service_context) {			
//		return (containerRequestContext)->{
//			_logger.error("Handling UPDATE request");
//			//parse out the url params
//			final Either<String,Tuple2<ICrudService<T>, Class<T>>> crud_service_either = RestUtils.parseRequest(containerRequestContext, service_context);
//			if ( crud_service_either.isLeft() )
//        		return Response.status(Status.BAD_REQUEST).entity(crud_service_either.left().value()).build();
//			
//			try {
//				return handleUpdateRequest(containerRequestContext, crud_service_either.right().value()._1, crud_service_either.right().value()._2);	        		        	
//        	} catch ( Exception ex ) {
//        		return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error: {0}", ex)).build();
//        	}
//		};
//	}
//	
//	public static <T> Function<ContainerRequestContext, Response> getDeleteFunction(IServiceContext service_context) {			
//		return (containerRequestContext)->{
//			_logger.error("Handling DELETE request");
//			//parse out the url params
//			final Either<String,Tuple2<ICrudService<T>, Class<T>>> crud_service_either = RestUtils.parseRequest(containerRequestContext, service_context);
//			if ( crud_service_either.isLeft() )
//        		return Response.status(Status.BAD_REQUEST).entity(crud_service_either.left().value()).build();
//			
//			try {
//				return handleDeleteRequest(containerRequestContext, crud_service_either.right().value()._1, crud_service_either.right().value()._2);	        		        	
//        	} catch ( Exception ex ) {
//        		return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error: {0}", ex)).build();
//        	}
//		};
//	}
	
//	private static <T> Response handleCreateRequest(final ContainerRequestContext containerRequestContext, final ICrudService<T> crud_service, final Class<T> clazz) {
//		//get id or a query object that was posted
//    	if (containerRequestContext.hasEntity()) {
//			try {
//				final String json = IOUtils.toString(containerRequestContext.getEntityStream(), "UTF-8");
//				_logger.error("input: " + json);
//				//TODO handle overwriting existing object
//				return Response.ok(RestUtils.convertObjectToJson(crud_service.storeObject(BeanTemplateUtils.from(json, clazz).get()).get().get()).toString()).build();
//			} catch (Exception e) {
//				return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error converting input stream to string: {0}", e)).build();
//			}    		
//    	} else {
//    		return Response.status(Status.BAD_REQUEST).entity("POST requires an object to insert in body of request").build();
//    	}
//	}
//	
//	private static <T> Response handleDeleteRequest(final ContainerRequestContext containerRequestContext, final ICrudService<T> crud_service, final Class<T> clazz) {
//		//get id or a query object that was posted
//		if ( containerRequestContext.getUriInfo().getPathParameters().containsKey("id") ) {
//    		//ID delete
//    		try {
//    			final String id = containerRequestContext.getUriInfo().getPathParameters().getFirst("id");
//        		_logger.error("id: " + id);
//				return Response.ok(RestUtils.convertSingleObjectToJson(crud_service.deleteObjectById(id).get(), DELETE_SUCCESS_FIELD_NAME).toString()).build();
//			} catch ( InterruptedException | ExecutionException e) {
//				return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error converting input stream to string: {0}", e)).build();
//			}
//    	} else if (containerRequestContext.hasEntity()) {
//    		//Body delete
//			try {
//				final String json = IOUtils.toString(containerRequestContext.getEntityStream(), "UTF-8");
//				_logger.error("query: " + json);
//				final QueryComponent<T> query = RestUtils.convertStringToQueryComponent(json, clazz);				
//				return Response.ok(RestUtils.convertSingleObjectToJson(crud_service.deleteObjectBySpec(query).get(), DELETE_SUCCESS_FIELD_NAME).toString()).build();
//			} catch (Exception e) {
//				return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error converting input stream to string: {0}", e)).build();
//			}    		
//    	} else {
//    		//TODO actually we should probably just do something else when this is empty (like return first item, or search all and return limit 10, etc)
//    		return Response.status(Status.BAD_REQUEST).entity("DELETE requires an id in the url or query in the body").build();
//    	}
//	}
//	
//	private static <T> Response handleUpdateRequest(final ContainerRequestContext containerRequestContext, final ICrudService<T> crud_service, final Class<T> clazz) {
//		//get id or a query object that was posted
//    	if (containerRequestContext.hasEntity()) {
//			try {
//				final String json = IOUtils.toString(containerRequestContext.getEntityStream(), "UTF-8");
//				_logger.error("input: " + json);
//				final Tuple2<QueryComponent<T>, UpdateComponent<T>> q_u = RestUtils.convertStringToUpdateComponent(json, clazz);
//				boolean upsert = false; //TODO get from url params
//				boolean before_updated = false; //TODO get from url params
//				return Response.ok(RestUtils.convertObjectToJson(crud_service.updateAndReturnObjectBySpec(q_u._1, Optional.of(upsert), q_u._2, Optional.of(before_updated), Collections.emptyList(), false).get().get()).toString()).build();
//			} catch (Exception e) {
//				return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error converting input stream to string: {0}", e)).build();
//			}    		
//    	} else {
//    		return Response.status(Status.BAD_REQUEST).entity("POST requires an object to insert in body of request").build();
//    	}
//	}
//
//	//TODO should I parse the id/query before calling this and just pass in optionals?
//	private static <T> Response handleQueryRequest(final ContainerRequestContext containerRequestContext, final ICrudService<T> crud_service, final Class<T> clazz) {
//		//get id or a query object that was posted
//    	if ( containerRequestContext.getUriInfo().getPathParameters().containsKey("id") ) {
//    		//ID query
//    		try {
//    			final String id = containerRequestContext.getUriInfo().getPathParameters().getFirst("id");
//        		_logger.error("id: " + id);
//				return Response.ok(RestUtils.convertObjectToJson(crud_service.getObjectById(id).get()).toString()).build();
//			} catch (JsonProcessingException | InterruptedException | ExecutionException e) {
//				return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error converting input stream to string: {0}", e)).build();
//			}
//    	} else if (containerRequestContext.hasEntity()) {
//    		//Body Query
//			try {
//				final String json = IOUtils.toString(containerRequestContext.getEntityStream(), "UTF-8");
//				_logger.error("query: " + json);
//				final QueryComponent<T> query = RestUtils.convertStringToQueryComponent(json, clazz);				
//				return Response.ok(RestUtils.convertCursorToJson(crud_service.getObjectsBySpec(query).get()).toString()).build();
//			} catch (Exception e) {
//				return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error converting input stream to string: {0}", e)).build();
//			}    		
//    	} else {
//    		//TODO actually we should probably just do something else when this is empty (like return first item, or search all and return limit 10, etc)
//    		return Response.status(Status.BAD_REQUEST).entity("GET requires an id in the url or query in the body").build();
//    	}
//	}
//
//	private static <T> Response handleCountRequest(final ContainerRequestContext containerRequestContext, final ICrudService<T> crud_service, final Class<T> clazz) throws InterruptedException, ExecutionException {
//		//get query or if there is none just return count
//		if (containerRequestContext.hasEntity()) {    		
//			try {
//				final String json = IOUtils.toString(containerRequestContext.getEntityStream(), "UTF-8");
//				_logger.error("query: " + json);
//				final QueryComponent<T> query = RestUtils.convertStringToQueryComponent(json, clazz);				
//				return Response.ok(RestUtils.convertSingleObjectToJson(crud_service.countObjectsBySpec(query).get(), COUNT_FIELD_NAME).toString()).build();
//			} catch (IOException e) {
//				return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error converting input stream to string: {0}", e)).build();
//			}
//    		
//    	} else {
//    		//didn't supply a query, just return the total count
//    		return Response.ok(RestUtils.convertSingleObjectToJson(crud_service.countObjects().get(), COUNT_FIELD_NAME).toString()).build();    		
//    	}		
//	}	
	
	//////////////////////NEW NON FUNCTION METHODS
	private static <T> Response handleQueryRequest(final Optional<String> query_json, final Optional<String> query_id, final ICrudService<T> crud_service, final Class<T> clazz,
			Optional<Long> limit) {
		//get id or a query object that was posted
		//TODO switch to query_id.map().orElse() ... just making a quick swap for the moment
    	if ( query_id.isPresent() ) {
    		//ID query
    		try {
    			final String id = query_id.get();
        		_logger.error("id: " + id);
				return Response.ok(RestUtils.convertObjectToJson(crud_service.getObjectById(id).get()).toString()).build();
			} catch (JsonProcessingException | InterruptedException | ExecutionException e) {
				return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error converting input stream to string: {0}", e)).build();
			}
    	} else if (query_json.isPresent()) {
    		//Body Query
			try {
				final String json = query_json.get();
				_logger.error("query: " + json);
				final QueryComponent<T> query = RestUtils.convertStringToQueryComponent(json, clazz, limit);				
				return Response.ok(RestUtils.convertCursorToJson(crud_service.getObjectsBySpec(query).get()).toString()).build();
			} catch (Exception e) {
				return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error converting input stream to string: {0}", e)).build();
			}    		
    	} else {
    		//TODO actually we should probably just do something else when this is empty (like return first item, or search all and return limit 10, etc)
    		return Response.status(Status.BAD_REQUEST).entity("GET requires an id in the url or query in the body").build();
    	}
	}
	
	private static <T> Response handleCountRequest(final Optional<String> query_json, final ICrudService<T> crud_service, final Class<T> clazz) throws InterruptedException, ExecutionException {
		//get query or if there is none just return count	  		
		return query_json.map(json->{
			try {
				_logger.error("query: " + json);
				final QueryComponent<T> query = RestUtils.convertStringToQueryComponent(json, clazz, Optional.empty());				
				return Response.ok(RestUtils.convertSingleObjectToJson(crud_service.countObjectsBySpec(query).get(), COUNT_FIELD_NAME).toString()).build();
			} catch (Exception e) {
				return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error converting input stream to string: {0}", e)).build();
			}
		}).orElse(Response.ok(RestUtils.convertSingleObjectToJson(crud_service.countObjects().get(), COUNT_FIELD_NAME).toString()).build());
	}
	
	private static <T> Response handleCreateRequest(final Optional<String> json, final ICrudService<T> crud_service, final Class<T> clazz) throws JsonProcessingException, InterruptedException, ExecutionException {
		//get id or a query object that was posted
		if ( json.isPresent() ) {
			_logger.error("input: " + json.get());
			//TODO handle overwriting existing object
			return Response.ok(RestUtils.convertObjectToJson(crud_service.storeObject(BeanTemplateUtils.from(json.get(), clazz).get()).get().get()).toString()).build();
		} else {
			return Response.status(Status.BAD_REQUEST).entity("POST requires json in the body").build();
		}
	}
	
	private static <T> Response handleUpdateRequest(final Optional<String> json, final ICrudService<T> crud_service, final Class<T> clazz) throws JsonParseException, JsonMappingException, IOException, InterruptedException, ExecutionException {
		//get id or a query object that was posted
		if ( json.isPresent() ) {			
			_logger.error("input: " + json.get());
			final Tuple2<QueryComponent<T>, UpdateComponent<T>> q_u = RestUtils.convertStringToUpdateComponent(json.get(), clazz);
			boolean upsert = false; //TODO get from url params
			boolean before_updated = false; //TODO get from url params
			return Response.ok(RestUtils.convertObjectToJson(crud_service.updateAndReturnObjectBySpec(q_u._1, Optional.of(upsert), q_u._2, Optional.of(before_updated), Collections.emptyList(), false).get().get()).toString()).build();
		} else {
			return Response.status(Status.BAD_REQUEST).entity("POST requires json in the body").build();
		}    
	}
	
	private static <T> Response handleDeleteRequest(final Optional<String> query_json, final Optional<String> query_id, final ICrudService<T> crud_service, final Class<T> clazz) {
		//get id or a query object that was posted
		if ( query_id.isPresent() ) {
    		//ID delete
    		try {
    			final String id = query_id.get();
        		_logger.error("id: " + id);
				return Response.ok(RestUtils.convertSingleObjectToJson(crud_service.deleteObjectById(id).get(), DELETE_SUCCESS_FIELD_NAME).toString()).build();
			} catch ( InterruptedException | ExecutionException e) {
				return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error converting input stream to string: {0}", e)).build();
			}
    	} else if (query_json.isPresent()) {
    		//Body delete
			try {
				final String json = query_json.get();
				_logger.error("query: " + json);
				final QueryComponent<T> query = RestUtils.convertStringToQueryComponent(json, clazz, Optional.empty());				
				return Response.ok(RestUtils.convertSingleObjectToJson(crud_service.deleteObjectBySpec(query).get(), DELETE_SUCCESS_FIELD_NAME).toString()).build();
			} catch (Exception e) {
				return Response.status(Status.BAD_REQUEST).entity(ErrorUtils.getLongForm("Error converting input stream to string: {0}", e)).build();
			}    		
    	} else {
    		//TODO actually we should probably just do something else when this is empty (like return first item, or search all and return limit 10, etc)
    		return Response.status(Status.BAD_REQUEST).entity("DELETE requires an id in the url or query in the body").build();
    	}
	}
}
