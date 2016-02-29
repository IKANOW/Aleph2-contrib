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
package com.mongodb;

import java.lang.reflect.Method;

import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;

/** Defensive extension to DBCollection, doesn't error out whenever a node goes down or master/slave assignments chage 
 * (but can block for a while)
 *  THIS CLASS HAS NO COVERAGE SO NEED TO HANDLE TEST ON MODIFICATION
 * @author acp
 */
public class DBCollectionProxyFactory {

	/** Get the enhanced DB collection from the provided one 
	 * @param dbc - the collection to enhance
	 * @return - the enhanced collection
	 */
	public static DBCollection get(final DBCollection dbc) {
		return get(dbc, false);
	}	
	
	/** Get the enhanced DB collection from the provided one 
	 * @param dbc - the collection to enhance
	 * @return - the enhanced collection
	 */
	public static DBCollection get(final DBCollection dbc, final boolean is_mock) {
		return get(dbc.getDB(), dbc.getName(), is_mock);
	}
	/** Get the enhanced DB collection from the provided one 
	 * @param db db name
	 * @param name collection name
	 * @return the enhanced collection
	 */
	@SuppressWarnings("deprecation")
	public static DBCollection get(final DB db, final String name, final boolean is_mock) {
		
		Enhancer collectionEnhancer = new Enhancer();
		collectionEnhancer.setSuperclass(is_mock
				? com.mongodb.FongoDBCollection.class
				: com.mongodb.DBCollectionImpl.class
				);
		MethodInterceptor collectionMi = new MethodInterceptor()
		{
			boolean _top_level = true;
			
			@Override
			public Object intercept(final Object object, final Method method,
					final Object[] args, final MethodProxy methodProxy )
					throws Throwable
			{
				if (_top_level) {
					try {
						_top_level = false;
						for (int count = 0; ; count++) {
							//DEBUG
							//System.out.println("intercepted method: " + method.toString() + ", loop=" + count + ");
							
							try {
								Object o = methodProxy.invokeSuper(object, args);
								
								if (o instanceof DBCursor) {
									o =  getCursor((DBCursor) o);
								}							
								return o;
							}
							catch (com.mongodb.MongoException e) {
								if (count < 60) {
									continue;
								}
								throw e;
							}
						}
					}
					finally {
						_top_level = true;
					}
				}
				else {
					return methodProxy.invokeSuper(object, args);
				}
			}
			
		};
		collectionEnhancer.setCallback(collectionMi);
		return (DBCollection) collectionEnhancer.create(
				is_mock
				? new Class[]{com.mongodb.FongoDB.class, String.class}
				: new Class[]{com.mongodb.DBApiLayer.class, String.class}
				, 
				new Object[]{db, name});
	}

	///////////////////////////////////////////////////////////////////////////
	
	//DO THE SAME FOR DBCURSOR

	protected static DBCursor getCursor(final DBCursor from) {
		Enhancer dbcursorEnhancer = new Enhancer();
		dbcursorEnhancer.setSuperclass(com.mongodb.DBCursor.class);
		
		MethodInterceptor collectionMi = new MethodInterceptor() {
			boolean _top_level = true;
			
			@Override
			public Object intercept(Object object, Method method,
					Object[] args, MethodProxy methodProxy )
					throws Throwable
			{
				if (_top_level) {
					try {
						_top_level = false;
						for (int count = 0; ; count++) {
							//DEBUG
							//System.out.println("intercepted method: " + method.toString() + ", loop=" + count + ");
							
							try {
								Object o = methodProxy.invokeSuper(object, args);								
								return o;
							}
							catch (com.mongodb.MongoException e) {
								if (count < 60) {
									continue;
								}
								throw e;
							}
						}
					}
					finally {
						_top_level = true;
					}
				}
				else {
					return methodProxy.invokeSuper(object, args);
				}
			}
		};
		dbcursorEnhancer.setCallback(collectionMi);
		return (DBCursor) dbcursorEnhancer.create(
				new Class[]{DBCollection.class, DBObject.class, DBObject.class, ReadPreference.class}, 
				new Object[]{from.getCollection(), from.getQuery(), from.getKeysWanted(), from.getReadPreference()});
	}
}
