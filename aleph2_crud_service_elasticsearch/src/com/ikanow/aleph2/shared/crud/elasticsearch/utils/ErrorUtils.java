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

/** Elasticsearch CRUD service errors
 * @author acp
 */
public class ErrorUtils extends com.ikanow.aleph2.data_model.utils.ErrorUtils {

	public static final String EXISTS_ON_IDS = "It is not possible to make 'exists' queries on _ids, since each data object has excactly one _id value";
	public static final String ALL_OF_ON_IDS = "It is not possible to make 'all_of' queries on _ids, since each data object can only have one _id value";
	public static final String NO_ID_RANGES_UNLESS_IDS_INDEXED = "In order to perform range _ids on queries, it is necessary to have indexed the _id explicitly";
	
	public static final String NOT_YET_IMPLEMENTED = "This feature ({0}) has not yet been implemented, but is on the roadmap";
	public static final String TRIED_TO_WRITE_INTO_RO_SERVICE = "This CRUD service was created as read-only but the method {0} attemped to write";
	
	public static final String STORE_OBJECTS_ALWAYS_COMPLETES = "Elasticsearch does not support continue_on_error:false in storeObjects";
}
