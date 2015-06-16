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

/** MongoDB CRUD service errors
 * @author acp
 */
public class ErrorUtils extends com.ikanow.aleph2.data_model.utils.ErrorUtils {

	public static final String INTERNAL_LOGIC_ERROR_NO_ID_FIELD = "Internal logic error: get String _id field for {0}";
	public static final String MISSING_MONGODB_INDEX_KEY = "Index key {0} didn't exist";
	public static final String BULK_REPLACE_DUPLICATES_NOT_SUPPORTED = "Bulk storeObjects with update not yet supported";
}
