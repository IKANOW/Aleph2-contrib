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
package com.ikanow.aleph2.search_service.elasticsearch.utils;

import com.ikanow.aleph2.data_model.utils.ErrorUtils;

/** Search index (elasticsearch) specific error strings
 * @author Alex
 */
public class SearchIndexErrorUtils extends ErrorUtils {

	public static final String INVALID_MAX_INDEX_SIZE = "The max index size must be more than 25MB if set: attempted value={0} MB";
	public static final String NON_ADMIN_BUCKET_NAME_OVERRIDE = "Only non-admins can override the default index name";
	public static final String NOT_YET_SUPPORTED = "Bucket {0}: This feature is not yet supported: {1}";
}
