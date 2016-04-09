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

package com.ikanow.aleph2.graph.titan.utils;

/** Titan Graph DB specific 
 * @author Alex
 *
 */
public class ErrorUtils extends com.ikanow.aleph2.data_model.utils.ErrorUtils {

	public static String DECOMPOSITION_ENRICHMENT_NEEDED = "Currently need to specify a custom graph decomposition enrichment module ({0})";
	public static String MERGE_ENRICHMENT_NEEDED = "Currently need to specify a custom graph decomposition merge module ({0})";
	
	public static String BUFFERS_NOT_SUPPORTED = "Secondary buffers not supported ({0})";
}
