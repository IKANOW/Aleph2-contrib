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

package com.ikanow.aleph2.analytics.hadoop.utils;

/** Collection of constants used in various Hadoop batch enrichment related places
 * @author Alex
 *
 */
public class HadoopBatchEnrichmentUtils {

	// Batch enrichment constants
	
	public static final String BATCH_SIZE_PARAM = "aleph2.batch.batchSize";
	public static final String BE_CONTEXT_SIGNATURE = "aleph2.batch.beContextSignature"; //(one of context signature or bucket signature must be filled in)
	public static final String BE_BUCKET_SIGNATURE = "aleph2.batch.beBucketSignature";  //(one of context signature or bucket signature must be filled in)
	public static final String BE_BUCKET_INPUT_CONFIG = "aleph2.batch.inputConfig";  //(one of context signature or bucket signature must be filled in)
	public static final String BE_DEBUG_MAX_SIZE = "aleph2.batch.debugMaxSize";

}
