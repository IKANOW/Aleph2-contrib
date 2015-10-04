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
package com.ikanow.aleph2.analytics.hadoop.utils;

/** Hadoop related errors
 * @author Alex
 */
public class HadoopErrorUtils {

	public static final String NO_MAPPERS = "Found no mappers bucket={0} job={1} config={2}";
	public static final String MISSING_REQUIRED_FIELD = "The field {0} in bucket {1} is missing but required";
	
	final public static String BATCH_TOPOLOGIES_NOT_YET_SUPPORTED = "Batch topologies not yet supported";
	final public static String TECHNOLOGY_NOT_MODULE = "Can only be called from technology, not module";
	final public static String SERVICE_RESTRICTIONS = "Can't call getAnalyticsContextSignature with different 'services' parameter; can't call getUnderlyingArtefacts without having called getEnrichmentContextSignature.";
	
	final public static String JOB_STOP_ERROR = "Failed to stop job {0} bucket {1}: could not found or other error";
	
	// General errors:
	
	public final static String EXCEPTION_CAUGHT = "Caught Exception: {0}";
	final public static String VALIDATION_ERROR = "Validation Error: {0}";
	final public static String NOT_YET_IMPLEMENTED = "This operation is not currently supported: {0}";
}
