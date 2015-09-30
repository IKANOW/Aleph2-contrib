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
package com.ikanow.aleph2.analytics.storm.utils;

/** Extension of ErrorUtils specific to streaming errors
 * @author Alex
 */
public class ErrorUtils extends com.ikanow.aleph2.data_model.utils.ErrorUtils{

	public static final String TOPOLOGY_NULL_ERROR = "Topology (bucket {0}) was null";
	
	public static final String INVALID_CALL_FROM_WRAPPED_ANALYTICS_CONTEXT_ENRICH = "This enrichment context is actually a wrapped analytics context and method={0} is not available";
	
	final public static String NOT_YET_IMPLEMENTED = "Functionality is not yet implemented: {0}";
	
	final public static String INVALID_TOPOLOGY_CLASSES = "Only spouts and bolts are supported, invalid topology: {0}"; 
		
	final public static String NOT_SUPPORTED_IN_STREAMING = "Functionality does not apply to streaming operations - this is for batch operations";
	
	final public static String SERVICE_RESTRICTIONS = "Can't call get*ContextSignature with different 'services' parameter; can't call getUnderlyingArtefacts without having called get*ContextSignature.";
	
	final public static String TECHNOLOGY_NOT_MODULE = "Can only be called from technology, not module";
	
	final public static String MODULE_NOT_TECHNOLOGY = "Can only be called from module, not technology";
	
	final public static String USER_TOPOLOGY_NOT_SET = "This method {0} cannot be called until the framework has set the user entry point";
	
	// Temp issues which will get addressed
	
	final public static String TEMP_INPUTS_MUST_BE_STREAMING = "Currently (will be fixed soon), only streaming inputs are supported (bucket={0}, job={1}, input type={2})";
	
	final public static String TEMP_TRANSIENT_OUTPUTS_MUST_BE_STREAMING = "Currently (will be fixed soon), transient outputs must be streaming (bucket={0}, job={1}, output type={2})";
}
