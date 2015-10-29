/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.analytics.hadoop.data_model;

import java.io.Serializable;
import java.util.Optional;

/** Config bean for available technology overrides (in the enrichment metadata control bean)
 * @author Alex
 */
public class HadoopTechnologyOverrideBean implements Serializable {
	private static final long serialVersionUID = -6990533978104476468L;

	/** Jackson c'tor
	 */
	protected HadoopTechnologyOverrideBean() {}
	
	/** The desired number of reducers to use in grouping operations (no default since might want to calculate based on cluster settings)
	 * @return
	 */
	public Integer num_reducers() { return num_reducers; }

	/** For jobs with mapping operations, determines whether to use the combiner or not (optional, defaults to true)
	 * @return
	 */
	public  Boolean use_combiner() { return Optional.ofNullable(use_combiner).orElse(true); }
	
	/** Requests an optimal batch size for this job - NOTE not guaranteed to be granted
	 * @return
	 */
	public Integer requested_batch_size() { return requested_batch_size; }		
	
	private Integer num_reducers;
	private Boolean use_combiner;
	private Integer requested_batch_size;
}
