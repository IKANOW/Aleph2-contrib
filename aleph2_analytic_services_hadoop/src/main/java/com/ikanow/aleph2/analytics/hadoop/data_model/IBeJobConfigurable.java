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

import java.util.List;

import com.ikanow.aleph2.analytics.hadoop.services.BatchEnrichmentContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;

/** Interface for a batch enrichment job
 * @author Alex
 */
public interface IBeJobConfigurable {

	/** Configures the bucket
	 * @param dataBucketBean
	 */
	public void setDataBucket(DataBucketBean dataBucketBean);


	/** Configures the enrichment context (note uses the concrete version not the generic one, need some implementation details)
	 * @param enrichmentContext
	 */
	public void setEnrichmentContext(BatchEnrichmentContext enrichmentContext);


	/** The set of modules + their control parameters
	 * @param ecMetadata
	 */
	public void setEcMetadata(List<EnrichmentControlMetadataBean> ecMetadata);


	/** Override the default batch size
	 * @param size
	 */
	public void setBatchSize(int size);
}
