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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

/** This class contains data objects for one batch entrichment bucket enhancement job. 
*/

public class BeJobBean {
	private String bucketPathStr;
	private String bucketInputPath = null;
	private Map<String, String> sharedLibraries = null;
	private DataBucketBean dataBucketBean = null;
	private String enrichmentControlMetadataName;
	
	/** Jackson c'tor
	 */
	public BeJobBean(){
		
	}

	/** User c'tor 
	 * @param dataBucketBean
	 * @param enrichmentControlMetadataName
	 * @param sharedLibraries
	 * @param bucketPathStr
	 * @param bucketInputPath
	 * @param bucketOutPath
	 */
	public BeJobBean(DataBucketBean dataBucketBean, String enrichmentControlMetadataName, Map<String, String> sharedLibraries, String bucketPathStr, String bucketInputPath){
		this.dataBucketBean = dataBucketBean;
		this.enrichmentControlMetadataName = enrichmentControlMetadataName;
		this.sharedLibraries =  sharedLibraries;
		this.bucketPathStr = bucketPathStr;
		this.bucketInputPath = bucketInputPath;
	}
	
	
	/** The bucket being 
	 * @return
	 */
	public DataBucketBean getDataBucketBean() {
		return dataBucketBean;
	}
	public Map<String, String> getSharedLibraries() {
		return sharedLibraries;
	}

	public String getBucketPathStr() {
		return bucketPathStr;
	}

	
	public String getEnrichmentControlMetadataName() {
		return enrichmentControlMetadataName;
	}

	public String getBucketInputPath() {
		return bucketInputPath;
	}

	public static EnrichmentControlMetadataBean extractEnrichmentControlMetadata(DataBucketBean dataBucketBean,String enrichmentControlMetadataName){
		final Optional<EnrichmentControlMetadataBean> oecm = Optional.ofNullable(dataBucketBean.batch_enrichment_configs())
																.orElse(Collections.emptyList())
																.stream().filter(ec -> ec.name().equals(enrichmentControlMetadataName))
																.findFirst();
		return oecm.orElse(BeanTemplateUtils.build(EnrichmentControlMetadataBean.class).done().get());		
	}
}
