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
package com.ikanow.aleph2.analytics.hadoop.assets;

import java.util.Optional;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;

/** Default Batch enrichment module.
 * @author jfreydank
 *
 */
public class BePassthroughModule implements IEnrichmentBatchModule {
	private static final Logger logger = LogManager.getLogger(BePassthroughModule.class);

	protected IEnrichmentModuleContext _context;
	protected DataBucketBean _bucket;
	protected boolean _finalStage;
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageInitialize(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, boolean)
	 */
	@Override
	public void onStageInitialize(IEnrichmentModuleContext context, DataBucketBean bucket, EnrichmentControlMetadataBean control, boolean final_stage) {
		logger.debug("BatchEnrichmentModule.onStageInitialize:"+ context+", DataBucketBean:"+ bucket+", final_stage"+final_stage);
		this._context = context;
		this._bucket = bucket;
		this._finalStage = final_stage;

	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onObjectBatch(java.util.stream.Stream, java.util.Optional, java.util.Optional)
	 */
	@Override
	public void onObjectBatch(final Stream<Tuple2<Long, IBatchRecord>> batch, Optional<Integer> batch_size, Optional<JsonNode> grouping_key) {
		if (logger.isDebugEnabled()) logger.debug("BatchEnrichmentModule.onObjectBatch:" + batch_size);
		batch.forEach(t2 -> {

			// not sure what to do with streaming (probably binary) data - probably will have to just ignore it in default mode?
			// (the alternative is to build Tika directly in? or maybe dump it directly in .. not sure how Jackson manages raw data?)
			Optional<ObjectNode> streamBytes = Optional.empty();						
			_context.emitImmutableObject(t2._1(), t2._2().getJson(), streamBytes, Optional.empty());

		}); // for 
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageComplete(boolean)
	 */
	@Override
	public void onStageComplete(boolean is_original) {
		logger.debug("BatchEnrichmentModule.onStageComplete()");
	}

}
