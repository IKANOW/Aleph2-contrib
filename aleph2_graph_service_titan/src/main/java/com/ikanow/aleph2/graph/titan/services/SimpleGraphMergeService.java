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

package com.ikanow.aleph2.graph.titan.services;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.GraphAnnotationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.SetOnce;

import fj.data.Validation;

/** Very simple code that "merges" matching vertices and edges (just picks the first one it sees)
 * @author Alex
 */
public class SimpleGraphMergeService implements IEnrichmentBatchModule {

	protected SetOnce<IEnrichmentModuleContext> _context = new SetOnce<>(); 
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageInitialize(com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean, scala.Tuple2, java.util.Optional)
	 */
	@Override
	public void onStageInitialize(IEnrichmentModuleContext context,
			DataBucketBean bucket, EnrichmentControlMetadataBean control,
			Tuple2<ProcessingStage, ProcessingStage> previous_next,
			Optional<List<String>> next_grouping_fields) {
		_context.set(context);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onObjectBatch(java.util.stream.Stream, java.util.Optional, java.util.Optional)
	 */
	@Override
	public void onObjectBatch(Stream<Tuple2<Long, IBatchRecord>> batch,
			Optional<Integer> batch_size, Optional<JsonNode> grouping_key) {

		// (some horrible mutable state to keep this simple)
		final LinkedList<Tuple2<Long, IBatchRecord>> mutable_user_elements = new LinkedList<>();
		
		final Optional<Validation<BasicMessageBean, JsonNode>> output = batch
			.filter(t2 -> { 
				if (!t2._2().injected() && mutable_user_elements.isEmpty()) { // (save one element per label in case there are no injected elements)
					mutable_user_elements.add(t2);
				}
				return t2._2().injected();
			}) 
			.filter(t2 -> {
				final ObjectNode o = (ObjectNode) t2._2().getJson();
				return Optional.ofNullable(o.get(GraphAnnotationBean.type)).filter(j -> (null != j) && j.isTextual()).map(j -> j.asText()).map(type -> {
					if (GraphAnnotationBean.ElementType.edge.toString().equals(type)) {
						// Return the first matching edge:
						return true;
					}
					else if (GraphAnnotationBean.ElementType.vertex.toString().equals(type)) {
						// Return the first matching vertex:
						return true;
					}
					else return false;
				})
				.orElse(false);
			})
			.findFirst()
			.<Validation<BasicMessageBean, JsonNode>>map(element -> 
					_context.get().emitImmutableObject(_context.get().getNextUnusedId(), element._2().getJson(), Optional.empty(), Optional.empty(), Optional.empty())
				);
		
		if (!output.isPresent()) { // If didn't find any matching elements, then stick the first one we did find in there
			mutable_user_elements.forEach(t2 -> 
				_context.get().emitImmutableObject(t2._1(), t2._2().getJson(), Optional.empty(), Optional.empty(), Optional.empty())
			);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule#onStageComplete(boolean)
	 */
	@Override
	public void onStageComplete(boolean is_original) {
		// (Currently nothing to do)
	}

}
