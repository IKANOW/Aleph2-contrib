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
package com.ikanow.aleph2.v1.document_db.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.mapreduce.InputFormat;

import scala.Tuple2;
import scala.Tuple4;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BucketUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.v1.document_db.data_model.V1DocDbConfigBean;
import com.ikanow.aleph2.v1.document_db.hadoop.assets.Aleph2V1InputFormat;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import fj.data.Either;

/** Utilities to bridge the legacy V1 mongodb service and V2
 * @author Alex
 */
public class V1DocumentDbHadoopUtils {
	final private static ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty()); 
	
	final static Set<String> DESCRIBE_FILTER = ImmutableSet.<String>builder().addAll(
			Arrays.asList(
					"mongo.job.verbose",
					"mongo.job.background"
					)).build();
		
	/** 
	 * @param input_config - the input settings
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static IAnalyticsAccessContext<InputFormat> getInputFormat(
			final String user_id,
			final AnalyticThreadJobBean.AnalyticThreadJobInputBean job_input, final Optional<ISecurityService> maybe_security, final V1DocDbConfigBean config)
	{
		//TODO (ALEPH-20): need to perform security in here
		
		return new IAnalyticsAccessContext<InputFormat>() {
			private LinkedHashMap<String, Object> _mutable_output = null;
			
			@Override
			public String describe() {
				//(return the entire thing)
				return ErrorUtils.get("service_name={0} options={1}", 
						this.getAccessService().right().value().getSimpleName(),
						this.getAccessConfig().get().entrySet().stream().filter(kv -> !DESCRIBE_FILTER.contains(kv.getKey())).collect(Collectors.toMap(kv->kv.getKey(), kv->kv.getValue()))
						);				
			}
			
			/* (non-Javadoc)
			 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext#getAccessService()
			 */
			@Override
			public Either<InputFormat, Class<InputFormat>> getAccessService() {
				return Either.right((Class<InputFormat>)(Class<?>)Aleph2V1InputFormat.class);
			}

			/* (non-Javadoc)
			 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext#getAccessConfig()
			 */
			@Override
			public Optional<Map<String, Object>> getAccessConfig() {
				if (null != _mutable_output) {
					return Optional.of(_mutable_output);
				}				
				_mutable_output = new LinkedHashMap<>();
				
				// Parse various inputs:
				
				final List<String> communities = 
						Arrays.stream(job_input.resource_name_or_id().substring(BucketUtils.EXTERNAL_BUCKET_PREFIX.length()).split(","))
								.collect(Collectors.toList())
								; 
				
				// Validate communities:
				maybe_security.ifPresent(sec -> {
					communities.stream()
						.filter(cid -> !sec.isUserPermitted(Optional.of(user_id), Tuples._2T("community", cid), Optional.of(ISecurityService.ACTION_READ)))
						.findAny()
						.ifPresent(cid -> {
							throw new RuntimeException(ErrorUtils.get(V1DocumentDbErrorUtils.V1_DOCUMENT_USER_PERMISSIONS, user_id, cid));
						});
				});
				
				final String query = _mapper.convertValue(Optional.ofNullable(job_input.filter()).orElse(Collections.emptyMap()), 
															JsonNode.class).toString();
				
				final Tuple4<String, Tuple2<Integer, Integer>, BasicDBObject, DBObject> horrible_object = 
						LegacyV1HadoopUtils.parseQueryObject(query, communities);
				
				final String db_server = config.mongodb_connection();
				
				// Here's all the fields to fill in
				
				// 1) Generic MongoDB fields:
				//name of job shown in jobtracker --><name>mongo.job.name</name><value>title
				//run the job verbosely ? --><name>mongo.job.verbose</name><value>true
				//Run the job in the foreground and wait for response, or background it? --><name>mongo.job.background</name><value>false
				//If you are reading from mongo, the URI --><name>mongo.input.uri</name><value>mongodb://"+dbserver+"/"+input
				//The number of documents to limit to for read [OPTIONAL] --><name>mongo.input.limit</name><value>" + nLimit
				//The query, in JSON, to execute [OPTIONAL] --><name>mongo.input.query</name><value>" + StringEscapeUtils.escapeXml(query)
				//The fields, in JSON, to read [OPTIONAL] --><name>mongo.input.fields</name><value>"+( (fields==null) ? ("") : fields )
				//InputFormat Class --><name>mongo.job.input.format</name><value>com.ikanow.infinit.e.data_model.custom.InfiniteMongoInputFormat
				
				_mutable_output.put("mongo.job.name", 
						Optional.ofNullable(job_input.data_service()).orElse("unknown") + ":" +
						Optional.ofNullable(job_input.resource_name_or_id()).orElse("unknown")); // (i think this is ignored in fact)				
				_mutable_output.put("mongo.job.verbose", "true");				
				_mutable_output.put("mongo.job.background", "false");
				_mutable_output.put("mongo.input.uri", "mongodb://" + db_server + "/doc_metadata.metadata");
				_mutable_output.put("mongo.input.query", horrible_object._1());
				_mutable_output.put("mongo.input.fields", Optional.ofNullable(horrible_object._4()).map(o -> o.toString()).orElse(""));
				_mutable_output.put("mongo.input.limit", Optional.ofNullable(job_input.config()).map(cfg -> cfg.record_limit_request()).map(o -> o.toString()).orElse("0"));
				
				// 2) Basic Infinit.e/MongoDB fields:
				//Maximum number of splits [optional] --><name>max.splits</name><value>"+nSplits
				//Maximum number of docs per split [optional] --><name>max.docs.per.split</name><value>"+nDocsPerSplit
				_mutable_output.put("max.splits", horrible_object._2()._1().toString());
				_mutable_output.put("max.docs.per.split", horrible_object._2()._2().toString());
				
				
				// 3) Advanced Infinit.e/MongoDB fields:				
				//Infinit.e src tags filter [optional] --><name>infinit.e.source.tags.filter</name><value>"+srcTags.toString()
				_mutable_output.put("infinit.e.source.tags.filter", Optional.ofNullable(horrible_object._3()).map(o -> o.toString()).orElse("{}"));
				
				return Optional.of(Collections.unmodifiableMap(_mutable_output));
			}			
		};
	}
	
	// Here's a list of removed options to keep the above code simpler
	
	// A) REMOVED BECAUSE THEY ARE OPTIONAL AND WE DON'T WANT THEM
	// A.1) Generic MongoDB fields:
	// A JSON sort specification for read [OPTIONAL] --><name>mongo.input.sort</name><value>
	//The number of documents to skip in read [OPTIONAL] --><!-- Are we running limit() or skip() first? --><name>mongo.input.skip</name><value>0
	//Partitioner class [optional] --><name>mongo.job.partitioner</name><value>
	//Sort Comparator class [optional] --><name>mongo.job.sort_comparator</name><value>
	//Split Size [optional] --><name>mongo.input.split_size</name><value>32
	// A.2) Basic Infinit.e/MongoDB fields:
	//User Arguments [optional] --><name>infinit.e.userid</name><value>"+ StringEscapeUtils.escapeXml(userId.toString())
	//User Arguments [optional] --><name>arguments</name><value>"+ StringEscapeUtils.escapeXml(arguments)
	//Infinit.e quick admin check [optional] --><name>infinit.e.is.admin</name><value>"+isAdmin
	//Infinit.e userid [optional] --><name>infinit.e.userid</name><value>"+userId
	// A.3) Advanced Infinit.e/MongoDB fields:				
	//Infinit.e cache list [optional] --><name>infinit.e.cache.list</name><value>"+cacheList
	//"\n\t<property><!-- This jobs output collection for passing into the mapper along with input collection [optional] --><name>infinit.e.selfMerge</name><value>"+originalOutputCollection
	//Run on multiple collections [optional] --><name>infinit.e.otherCollections</name><value>"+otherCollections
	
	// B) REMOVED BECAUSE THERE ARE OUTPUT RELATED
	// B.1) Generic MongoDB fields:
	//If you are writing to mongo, the URI --><name>mongo.output.uri</name><value>mongodb://"+dbserver+"/"+output
	//OutputFormat Class --><name>mongo.job.output.format</name><value>com.ikanow.infinit.e.data_model.custom.InfiniteMongoOutputFormat
	//Output key class for the output format --><name>mongo.job.output.key</name><value>"+outputKey
	//Output value class for the output format --><name>mongo.job.output.value</name><value>"+outputValue
	//Output key class for the mapper [optional] --><name>mongo.job.mapper.output.key</name><value>"+mapperKeyClass
	//Output value class for the mapper [optional] --><name>mongo.job.mapper.output.value</name><value>"+mapperValueClass
	// B.2) Basic Infinit.e/MongoDB fields:
	//Infinit.e incremental mode [optional] --><name>update.incremental</name><value>"+tmpIncMode
	// B.3) Advanced Infinit.e/MongoDB fields:				
	
	// C) REMOVED BECAUSE WE CAN'T FILL THEM IN AND HOPE THEY AREN'T NEEDED!
	// C.1) Generic MongoDB fields:
	//Class for the mapper --><name>mongo.job.mapper</name><value>"+ mapper
	//Reducer class --><name>mongo.job.reducer</name><value>"+reducer
	//Class for the combiner [optional] --><name>mongo.job.combiner</name><value>"+combiner
	// C.2) Basic Infinit.e/MongoDB fields:
	// C.3) Advanced Infinit.e/MongoDB fields:				
	
	
}
