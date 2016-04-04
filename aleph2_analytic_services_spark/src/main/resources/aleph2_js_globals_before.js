// Globals:
// _a2_global_context - com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext
// _a2_global_config - com.fasterxml.jackson.databind.node.ObjectNode
// _a2_global_mapper - com.fasterxml.jackson.databind.ObjectMapper
// _a2_global_bucket - com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean
// _a2_global_job - com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean
// _a2_bucket_logger - com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger
// _a2_enrichment_name - String
// _a2_spark_inputs - Multimap<String, JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>>
// _a2_spark_inputs_all - JavaPairRDD<Object, Tuple2<Long, IBatchRecord>>
// _a2_spark_context - org.apache.spark.api.java.JavaSparkContext

// Global methods
function _a2_global_js_to_json(json) {
	if (json instanceof com.fasterxml.jackson.databind.node.ObjectNode) {
		return json;
	}
	else if (json instanceof java.lang.String) {
		return _a2_global_mapper.readTree(json);		
	}
	else if (json instanceof String) {
		return _a2_global_mapper.readTree(String(json));				
	}
	else { //js object 
		var json_str = JSON.stringify(json);
		return _a2_global_mapper.readTree(String(json_str));						
	}	
	
}

// Callbacks:
function _a2_global_emit(json) { // output to next stage in pipeline			
	return _a2_global_context.emitObject(Optional.empty(), _a2_global_context.context.getJob().get(), Packages.fj.data.Either.left(t2._2().getJson()), Optional.empty());
}
function _a2_global_emit_external(bucket_path, json) { // emit to the input of an external bucket (or the current bucket's output, though that's not really intended)
	var bucket = com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.build(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.class)
						.with(String("full_name"), String(bucket_path))
					.done().get();
	return _a2_global_context.emitObject(Optional.of(bucket), _a2_global_context.context.getJob().get(), Packages.fj.data.Either.left(t2._2().getJson()), Optional.empty());
}

function _a2_global_to_json(jsonnode) {
	return JSON.parse(jsonnode.toString());
}

function _a2_global_list_to_js(jlist) {
	return Java.from(jlist);
}

//TODO (make this more sophisticated)
function _a2_bucket_log(level, msg) {
	var success = (level != org.apache.logging.log4j.Level.ERROR) && (level != org.apache.logging.log4j.Level.WARN);
	_a2_bucket_logger.inefficientLog(level,
			com.ikanow.aleph2.data_model.utils.ErrorUtils.buildMessage(success, "SparkJsInterpreterTopology." + _a2_enrichment_name, _a2_enrichment_name + ".main", msg)
			);
}

function Aleph2Api() {
	this.context = _a2_global_context;
	this.spark_context = _a2_spark_context;
	this.inputs = _a2_spark_inputs;
	this.all_inputs = _a2_spark_inputs_all;
	this.config = _a2_global_to_json(_a2_global_config);
	this.bucket = _a2_global_bucket;
	this.job = _a2_global_job;
	this.emit = _a2_global_emit;
	this.externalEmit = _a2_global_emit_external;
	this.to_json = _a2_global_to_json;
	this.list_to_js = _a2_global_list_to_js;
	this.logger = _a2_bucket_logger;
	this.log_trace = function(msg) { _a2_bucket_log(org.apache.logging.log4j.Level.TRACE, msg); }
	this.log_debug = function(msg) { _a2_bucket_log(org.apache.logging.log4j.Level.DEBUG, msg); }
	this.log_info = function(msg) { _a2_bucket_log(org.apache.logging.log4j.Level.INFO, msg); }
	this.log_warn = function(msg) { _a2_bucket_log(org.apache.logging.log4j.Level.WARN, msg); }
	this.log_error = function(msg) { _a2_bucket_log(org.apache.logging.log4j.Level.ERROR, msg); }
}
var _a2 = new Aleph2Api();

