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
package com.ikanow.aleph2.analytics.storm.assets;

import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingTopology;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.distributed_services.utils.KafkaUtils;

import fj.data.Either;

/** Implementation of KafkaBolt that loads the Aleph2 context and works with Aleph2 data objects
 * @author Alex
 */
public class TransientStreamingOutputBolt extends BaseRichBolt {
	private static final long serialVersionUID = -1608897241789495451L;
	private static final Logger _logger = LogManager.getLogger();
	
	protected final DataBucketBean _bucket; 
	protected final AnalyticThreadJobBean _job;
	protected final String _context_signature;
	protected final String _user_topology_entry_point; 
	protected final String _topic_name;
	protected final String _broker_list;
	
	transient protected IAnalyticsContext _context;
	transient protected IEnrichmentStreamingTopology _user_topology;
	
	/** User constructor (pre serialization)
	 * @param bucket
	 * @param context_signature
	 * @param user_topology_entry_point
	 * @param topic_name
	 */
	public TransientStreamingOutputBolt(final DataBucketBean bucket, final AnalyticThreadJobBean job, final String context_signature, final String user_topology_entry_point, final String topic_name) {		
		_broker_list = KafkaUtils.getBrokers();

		_bucket = bucket;
		_job = job;
		_context_signature = context_signature;
		_user_topology_entry_point = user_topology_entry_point;
		_topic_name = topic_name;
	}
	/* (non-Javadoc)
	 * @see storm.kafka.bolt.KafkaBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(final @SuppressWarnings("rawtypes") Map arg0, final TopologyContext arg1, final OutputCollector arg2) {
		try {
			_context = ContextUtils.getAnalyticsContext(_context_signature);
			_user_topology = (IEnrichmentStreamingTopology) Class.forName(_user_topology_entry_point).newInstance();
		}
		catch (Exception e) { // nothing to be done here?
			_logger.error("Failed to get context", e);
		}
	}	
	
    /* (non-Javadoc)
     * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
     */
    @Override
    public void execute(Tuple input) {
    	_context.sendObjectToStreamingPipeline(Optional.of(_bucket), _job, 
    			Either.left(_user_topology.rebuildObject(input, OutputBolt::tupleToLinkedHashMap)), Optional.empty());
    }	
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}


