package com.ryxc.storm;

import java.util.Map;

import clojure.main;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class ClusterStormTopologyShufferGrouping {
	
	public static class DataSourceSpout extends BaseRichSpout{

		private Map conf;
		private TopologyContext context;
		private SpoutOutputCollector collector;
		
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			this.conf = conf;
			this.context = context;
			this.collector = collector;
		}
		int num = 0;
		public void nextTuple() {
			num ++;
			this.collector.emit(new Values(num%2,num));
			System.out.println("--spout:"+num);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("flag","num"));
			
		}
		
	}
	
	static class NumBlot extends BaseRichBolt{
		private Map stormConf;
		private TopologyContext context;
		private OutputCollector collector;
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}
		
		public void execute(Tuple input) {
			Integer num = input.getIntegerByField("num");
			//sum += num;
			System.out.println("线程ID:"+Thread.currentThread().getId()+", 接收到-blot:"+num);
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
		}
		
		
	}
	
	public static void main(String [] args){
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("aa", new DataSourceSpout());
		topologyBuilder.setBolt("bb", new NumBlot(),3).fieldsGrouping("aa", new Fields());
		
		/*LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("topology_name", new Config(), topologyBuilder.createTopology());*/
		
		
		try {
			
			Config config = new Config();
			
			config.setNumAckers(0);
			
			StormSubmitter.submitTopology(ClusterStormTopologyShufferGrouping.class.getSimpleName(), config, topologyBuilder.createTopology());
		} catch (AlreadyAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
