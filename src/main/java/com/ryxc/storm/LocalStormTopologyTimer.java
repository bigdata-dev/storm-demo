package com.ryxc.storm;

import java.util.Map;

import clojure.main;
import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.LocalCluster;
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


public class LocalStormTopologyTimer {
	
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
			this.collector.emit(new Values(num));
			System.out.println("--spout:"+num);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("num"));
			
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
		
		int sum = 0;
		public void execute(Tuple input) {
			
			if(input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)){
				System.out.println("线程ID:"+Thread.currentThread().getId()+" 系统ID:"+input.getSourceComponent()+"---------------------- 执行定时任务");
			}else{
				Integer num = input.getIntegerByField("num");
				sum += num;
				System.out.println("## blot:"+sum);
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
		}
		
		
	}
	
	public static void main(String [] args){
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("aa", new DataSourceSpout());
		topologyBuilder.setBolt("bb", new NumBlot(),3).shuffleGrouping("aa");
		
		LocalCluster localCluster = new LocalCluster();
		Config config = new Config();
		config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10); //设置一个定时器，默认30秒
		localCluster.submitTopology("topology_name", config, topologyBuilder.createTopology());
	}

}
