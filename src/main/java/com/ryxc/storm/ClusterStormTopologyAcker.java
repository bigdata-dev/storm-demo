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


public class ClusterStormTopologyAcker {
	
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
			this.collector.emit(new Values(num%2,num),num);
			System.out.println("--spout:"+num);
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("flag","num"));
			
		}

		@Override
		public void ack(Object msgId) {
			System.out.println("ack方法被调用："+msgId);
		}

		@Override
		public void fail(Object msgId) {
			System.out.println("fail方法被调用："+msgId);
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
			try {
				Integer flag = input.getIntegerByField("flag");
				Integer num = input.getIntegerByField("num");
				//sum += num;
				System.out.println("线程ID:"+Thread.currentThread().getId()+", 接收到 num="+num +",flag="+flag);
				if(num%2==0){
					num = num/0;
				}
				this.collector.ack(input);
				System.out.println("触发 ack");
			} catch (Exception e) {
				//e.printStackTrace();
				this.collector.fail(input);
				System.out.println("触发 fail");
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
		}
		
		
	}
	
	public static void main(String [] args){
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("aa", new DataSourceSpout());
		topologyBuilder.setBolt("bb", new NumBlot(),1).allGrouping("aa");
		
		/*LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("topology_name", new Config(), topologyBuilder.createTopology());
		*/
		
	   try {
			
			Config config = new Config();
			//消息确认线程
			config.setNumAckers(1);
			
			StormSubmitter.submitTopology(ClusterStormTopologyAcker.class.getSimpleName(), config, topologyBuilder.createTopology());
		} catch (AlreadyAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
