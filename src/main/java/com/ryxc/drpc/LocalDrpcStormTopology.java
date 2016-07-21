package com.ryxc.drpc;

import java.util.Map;

import clojure.main;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
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


public class LocalDrpcStormTopology {
	
	static class MyBlot extends BaseRichBolt{
		private Map stormConf;
		private TopologyContext context;
		private OutputCollector collector;
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}
		
		//tuple 封装两个参数 第一个是请求ID 第二个是请求参数
		public void execute(Tuple input) {
			String value = input.getString(1);
			value = "hello "+value;
			this.collector.emit(new Values(input.getValue(0),value));
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id","param"));
		}
		
		
	}
	
	public static void main(String [] args){
		
		//对外提供服务的名称
		LinearDRPCTopologyBuilder linearDRPCTopologyBuilder = new LinearDRPCTopologyBuilder("addHello");
		linearDRPCTopologyBuilder.addBolt(new MyBlot());

		LocalCluster localCluster = new LocalCluster();
		//相当于在本地创建drpc服务
		LocalDRPC drpcServer = new LocalDRPC();
		localCluster.submitTopology("topology_name", new Config(), linearDRPCTopologyBuilder.createLocalTopology(drpcServer));
		
		//创建一个客户端
		String result = drpcServer.execute("addHello", "word");
		System.out.println("客户端调用结果："+result);
		
	}

}
