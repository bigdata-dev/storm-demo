package com.ryxc.trident;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TridentTopologyFilter {

	public static class PrintBolt extends BaseFunction {

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			Integer value = tuple.getInteger(0);
			System.out.println("接收到数据:"+value);
		}
		  
    }
	
	public static class MyFilter extends BaseFilter {

		@Override
		public boolean isKeep(TridentTuple tuple) {
			Integer value = tuple.getInteger(0);
			Integer flag = value%2;
			return flag==0?true:false;
		}

		
		  
    }
	
	public static void main(String[] args) {
		TridentTopology tridentTopology = new TridentTopology();
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 1, new Values(1), new Values(2), new Values(3));
        //spout.setCycle(true);  //spout 可以循环发送
		tridentTopology.newStream("aaa", spout)
		.each(new Fields("sentence"),new MyFilter())
		.each(new Fields("sentence"),new PrintBolt(), new Fields("aa"));//第一个参数 spout的输出字段 第二个参数具体的处理单元 第三个数具体处理
		
	    LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("fun", new Config(), tridentTopology.build());
	}

}
