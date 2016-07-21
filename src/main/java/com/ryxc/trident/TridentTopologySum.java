package com.ryxc.trident;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TridentTopologySum {
	
	public static class MyDataSourceSpout implements IBatchSpout {

	    HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();
	    
	    @Override
	    public void open(Map conf, TopologyContext context) {
	    }

	    int num = 0;
	    @Override
	    public void emitBatch(long batchId, TridentCollector collector) {
	        List<List<Object>> batch = this.batches.get(batchId);
	        if(batch == null){
	            batch = new ArrayList<List<Object>>();
	            batch.add(new Values(num++));
	            this.batches.put(batchId, batch);
	        }
	        for(List<Object> list : batch){
	            collector.emit(list);
	        }
	    }

	    @Override
	    public void ack(long batchId) {
	        this.batches.remove(batchId);
	    }

	    @Override
	    public void close() {
	    }

	    @Override
	    public Map getComponentConfiguration() {
	        Config conf = new Config();
	        conf.setMaxTaskParallelism(1);
	        return conf;
	    }

	    @Override
	    public Fields getOutputFields() {
	        return new Fields("sentence");
	    }
	    
	}

	public static class SumBolt extends BaseFunction {

		int sum = 0;
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			Integer value = tuple.getInteger(0);
			sum += value;
			System.out.println("和:"+sum);
		}
		  
    }
	

	
	public static void main(String[] args) {
		TridentTopology tridentTopology = new TridentTopology();
		tridentTopology.newStream("aaa", new MyDataSourceSpout())
				.each(new Fields("sentence"), new SumBolt(),new Fields("aa"));
		
	    LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("fun", new Config(), tridentTopology.build());
	}

}
