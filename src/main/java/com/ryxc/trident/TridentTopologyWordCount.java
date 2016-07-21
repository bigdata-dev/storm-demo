package com.ryxc.trident;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.spout.IBatchSpout;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TridentTopologyWordCount {
	
	public static class MyDataSourceSpout implements IBatchSpout {

	    HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();
	    
	    @Override
	    public void open(Map conf, TopologyContext context) {
	    }

	    @Override
	    public void emitBatch(long batchId, TridentCollector collector) {
	        try {
				List<List<Object>> batch = this.batches.get(batchId);
				if(batch == null){
				    batch = new ArrayList<List<Object>>();
				    Collection<File> listFiles = FileUtils.listFiles(new File("d:\\test"), new String[]{"txt"}, true);	
				    for(File file:listFiles){
				    	List<String> readLines = FileUtils.readLines(file);
				    	for(String line:readLines){
				    		batch.add(new Values(line));
				    	}
				    	FileUtils.moveFile(file, new File(file.getAbsolutePath()+System.currentTimeMillis()));
				    }
				    if(batch.size()>0){
				    	this.batches.put(batchId, batch);
				    }
				}
				for(List<Object> list : batch){
				    collector.emit(list);
				}
			} catch (IOException e) {
				e.printStackTrace();
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
	        return new Fields("line");
	    }
	    
	}

	public static class SplitBolt extends BaseFunction {

		int sum = 0;
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String line = tuple.getStringByField("line");
			String[] split = line.split("\t");
			for (String word : split) {
				collector.emit(new Values(word));
			}
		}
		
		
		
		  
    }
	
	public static class WordCountBolt extends BaseAggregator<Map<String, Integer>>{

		@Override
		public Map<String, Integer> init(Object batchId,
				TridentCollector collector) {
			return new HashMap<String, Integer>();
		}

		@Override
		public void aggregate(Map<String, Integer> val, TridentTuple tuple,
				TridentCollector collector) {
			String key =  tuple.getStringByField("word");
			val.put(key, MapUtils.getInteger(val, key,0)+1);
		}

		@Override
		public void complete(Map<String, Integer> val,
				TridentCollector collector) {
			collector.emit(new Values(val));
		}
		
	}
	
	public static class PrintBolt extends BaseFunction{
		HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
		
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			Map<String, Integer> map = (Map<String, Integer>)tuple.get(0);
			for (Entry<String, Integer> entry : map.entrySet()) {
				String key = entry.getKey();
				Integer value = entry.getValue();
				Integer count = hashMap.get(key);
				if(count==null){
					count = 0;
				}
				hashMap.put(key, count+value);
			}
			Utils.sleep(1000);
			System.out.println("=======================================");
			for (Entry<String, Integer> entry : hashMap.entrySet()) {
				System.out.println(entry);
			}
			
		}
		
	}
	
	

	
	public static void main(String[] args) {
		TridentTopology tridentTopology = new TridentTopology();
		tridentTopology.newStream("aaa", new MyDataSourceSpout())
				.each(new Fields("line"), new SplitBolt(),new Fields("word"))
				.groupBy(new Fields("word"))
				.aggregate(new Fields("word"), new WordCountBolt(),new Fields("aaa"))
				.each(new Fields("aaa"), new PrintBolt(), new Fields("bbb"));
				
		
	    LocalCluster cluster = new LocalCluster();
	    cluster.submitTopology("fun", new Config(), tridentTopology.build());
	}

}
