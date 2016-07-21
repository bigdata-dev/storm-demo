package com.ryxc.storm;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import backtype.storm.Config;
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

import com.ryxc.storm.LocalStormTopology.NumBlot;



public class LocalWordCountStormTopology {
	
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

		public void nextTuple() {
			Collection<File> listFiles = FileUtils.listFiles(new File("d://test"),new String[]{"txt"},true);
			for(File file:listFiles){
				try {
					List<String> readLines = FileUtils.readLines(file);
					for(String line:readLines){
						this.collector.emit(new Values(line));
					}
					FileUtils.moveFile(file, new File(file.getAbsolutePath()+System.currentTimeMillis()));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("line"));
		}
		
	}
	
	static class SplitBlot extends BaseRichBolt{
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
			String line = input.getStringByField("line");
			String[] split = line.split("\t");
			for (String word : split) {
				this.collector.emit(new Values(word));
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}
	
	
	static class CountBlot extends BaseRichBolt{
		private Map stormConf;
		private TopologyContext context;
		private OutputCollector collector;
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}
		
		Map<String,Integer> map = new HashMap<String,Integer>();
		public void execute(Tuple input) {
			String word = input.getStringByField("word");
			if(!map.containsKey(word)){
				map.put(word, 1);
			}else{
				Integer count = map.get(word);
				map.put(word, count+1);
			}
			System.out.println("---map:"+map);
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		}
		
		
	}
	
	public static void main(String [] args){
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("aa", new DataSourceSpout());
		topologyBuilder.setBolt("bb", new SplitBlot()).shuffleGrouping("aa");
		topologyBuilder.setBolt("cc", new CountBlot()).shuffleGrouping("bb");
		
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("topology_name", new Config(), topologyBuilder.createTopology());
	}

}
