package com.scratchy.storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PrintOutBolt extends BaseRichBolt {
  private final static Logger printer = LoggerFactory.getLogger(PrintOutBolt.class);

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

  }

  @Override
  public void execute(Tuple tuple) {
    printer.info("[" + tuple.getLong(0) + "] " + tuple.getString(1));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

  }
}
