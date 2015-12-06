package com.scratchy.storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.scratchy.text.EmoticonExtractor;

import java.util.Map;

public class FrequencyCounterBolt extends BaseRichBolt {

  private OutputCollector collector;

  private final Map<Long, EmoticonExtractor> textParsers;

  public FrequencyCounterBolt(Map<Long, EmoticonExtractor> textParsers) {
    this.textParsers = textParsers;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    long id = input.getLong(0);
    String message = input.getString(1);
    textParsers
            .get(id)
            .frequency(message)
            .forEach(
                    (icon, freq) -> collector.emit(new Values(icon, freq))
            );
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("obj", "count"));
  }
}
