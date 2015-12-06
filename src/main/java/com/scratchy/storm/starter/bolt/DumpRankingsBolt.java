package com.scratchy.storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.scratchy.db.Data;
import com.scratchy.storm.starter.tools.Rankable;
import com.scratchy.storm.starter.tools.Rankings;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class DumpRankingsBolt extends BaseRichBolt {

  private static final int N = 10;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    try (Jedis jedis = Data.jedis()) {
      jedis.del(Data.topIconsKey());
      jedis.del(Data.topValuesKey());
      for (int i = 1; i <= N; i++) {
        jedis.rpush(Data.topIconsKey(), String.valueOf(i));
        jedis.rpush(Data.topValuesKey(), String.valueOf(i));
      }
    }
  }

  private static String entry(Rankable rank) {
    return rank.getCount() + ":" + rank.getObject();
  }

  @Override
  public void execute(Tuple tuple) {
    try (Jedis jedis = Data.jedis()) {
      Rankings rankings = (Rankings) tuple.getValue(0);
      for(int i = 0; i < rankings.size(); i++) {
        Rankable entry = rankings.get(i);
        jedis.lset(Data.topIconsKey(), i, (String) entry.getObject());
        jedis.lset(Data.topValuesKey(), i, String.valueOf(entry.getCount()));
      }
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}
