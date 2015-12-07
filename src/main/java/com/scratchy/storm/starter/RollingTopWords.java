/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.scratchy.storm.starter;

import backtype.storm.Config;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import com.scratchy.storm.starter.bolt.*;
import com.scratchy.db.Data;
import com.scratchy.storm.starter.spout.IrcSpout;
import com.scratchy.storm.starter.util.Bootstrap;
import com.scratchy.storm.starter.util.StormRunner;
import com.scratchy.text.EmoticonExtractor;
import com.typesafe.config.ConfigFactory;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RollingTopWords {
  private static final Logger log = Logger.getLogger(RollingTopWords.class);

  private static class K {
    public static final String top = "top-n";
    public static final String runningFor = "running-for";
    public static final String windowDuration = "window.duration";
    public static final String windowPrecision = "window.precision";
  }

  private static class Parallelism {
    public static final int freqCounterBolt = 10;
    public static final int rollingCounterBolt = 4;
    public static final int intermediateRankerBolt = 4;
  }

  private static class Ids {
    public static final String freqCounterBolt = "frequencyCounter";
    public static final String counterBolt = "counter";
    public static final String intermediateRankerBolt = "intermediateRanker";
    public static final String totalRankerBolt = "finalRanker";
    public static final String dumperBolt = "ranksDumper";
    public static final String topology = "slidingWindowCounts";
  }

  private static class Columns {
    private static final Fields freqCounter = new Fields("obj", "count");
    private static final Fields counter = new Fields("obj");
  }

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;
  private final int topN;
  private final int windowDuration;
  private final int windowPrecision;

  public RollingTopWords(String topologyName) throws InterruptedException, IOException {
    builder = new TopologyBuilder();
    this.topologyName = topologyName;
    topologyConfig = createTopologyConfiguration();

    com.typesafe.config.Config conf = ConfigFactory.load();
    runtimeInSeconds = conf.getInt(K.runningFor);
    windowDuration = conf.getInt(K.windowDuration);
    windowPrecision = conf.getInt(K.windowPrecision);
    topN = conf.getInt(K.top);

    wireTopology();
  }

  private static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.setDebug(true);
    return conf;
  }

  private List<IrcSpout> setSpouts(TopologyBuilder builder, Map<Long, String> topChannels) {
    return topChannels.entrySet()
        .stream()
        .map(entry -> {
          long channelId = entry.getKey();
          String channelName = entry.getValue();
          IrcSpout spout = new IrcSpout(channelId, channelName, true);
          builder.setSpout(spout.descriptor(), spout);
          return spout;
        })
        .collect(Collectors.toList());
  }

  public void setFreqCounter(List<IrcSpout> spouts, Map<Long, String> topChannels) {
    Map<Long, EmoticonExtractor> textParsers = Data.textParsers(topChannels.keySet());
    BaseRichBolt freqCounter = new FrequencyCounterBolt(textParsers);

    BoltDeclarer freqBolt = builder.setBolt(Ids.freqCounterBolt, freqCounter, Parallelism.freqCounterBolt);
    for (IrcSpout spout : spouts) {
      freqBolt = freqBolt.shuffleGrouping(spout.descriptor());
    }
  }

  public void setCounter(TopologyBuilder builder) {
    builder
        .setBolt(Ids.counterBolt, new RollingCountBolt(windowDuration, windowPrecision), Parallelism.rollingCounterBolt)
        .fieldsGrouping(Ids.freqCounterBolt, Columns.freqCounter);
  }

  private void setIntermediateRanker(TopologyBuilder builder) {
    builder
        .setBolt(Ids.intermediateRankerBolt, new IntermediateRankingsBolt(topN), Parallelism.intermediateRankerBolt)
        .fieldsGrouping(Ids.counterBolt, Columns.counter);
  }

  private void setTotalRanker(TopologyBuilder builder) {
    builder
        .setBolt(Ids.totalRankerBolt, new TotalRankingsBolt(topN))
        .globalGrouping(Ids.intermediateRankerBolt);
  }

  private void setDumper(TopologyBuilder builder) {
    builder
        .setBolt(Ids.dumperBolt, new DumpRankingsBolt())
        .globalGrouping(Ids.totalRankerBolt);
  }

  private void wireTopology() throws InterruptedException, IOException {
    Map<Long, String> topChannels = Data.channels();
    List<IrcSpout> spouts = setSpouts(builder, topChannels);
    setFreqCounter(spouts, topChannels);
    setCounter(builder);
    setIntermediateRanker(builder);
    setTotalRanker(builder);
    setDumper(builder);
  }

  public void runLocally() throws InterruptedException {
    StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
  }

  public void runRemotely() throws Exception {
    StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
  }

  public static void main(String[] args) throws Exception {
    // TODO: load in parallel
    Bootstrap.launch();

    String topologyName = Ids.topology;
    if (args.length >= 1) {
      topologyName = args[0];
    }
    boolean runLocally = true;
    if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
      runLocally = false;
    }

    log.info("Topology name: " + topologyName);
    RollingTopWords rtw = new RollingTopWords(topologyName);
    if (runLocally) {
      log.info("Running in local mode");
      rtw.runLocally();
    } else {
      log.info("Running in remote (cluster) mode");
      rtw.runRemotely();
    }
  }
}
