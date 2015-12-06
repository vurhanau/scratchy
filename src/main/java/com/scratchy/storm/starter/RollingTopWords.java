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
import backtype.storm.tuple.Fields;
import com.scratchy.storm.starter.bolt.*;
import com.scratchy.db.Data;
import com.scratchy.storm.starter.spout.IrcSpout;
import com.scratchy.storm.starter.util.StormRunner;
import com.scratchy.text.EmoticonExtractor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class RollingTopWords {
  private static final Logger LOG = Logger.getLogger(RollingTopWords.class);
  private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
  private static final int TOP_N = 5;

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;

  public RollingTopWords(String topologyName) throws InterruptedException, IOException {
    builder = new TopologyBuilder();
    this.topologyName = topologyName;
    topologyConfig = createTopologyConfiguration();
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

    wireTopology();
  }

  private static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.setDebug(true);
    return conf;
  }

  private List<IrcSpout> putSpouts(TopologyBuilder builder, Map<Long, String> topChannels) {
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

  private void wireTopology() throws InterruptedException, IOException {
    String messageParser = "messageParser";
    String counterId = "counter";
    String intermediateRankerId = "intermediateRanker";
    String totalRankerId = "finalRanker";
    String dumperId = "ranksDumper";

    Map<Long, String> topChannels = Data.channels();
    List<IrcSpout> spouts = putSpouts(builder, topChannels);
    Map<Long, EmoticonExtractor> textParsers = Data.textParsers(topChannels.keySet());
    BoltDeclarer freqBolt = builder.setBolt(messageParser, new FrequencyCounterBolt(textParsers), 10);
    for(IrcSpout spout : spouts) {
      freqBolt = freqBolt.shuffleGrouping(spout.descriptor());
    }

    builder.setBolt(counterId, new RollingCountBolt(9, 3), 4)
            .fieldsGrouping(messageParser, new Fields("obj", "count"));

    builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 4)
            .fieldsGrouping(counterId, new Fields("obj"));
    builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);
    builder.setBolt(dumperId, new DumpRankingsBolt()).globalGrouping(totalRankerId);
  }

  public void runLocally() throws InterruptedException {
    StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
  }

  public void runRemotely() throws Exception {
    StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
  }

  /**
   * Submits (runs) the topology.
   * <p>
   * Usage: "RollingTopWords [topology-name] [local|remote]"
   * <p>
   * By default, the topology is run locally under the name "slidingWindowCounts".
   * <p>
   * Examples:
   * <p>
   * <pre>
   * {@code
   *
   * # Runs in local mode (LocalCluster), with topology name "slidingWindowCounts"
   * $ storm jar storm-starter-jar-with-dependencies.jar storm.starter.RollingTopWords
   *
   * # Runs in local mode (LocalCluster), with topology name "foobar"
   * $ storm jar storm-starter-jar-with-dependencies.jar storm.starter.RollingTopWords foobar
   *
   * # Runs in local mode (LocalCluster), with topology name "foobar"
   * $ storm jar storm-starter-jar-with-dependencies.jar storm.starter.RollingTopWords foobar local
   *
   * # Runs in remote/cluster mode, with topology name "production-topology"
   * $ storm jar storm-starter-jar-with-dependencies.jar storm.starter.RollingTopWords production-topology remote
   * }
   * </pre>
   *
   * @param args First positional argument (optional) is topology name, second positional argument (optional) defines
   *             whether to run the topology locally ("local") or remotely, i.e. on a real cluster ("remote").
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
//    Bootstrap.launch();

    String topologyName = "slidingWindowCounts";
    if (args.length >= 1) {
      topologyName = args[0];
    }
    boolean runLocally = true;
    if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
      runLocally = false;
    }

    LOG.info("Topology name: " + topologyName);
    RollingTopWords rtw = new RollingTopWords(topologyName);
    if (runLocally) {
      LOG.info("Running in local mode");
      rtw.runLocally();
    } else {
      LOG.info("Running in remote (cluster) mode");
      rtw.runRemotely();
    }
  }
}
