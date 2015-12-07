package com.scratchy.storm.starter;

import backtype.storm.Config;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import com.scratchy.db.Data;
import com.scratchy.storm.starter.bolt.PrintOutBolt;
import com.scratchy.storm.starter.spout.IrcSpout;
import com.scratchy.storm.starter.util.StormRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PrintOutTopology {
  private static final Logger LOG = Logger.getLogger(RollingTopWords.class);
  private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;

  public PrintOutTopology(String topologyName) throws InterruptedException, IOException {
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
    String printOut = "printOut";
    Map<Long, String> topChannels = Data.channels();
    List<IrcSpout> spouts = putSpouts(builder, topChannels);
    BoltDeclarer printerBolt = builder.setBolt(printOut, new PrintOutBolt(), 10);
    for(IrcSpout spout : spouts) {
      printerBolt = printerBolt.shuffleGrouping(spout.descriptor());
    }
  }

  public void runLocally() throws InterruptedException {
    StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
  }

  public void runRemotely() throws Exception {
    StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
  }

  public static void main(String[] args) throws Exception {
    String topologyName = "printer";
    if (args.length >= 1) {
      topologyName = args[0];
    }
    boolean runLocally = true;
    if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
      runLocally = false;
    }

    LOG.info("Topology name: " + topologyName);
    PrintOutTopology rtw = new PrintOutTopology(topologyName);
    if (runLocally) {
      LOG.info("Running in local mode");
      rtw.runLocally();
    } else {
      LOG.info("Running in remote (cluster) mode");
      rtw.runRemotely();
    }
  }
}
