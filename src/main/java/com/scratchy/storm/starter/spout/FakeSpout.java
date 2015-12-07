package com.scratchy.storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.scratchy.crawlers.ChatCrawler;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public class FakeSpout extends BaseRichSpout {
  private SpoutOutputCollector collector;

  private final long channelId;
  private final String channelName;
  private final ConcurrentLinkedQueue<Values> incoming;
  private final long interval = 2_000L;

  public FakeSpout(long channelId, String channelName, boolean store) {
    this.channelId = channelId;
    this.channelName = channelName;
    this.incoming = new ConcurrentLinkedQueue<>();
  }

  public String descriptor() {
    return "fake-spout-" + channelId;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("channel-id", "message"));
  }

  private Values tuple() {
    return new Values(channelId, "[" + channelName + "]: hello world");
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;
    CompletableFuture.runAsync(() -> {
      while(true) {
        incoming.add(tuple());
        try {
          Thread.sleep(interval);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
  }

  @Override
  public void nextTuple() {
    if(!incoming.isEmpty()) {
      collector.emit(incoming.poll());
    }
  }
}
