package com.scratchy.storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.scratchy.crawlers.ChatCrawler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class IrcSpout extends BaseRichSpout implements IrcListener.Delegate {

  private final static Logger log = LoggerFactory.getLogger("noBullshit");

  private final Queue<String> incoming;

  private final long channelId;

  private final String channelName;

  private final boolean store;

  private final ChatCrawler bot;

  private SpoutOutputCollector collector;

  public IrcSpout(long channelId, String channelName) {
    this(channelId, channelName, false);
  }

  public IrcSpout(long channelId, String channelName, boolean store) {
    this.channelId = channelId;
    this.channelName = channelName;
    this.incoming = new ConcurrentLinkedQueue<>();
    this.bot = new ChatCrawler(channelName, new IrcListener(this));
    this.store = store;
  }

  @Override
  public boolean shouldStore() {
    return store;
  }

  @Override
  public long delegateId() {
    return channelId;
  }

  public long id() {
    return channelId;
  }

  @Override
  public void receive(String message) {
    incoming.add(message);
  }

  public String descriptor() {
    return "irc-spout-" + channelId;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("channel-id", "message"));
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;
    bot.start();
    log.info("/irc-spout: started");
  }

  @Override
  public void nextTuple() {
    if(!incoming.isEmpty()) {
      String message = incoming.poll();
      collector.emit(new Values(channelId, message));
    }
  }
}
