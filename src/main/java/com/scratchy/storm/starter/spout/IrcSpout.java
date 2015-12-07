package com.scratchy.storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.scratchy.crawlers.ChatCrawler;
import com.scratchy.db.MessageDumper;
import org.pircbotx.hooks.ListenerAdapter;
import org.pircbotx.hooks.types.GenericMessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

public class IrcSpout extends BaseRichSpout {

  private final static Logger log = LoggerFactory.getLogger("noBullshit");

  private final Queue<String> incoming;

  private final long channelId;

  private final String channelName;

  private final boolean store;

  private SpoutOutputCollector collector;

  public IrcSpout(long channelId, String channelName) {
    this(channelId, channelName, false);
  }

  public IrcSpout(long channelId, String channelName, boolean store) {
    this.channelId = channelId;
    this.channelName = channelName;
    this.incoming = new ConcurrentLinkedQueue<>();
    this.store = store;
  }

  private class Listener extends ListenerAdapter {
    @Override
    public void onGenericMessage(GenericMessageEvent event) throws Exception {
      String message = event.getMessage();
      if (store) {
        MessageDumper.put(channelId, message);
      }
      incoming.add(event.getMessage());
    }
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
    IO.async(() -> new ChatCrawler(channelName, new Listener()).start());
  }

  @Override
  public void nextTuple() {
    if (!incoming.isEmpty()) {
      String message = incoming.poll();
      collector.emit(new Values(channelId, message));
    }
  }
}
