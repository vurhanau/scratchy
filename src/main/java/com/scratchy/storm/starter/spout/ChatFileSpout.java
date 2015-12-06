package com.scratchy.storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.scratchy.text.EmoticonExtractor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ChatFileSpout extends BaseRichSpout {

  private final AtomicInteger counter;
  private final int total;
  private final List<String> messages;
  private EmoticonExtractor parser;
  private SpoutOutputCollector collector;

  private ChatFileSpout(List<String> messages, EmoticonExtractor parser) {
    this.counter = new AtomicInteger(0);
    this.messages = messages;
    this.total = messages.size();
    this.parser = parser;
  }

  public static ChatFileSpout create(String chatPath, String emoticonsPath) throws IOException {
    List<String> messages = Files.readAllLines(Paths.get(chatPath));
    EmoticonExtractor parser = EmoticonExtractor.create(emoticonsPath);
    return new ChatFileSpout(messages, parser);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("emoticon", "frequency"));
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);
    int cnt = counter.incrementAndGet();
    String message = messages.get(cnt % total);
    Map<String, Integer> freq = parser.frequency(message);
    freq.entrySet().forEach(entry -> {
      collector.emit(new Values(entry.getKey(), entry.getValue()));
    });
  }
}
