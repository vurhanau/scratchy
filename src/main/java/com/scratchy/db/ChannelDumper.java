package com.scratchy.db;

import com.scratchy.crawlers.ChannelCrawler;
import com.scratchy.obj.Channel;
import com.scratchy.obj.ChannelStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ChannelDumper {

  private static final Logger log = LoggerFactory.getLogger(ChannelDumper.class);

  private static String id(ChannelStream channel) {
    return String.valueOf(channel.getId());
  }

  // twitch:channels:pack
  // twitch:channels:{id}
  public static void toRedis(int topn) throws IOException {
    try (Jedis jedis = Data.jedis()) {
      log.debug("/channel-dumper: downloading top-{} channels...", topn);
      List<ChannelStream> channels = ChannelCrawler.download(topn);
      log.debug("/channel-dumper: dumping top-{} channels...", topn);
      channels
          .stream()
          .forEach(channel -> {
            jedis.rpush(Data.channelsPackKey, id(channel));
            jedis.hmset(Data.channelKey(channel.getId()), Data.hmmap(channel));
          });
      log.debug("/channel-dumper: top-{} channels - done!", topn);
    }
  }

  public static void toFile(int n, Path file) throws IOException {
    log.debug("/channel-dumper: downloading top-{} channels...", n);
    List<ChannelStream> downloaded = ChannelCrawler.download(n);
    log.debug("/channel-dumper: dumping top-{} channels...", n);
    List<String> lines = downloaded.stream().map(channel -> channel.toString()).collect(Collectors.toList());
    Files.write(file, lines, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    log.debug("/channel-dumper: dumping top-{} - done!", n);
  }
}
