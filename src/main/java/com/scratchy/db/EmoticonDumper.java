package com.scratchy.db;

import com.scratchy.crawlers.EmoticonCrawler;
import com.scratchy.obj.ChannelStream;
import com.scratchy.obj.ChannelStreamPack;
import com.scratchy.obj.Emoticon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EmoticonDumper {

  private static final Logger log = LoggerFactory.getLogger(EmoticonDumper.class);

  public static void packToRedis() throws IOException {
    try (Jedis jedis = Data.jedis()) {
      long llen = jedis.llen(Data.channelsPackKey);
      List<String> ids = jedis.lrange(Data.channelsPackKey, 0, llen);
      for (String id : ids) {
        String channel = jedis.hget(Data.channelKey(id), "channel-name");
        log.debug("/emoticon-dumper: [{}] - downloading emoticons...", channel);
        List<Emoticon> icons = EmoticonCrawler.download(channel);
        log.debug("/emoticon-dumper: [{}] - dumping emoticons...", channel);

        Set<String> inUse = new HashSet<>();
        for (Emoticon icon : icons) {
          String re = icon.getRegex();
          if (inUse.contains(re)) continue;

          jedis.rpush(Data.emoticonsKey(id), re);
          jedis.rpush(Data.emoticonUrlsKey(id), icon.getUrl());
          inUse.add(re);
        }
        log.debug("/emoticon-dumper: [{}] - done!", channel);
      }
    }
  }

  public static void toFile(ChannelStreamPack channels, Path file) throws IOException {
    for (ChannelStream channel : channels) {
      log.debug("/emoticon-dumper: [{}] - downloading...", channel.displayName());
      List<String> icons = new ArrayList<>();
      icons.add("# " + channel.toString());
      icons.addAll(EmoticonCrawler.downloadStrings(channel));
      log.debug("/emoticon-dumper: [{}] - dumping...", channel.displayName());
      Files.write(file, icons, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
      log.debug("/emoticon-dumper: [{}] - done!", channel.displayName());
    }
  }

  public static void toFile(String channel, Path file) throws IOException {
    Iterable<String> lines = EmoticonCrawler.downloadStrings(channel);
    Files.write(file, lines, StandardOpenOption.CREATE);
  }
}
