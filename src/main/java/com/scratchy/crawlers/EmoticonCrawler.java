package com.scratchy.crawlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scratchy.obj.ChannelStream;
import com.scratchy.obj.ChannelStreamPack;
import com.scratchy.obj.Emoticon;
import com.scratchy.obj.EmoticonPack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EmoticonCrawler {

  private static final String urlBase = "https://api.twitch.tv/kraken/chat/%s/emoticons";

  private static final Logger log = LoggerFactory.getLogger(EmoticonCrawler.class);

  public static Map<Long, List<Emoticon>> download(ChannelStreamPack channels) throws IOException {
    Map<Long, List<Emoticon>> emoticonMap = new HashMap<>(channels.size());
    for(ChannelStream channel : channels) {
      List<Emoticon> icons = download(channel);
      emoticonMap.put(channel.getId(), icons);
    }
    return emoticonMap;
  }

  public static List<Emoticon> download(ChannelStream channel) throws IOException {
    return download(channel.name());
  }

  public static List<Emoticon> download(String channel) throws IOException {
    String uri = String.format(urlBase, channel);
    ObjectMapper parser = new ObjectMapper();
    log.debug("/emoticon-crawler: [{}] - {}", channel, uri);
    EmoticonPack val = parser.readValue(new URL(uri), EmoticonPack.class);
    return val.getEmoticons();
  }

  public static List<String> downloadStrings(ChannelStream channel) throws IOException {
    return downloadStrings(channel.name());
  }

  public static List<String> downloadStrings(String channel) throws IOException {
    String uri = String.format(urlBase, channel);
    ObjectMapper parser = new ObjectMapper();
    log.debug("/emoticon-crawler: [{}] - {}", channel, uri);
    EmoticonPack val = parser.readValue(new URL(uri), EmoticonPack.class);
    return val.stream().map(c -> c.getRegex()).collect(Collectors.toList());
  }
}
