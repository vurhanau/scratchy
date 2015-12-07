package com.scratchy.crawlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scratchy.obj.ChannelStream;
import com.scratchy.obj.ChannelStreamPack;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.IOException;
import java.net.URL;
import java.util.List;

public class ChannelCrawler {

  private final static String urlBase = "https://api.twitch.tv/kraken/streams?limit=%d";

  public static List<ChannelStream> download() throws IOException {
    Config conf = ConfigFactory.load();
    int topN = conf.getInt("rankSize");
    return download(topN);
  }

  public static List<ChannelStream> download(int topN) throws IOException {
    ObjectMapper parser = new ObjectMapper();
    String url = String.format(urlBase, topN);
    ChannelStreamPack pack = parser.readValue(new URL(url), ChannelStreamPack.class);
    return pack.getChannelStreams();
  }
}
