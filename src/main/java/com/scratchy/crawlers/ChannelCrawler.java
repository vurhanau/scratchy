package com.scratchy.crawlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scratchy.Global;
import com.scratchy.obj.ChannelStream;
import com.scratchy.obj.ChannelStreamPack;

import java.io.IOException;
import java.net.URL;
import java.util.List;

public class ChannelCrawler {

  public static List<ChannelStream> download() throws IOException {
    return download(Global.topN());
  }

  public static List<ChannelStream> download(int topN) throws IOException {
    ObjectMapper parser = new ObjectMapper();
    String url = String.format(Global.channelsUrlBase(), topN);
    ChannelStreamPack pack = parser.readValue(new URL(url), ChannelStreamPack.class);
    return pack.getChannelStreams();
  }
}
