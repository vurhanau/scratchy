package com.scratchy.storm.starter.util;

import com.scratchy.db.ChannelDumper;
import com.scratchy.db.Data;
import com.scratchy.db.EmoticonDumper;

import java.io.IOException;

public class Bootstrap {

  public static void launch() throws IOException {
    Data.flushRedis();
    ChannelDumper.toRedis(10);
    EmoticonDumper.packToRedis();
  }
}
