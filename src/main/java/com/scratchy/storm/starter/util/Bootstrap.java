package com.scratchy.storm.starter.util;

import com.scratchy.Global;
import com.scratchy.db.ChannelDumper;
import com.scratchy.db.Data;
import com.scratchy.db.EmoticonDumper;

import java.io.IOException;

public class Bootstrap {

  public static void launch() throws IOException {
    Data.flushRedis();
    ChannelDumper.toRedis(Global.topN());
    EmoticonDumper.packToRedis();
  }

  public static void main(String[] args) throws IOException {
    launch();
  }
}
