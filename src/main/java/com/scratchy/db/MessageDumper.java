package com.scratchy.db;

import redis.clients.jedis.Jedis;

public class MessageDumper {

  public static void put(long channelId, String message) {
    try (Jedis jedis = Data.jedis()) {
      jedis.rpush(Data.messagePackKey(channelId), message);
    }
  }
}
