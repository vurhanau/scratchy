package com.scratchy.db;

import com.scratchy.Global;
import com.scratchy.obj.ChannelStream;
import com.scratchy.text.EmoticonExtractor;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Data {

  private final static JedisPool pool = new JedisPool(new JedisPoolConfig(), Global.redisHost(), Global.redisPort());

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        pool.close();
    }));
  }

  public static class ChannelFields {
    public static final String name = "channel-name";
    public static final String displayName = "channel-display-name";
    public static final String logo = "channel-logo";
    public static final String game = "game";
    public static final String viewers = "viewers";
    public static final String url = "channel-url";
    public static final String undefValue = "null";
  }

  private static final String appIdentifier = "scratchy";

  public static final String channelsPackKey = appIdentifier + ":twitch:channels:pack";

  public static String messagePackKey(long channel) {
    return String.format("%s:twitch:messages:%d", appIdentifier, channel);
  }

  public static String topIconsKey() {
    return String.format("%s:twitch:emoticons:topn:symbols", appIdentifier);
  }

  public static String topValuesKey() {
    return String.format("%s:twitch:emoticons:topn:val", appIdentifier);
  }

  public static String channelKey(long id) {
    return String.format("%s:twitch:channels:%d", appIdentifier, id);
  }

  public static String channelKey(String id) {
    return String.format("%s:twitch:channels:%s", appIdentifier, id);
  }

  public static String emoticonsKey(long channelId) {
    return String.format("%s:twitch:emoticons:%d:regexps", appIdentifier, channelId);
  }

  public static String emoticonsKey(String channelId) {
    return String.format("%s:twitch:emoticons:%s:regexps", appIdentifier, channelId);
  }

  public static String emoticonUrlsKey(String channelId) {
    return String.format("%s:twitch:emoticons:%s:urls", appIdentifier, channelId);
  }

  public static Jedis jedis() {
    return pool.getResource();
  }

  public static void flushRedis() {
    try (Jedis jedis = pool.getResource()) {
      jedis.keys(appIdentifier + ":twitch:*").forEach(key -> jedis.del(key));
    }
  }

  public static List<String> emoticons(long channel) {
    try (Jedis jedis = pool.getResource()) {
      return emoticons(jedis, channel);
    }
  }

  private static List<String> emoticons(Jedis jedis, long channel) {
    String emoPackKey = emoticonsKey(channel);
    long len = jedis.llen(emoPackKey);
    return jedis.lrange(emoPackKey, 0, len);
  }

  public static Map<Long, String> channels() {
    try (Jedis jedis = pool.getResource()) {
      long len = jedis.llen(channelsPackKey);
      return jedis.lrange(channelsPackKey, 0, len)
              .stream()
              .collect(
                      Collectors.toMap(
                              cid -> Long.valueOf(cid),
                              cid -> jedis.hget(channelKey(cid), ChannelFields.name)
                      )
              );
    }
  }

  public static Map<Long, EmoticonExtractor> textParsers(Collection<Long> ids) {
    try (Jedis jedis = pool.getResource()) {
      return ids.stream().collect(
              Collectors.toMap(
                      id -> id,
                      id -> EmoticonExtractor.create(emoticons(jedis, id))
              )
      );
    }
  }

  public static Map<String, String> hmmap(ChannelStream item) {
    Map<String, String> hmmap = new HashMap<>();
    hmmap.put(ChannelFields.name, item.name());
    hmmap.put(ChannelFields.displayName, item.displayName());
    String logo = item.getChannel().getLogoLink();
    hmmap.put(ChannelFields.logo, logo != null ? logo : ChannelFields.undefValue);
    hmmap.put(ChannelFields.game, item.getGame());
    hmmap.put(ChannelFields.viewers, String.valueOf(item.getViewers()));
    hmmap.put(ChannelFields.url, item.getChannel().getUrl());
    return hmmap;
  }
}
