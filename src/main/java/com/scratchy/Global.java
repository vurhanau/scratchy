package com.scratchy;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.Arrays;

import static com.scratchy.Global.ConfigKeys.*;

public class Global {

  private static final Config appConf = ConfigFactory.load();

  public static class ConfigKeys {
    public static final String topN = "top-n";
    public static final String runningFor = "running-for";
    public static final String windowDuration = "window.duration";
    public static final String windowPrecision = "window.precision";
    public static final String channelsUrl = "crawler.url-base.channels";
    public static final String emoticonsUrl = "crawler.url-base.emoticons";
    public static final String crawlerPoolSize = "crawler.pool.size";
    public static final String ircName = "crawler.irc.name";
    public static final String ircHost = "crawler.irc.server.hostname";
    public static final String ircPassword = "server.password";
    public static final String redisHost = "redis.ip";
    public static final String redisPort = "redis.port";
    public static final String[] all = {
        topN, runningFor,
        windowDuration, windowPrecision,
        crawlerPoolSize, ircName, ircHost, ircPassword,
        redisHost, redisPort
    };
  }

  public static boolean validConfig() {
    return Arrays.stream(all).allMatch(key -> appConf.hasPath(key));
  }

  public static int topN() {
    return appConf.getInt(topN);
  }

  public static int runningFor() {
    return appConf.getInt(runningFor);
  }

  public static int windowDuration() {
    return appConf.getInt(windowDuration);
  }

  public static int windowPrecision() {
    return appConf.getInt(windowPrecision);
  }

  public static int crawlerPoolSize() {
    return appConf.getInt(crawlerPoolSize);
  }

  public static String ircName() {
    return appConf.getString(ircName);
  }

  public static String ircHost() {
    return appConf.getString(ircHost);
  }

  public static String ircPassword() {
    return appConf.getString(ircPassword);
  }

  public static String redisHost() {
    return appConf.getString(redisHost);
  }

  public static int redisPort() {
    return appConf.getInt(redisPort);
  }

  public static String channelsUrlBase() {
    return appConf.getString(channelsUrl);
  }

  public static String emoticonsUrlBase() {
    return appConf.getString(emoticonsUrl);
  }
}
