package com.scratchy.crawlers;

import com.typesafe.config.ConfigFactory;
import org.pircbotx.Configuration;
import org.pircbotx.PircBotX;
import org.pircbotx.exception.IrcException;
import org.pircbotx.hooks.ListenerAdapter;
import org.pircbotx.hooks.types.GenericMessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class ChatCrawler implements Serializable {

  private final static Logger log = LoggerFactory.getLogger("noBullshit");

  private final ListenerAdapter delegate;

  private final String channel;

  private static class Conf {
    private static final String root = "crawler.irc.";
    public static final String name = root + "name";
    public static final String host = root + "server-hostname";
    public static final String password = root + "server-password";
//    public static final String channel = root + "auto-join-channel";
  }

  public ChatCrawler(String channel, ListenerAdapter delegate) {
    this.channel = channel;
    this.delegate = delegate;
  }

  private Configuration ircConf() {
    com.typesafe.config.Config conf = ConfigFactory.load();
    return new Configuration.Builder()
            .setName(conf.getString(Conf.name))
            .setServerHostname(conf.getString(Conf.host))
            .setServerPassword(conf.getString(Conf.password))
            .addAutoJoinChannel(channel)
            .addListener(delegate)
            .buildConfiguration();
  }

  public static ListenerAdapter getDelegate() {
    return new ListenerAdapter() {
      @Override
      public void onGenericMessage(GenericMessageEvent event) throws Exception {
        System.out.println(event.getMessage());
//        super.onGenericMessage(event);
      }
    };
  }

  public void start() {
    PircBotX bot = new PircBotX(ircConf());
    //Connect to the server
    try {
      bot.startBot();
    } catch (IOException | IrcException e) {
      log.warn("/irc-crawler: oops, failure", e);
    } catch (Exception e) {
      log.warn("shit happened", e);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    new ChatCrawler("#capcomfighters", getDelegate()).start();
  }
}
