package com.scratchy.crawlers;

import com.scratchy.Global;
import org.pircbotx.Configuration;
import org.pircbotx.PircBotX;
import org.pircbotx.exception.IrcException;
import org.pircbotx.hooks.ListenerAdapter;
import org.pircbotx.hooks.types.GenericMessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ChatCrawler {

  private final static Logger log = LoggerFactory.getLogger(ChatCrawler.class);

  private final ListenerAdapter delegate;

  private final String channel;

  public ChatCrawler(String channel, ListenerAdapter delegate) {
    this.channel = channel;
    this.delegate = delegate;
  }

  public ChatCrawler(String channel) {
    this.channel = channel;
    this.delegate = getDelegate();
  }

  private Configuration ircConf() {
    return new Configuration.Builder()
            .setName(Global.ircName())
            .setServerHostname(Global.ircHost())
            .setServerPassword(Global.ircPassword())
            .addAutoJoinChannel("#" + channel)
            .addListener(delegate)
            .buildConfiguration();
  }

  public ListenerAdapter getDelegate() {
    return new ListenerAdapter() {
      @Override
      public void onGenericMessage(GenericMessageEvent event) throws Exception {
        log.info("[" + ChatCrawler.this.channel + "] " + event.getMessage());
      }
    };
  }

  public void start() {
    try {
      Configuration conf = ircConf();
      PircBotX bot = new PircBotX(conf);
      log.info("/irc-crawler: " + bot.toString());
      bot.startBot();
    } catch (IOException | IrcException e) {
      log.warn("/irc-crawler: oops, failure", e);
    }
  }
}
