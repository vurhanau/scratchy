package com.scratchy.storm.starter.spout;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IO {
  private static final ExecutorService pool;

  static {
    Config conf = ConfigFactory.load();
    int size = conf.getInt("crawler.pool.size");
    pool = Executors.newFixedThreadPool(size);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      pool.shutdown();
    }));
  }

  public static CompletableFuture<Void> async(Runnable runnable) {
    return CompletableFuture.runAsync(runnable, pool);
  }
}
