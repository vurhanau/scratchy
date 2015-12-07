package com.scratchy.storm.starter.spout;

import com.scratchy.Global;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IO {
  private static final ExecutorService pool;

  static {
    pool = Executors.newFixedThreadPool(Global.crawlerPoolSize());
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      pool.shutdown();
    }));
  }

  public static CompletableFuture<Void> async(Runnable runnable) {
    return CompletableFuture.runAsync(runnable, pool);
  }
}
