package com.scratchy.text;

import com.scratchy.db.Data;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.ToIntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class EmoticonExtractor implements Serializable {

  private final Map<String, Pattern> iconCache;

  private EmoticonExtractor(Map<String, Pattern> cache) {
    this.iconCache = cache;
  }

  public static EmoticonExtractor create(List<String> icons) {
    Map<String, Pattern> cache =
        icons.stream()
            .collect(
                Collectors.toMap(
                    re -> re,
                    re -> Pattern.compile(re)
                )
            );
    return new EmoticonExtractor(cache);
  }

  public static EmoticonExtractor create(Path emoFile) throws IOException {
    return create(Files.readAllLines(emoFile));
  }

  public static EmoticonExtractor create(String redisKey) {
    try (Jedis jedis = Data.jedis()) {
      long llen = jedis.llen(redisKey);
      List<String> icons = jedis.lrange(redisKey, 0, llen);
      return create(icons);
    }
  }

  private static int count(Pattern pattern, String message) {
    int occurrences = 0;
    Matcher matcher = pattern.matcher(message);
    while (matcher.find()) {
      occurrences++;
    }
    return occurrences;
  }

  private static int count(Pattern pattern, Collection<String> chat) {
    return chat.stream().mapToInt(message -> count(pattern, message)).sum();
  }

  public Map<String, Integer> frequency(ToIntFunction<Pattern> valueSelector) {
    Map<String, Integer> freqMap =
        iconCache.entrySet().stream().collect(
            Collectors.toMap(
                e -> e.getKey(),
                e -> valueSelector.applyAsInt(e.getValue())
            ));
    return truncate(freqMap);
  }

  public Map<String, Integer> frequency(Path chatFile) throws IOException {
    return frequency(Files.readAllLines(chatFile));
  }

  public Map<String, Integer> frequency(List<String> chat) {
    return frequency(pattern -> count(pattern, chat));
  }

  public Map<String, Integer> frequency(String message) {
    return frequency(pattern -> count(pattern, message));
  }

  private static Map<String, Integer> truncate(Map<String, Integer> original) {
    return original.entrySet().stream()
        .filter(e -> e.getValue() > 0)
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
  }

  public static Map<Integer, List<String>> inverted(Map<String, Integer> index) {
    Map<Integer, List<String>> invertedIndex = new HashMap<>();
    index.entrySet()
        .stream()
        .forEach(entry -> {
          int count = entry.getValue();
          String icon = entry.getKey();
          invertedIndex.computeIfPresent(count, (countKey, bucket) -> {
            bucket.add(icon);
            return bucket;
          });
          invertedIndex.computeIfAbsent(count, countKey -> {
            List<String> bucket = new ArrayList<>();
            bucket.add(icon);
            return bucket;
          });
        });
    return invertedIndex;
  }
}
