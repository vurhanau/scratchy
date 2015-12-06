package com.scratchy.obj;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ChannelStreamPack implements Iterable<ChannelStream> {

  @JsonProperty("streams")
  private List<ChannelStream> channelStreams;

  public ChannelStreamPack() {
  }

  public List<ChannelStream> getChannelStreams() {
    return channelStreams;
  }

  public void setChannelStreams(List<ChannelStream> channelStreams) {
    this.channelStreams = channelStreams;
  }

  public Stream<ChannelStream> stream() {
    return channelStreams.stream();
  }

  public int size() {
    return channelStreams.size();
  }

  @Override
  public Iterator<ChannelStream> iterator() {
    return channelStreams != null
            ? channelStreams.iterator()
            : Collections.emptyListIterator();
  }

  @Override
  public String toString() {
    return channelStreams.stream()
            .map(s -> s.toString())
            .collect(Collectors.joining(System.lineSeparator()));
  }
}
