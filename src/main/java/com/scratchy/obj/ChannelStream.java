package com.scratchy.obj;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ChannelStream {

  @JsonProperty("_id")
  private long id;

  private String game;

  private int viewers;

  @JsonProperty("created_at")
  private String createdAt;

  private Channel channel;

  public ChannelStream() {
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getGame() {
    return game;
  }

  public void setGame(String game) {
    this.game = game;
  }

  public int getViewers() {
    return viewers;
  }

  public void setViewers(int viewers) {
    this.viewers = viewers;
  }

  public String getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(String createdAt) {
    this.createdAt = createdAt;
  }

  public Channel getChannel() {
    return channel;
  }

  public void setChannel(Channel channel) {
    this.channel = channel;
  }

  public String name() {
    return channel.getName();
  }

  public String displayName() {
    return channel.getDisplayName();
  }

  @Override
  public String toString() {
    String name = String.format("%1$15s", channel.getDisplayName());
    return String.format("[%d]: %s - %d", id, name, viewers);
  }
}
