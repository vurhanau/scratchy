package com.scratchy.obj;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Channel {
  @JsonProperty("_id")
  private long id;

  private String status;

  @JsonProperty("display_name")
  private String displayName;

  private String game;

  private String language;

  private String name;

  @JsonProperty("created_at")
  private String createdAt; //"2008-04-27T18:30:26Z"

  @JsonProperty("updated_at")
  private String updatedAt;

  private String url;

  private int views;

  private int followers;

  @JsonProperty("logo")
  private String logoLink;

  public Channel() {}

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  public String getGame() {
    return game;
  }

  public void setGame(String game) {
    this.game = game;
  }

  public String getLanguage() {
    return language;
  }

  public void setLanguage(String language) {
    this.language = language;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(String createdAt) {
    this.createdAt = createdAt;
  }

  public String getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(String updatedAt) {
    this.updatedAt = updatedAt;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public int getViews() {
    return views;
  }

  public void setViews(int views) {
    this.views = views;
  }

  public int getFollowers() {
    return followers;
  }

  public void setFollowers(int followers) {
    this.followers = followers;
  }

  public String getLogoLink() {
    return logoLink;
  }

  public void setLogoLink(String logoLink) {
    this.logoLink = logoLink;
  }
}
