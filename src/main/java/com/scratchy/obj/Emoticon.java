package com.scratchy.obj;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Emoticon {
  private int width;

  private int height;

  private String regex;

  private String state;

  @JsonProperty("subscriber_only")
  private boolean subscriberOnly;

  private String url;

  public Emoticon() {
  }

  public int getWidth() {
    return width;
  }

  public void setWidth(int width) {
    this.width = width;
  }

  public int getHeight() {
    return height;
  }

  public void setHeight(int height) {
    this.height = height;
  }

  public String getRegex() {
    return regex;
  }

  public void setRegex(String regex) {
    this.regex = regex;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public boolean isSubscriberOnly() {
    return subscriberOnly;
  }

  public void setSubscriberOnly(boolean subscriberOnly) {
    this.subscriberOnly = subscriberOnly;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }
}
