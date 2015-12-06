package com.scratchy.obj;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EmoticonPack implements Iterable<Emoticon> {

  private List<Emoticon> emoticons;

  public EmoticonPack() {
  }

  public List<Emoticon> getEmoticons() {
    return emoticons;
  }

  public void setEmoticons(List<Emoticon> emoticons) {
    this.emoticons = emoticons;
  }

  public java.util.stream.Stream<Emoticon> stream() {
    return emoticons.stream();
  }

  @Override
  public Iterator<Emoticon> iterator() {
    return emoticons != null
            ? emoticons.iterator()
            : Collections.emptyListIterator();
  }
}
