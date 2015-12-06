package com.scratchy.storm.starter.spout;

import com.scratchy.db.MessageDumper;
import org.pircbotx.hooks.ListenerAdapter;
import org.pircbotx.hooks.types.GenericMessageEvent;

import java.io.Serializable;

public class IrcListener extends ListenerAdapter implements Serializable {

  public interface Delegate {
    boolean shouldStore();

    long delegateId();

    void receive(String message);
  }

  private final Delegate delegate;

  public IrcListener(Delegate delegate) {
    this.delegate = delegate;
  }

  @Override
  public void onGenericMessage(GenericMessageEvent event) throws Exception {
    String message = event.getMessage();
    if (delegate.shouldStore()) {
      MessageDumper.put(delegate.delegateId(), message);
    }
    delegate.receive(event.getMessage());
    super.onGenericMessage(event);
  }
}
