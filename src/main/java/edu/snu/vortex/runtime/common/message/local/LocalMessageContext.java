package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.MessageContext;

import java.io.Serializable;
import java.util.Optional;

/**
 * A simple {@link MessageContext} implementation that works on a single node.
 */
final class LocalMessageContext implements MessageContext {

  private final String senderName;
  private Throwable throwable;
  private Object replyMessage;

  LocalMessageContext(final String senderName) {
    this.senderName = senderName;
  }

  @Override
  public String getSenderName() {
    return senderName;
  }

  @Override
  public <U extends Serializable> void reply(final U message) {
    this.replyMessage = message;
  }

  @Override
  public void replyThrowable(final Throwable th) {
    this.throwable = th;
  }

  public Optional<Throwable> getThrowable() {
    return Optional.ofNullable(throwable);
  }

  public Optional<Object> getReplyMessage() {
    return Optional.ofNullable(replyMessage);
  }
}
