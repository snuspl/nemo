package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.EndpointAddress;
import edu.snu.vortex.runtime.common.message.MessageContext;

import java.io.Serializable;
import java.util.Optional;

/**
 * A simple {@link MessageContext} implementation that works on a single node.
 */
final class LocalMessageContext implements MessageContext {

  private final EndpointAddress senderAddress;
  private Throwable throwable;
  private Object replyMessage;

  LocalMessageContext(final EndpointAddress senderAddress) {
    this.senderAddress = senderAddress;
  }

  @Override
  public EndpointAddress getSenderAddress() {
    return senderAddress;
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
