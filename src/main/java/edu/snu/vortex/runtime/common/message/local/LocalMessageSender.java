package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.EndpointAddress;
import edu.snu.vortex.runtime.common.message.MessageAddress;
import edu.snu.vortex.runtime.common.message.MessageSender;

import java.io.Serializable;
import java.util.concurrent.Future;

/**
 * A simple {@link MessageSender} implementation that works on a single node.
 * @param <T> a message type
 */
final class LocalMessageSender<T extends Serializable> implements MessageSender<T> {

  private final EndpointAddress senderAddress;
  private final MessageAddress targetAddress;
  private final LocalMessageDispatcher dispatcher;

  LocalMessageSender(final EndpointAddress senderAddress,
                     final MessageAddress targetAddress,
                     final LocalMessageDispatcher dispatcher) {
    this.senderAddress = senderAddress;
    this.targetAddress = targetAddress;
    this.dispatcher = dispatcher;
  }

  @Override
  public void send(final T message) {
    dispatcher.dispatchSendMessage(targetAddress, message);
  }

  @Override
  public <U extends Serializable> Future<U> ask(final T message) {
    return dispatcher.dispatchAskMessage(senderAddress, targetAddress, message);
  }
}
