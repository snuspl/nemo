package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.MessageSender;

import java.io.Serializable;
import java.util.concurrent.Future;

/**
 * A simple {@link MessageSender} implementation that works on a single node.
 * @param <T> a message type
 */
final class LocalMessageSender<T extends Serializable> implements MessageSender<T> {

  private final String senderNodeName;
  private final String targetNodeName;
  private final String messageTypeName;
  private final LocalMessageDispatcher dispatcher;

  LocalMessageSender(final String senderNodeName,
                     final String targetNodeName,
                     final String messageTypeName,
                     final LocalMessageDispatcher dispatcher) {
    this.senderNodeName = senderNodeName;
    this.targetNodeName = targetNodeName;
    this.messageTypeName = messageTypeName;
    this.dispatcher = dispatcher;
  }

  @Override
  public void send(final T message) {
    dispatcher.dispatchSendMessage(targetNodeName, messageTypeName, message);
  }

  @Override
  public <U extends Serializable> Future<U> ask(final T message) {
    return dispatcher.dispatchAskMessage(senderNodeName, targetNodeName, messageTypeName, message);
  }
}
