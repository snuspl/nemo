package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.MessageSender;

import java.util.concurrent.CompletableFuture;

/**
 * A simple {@link MessageSender} implementation that works on a single node.
 * @param <T> a message type
 */
public final class LocalMessageSender<T> implements MessageSender<T> {

  private final String senderId;
  private final String targetId;
  private final String listenerId;
  private final LocalMessageDispatcher dispatcher;
  private boolean isClosed;

  public LocalMessageSender(final String senderId,
                     final String targetId,
                     final String listenerId,
                     final LocalMessageDispatcher dispatcher) {
    this.senderId = senderId;
    this.targetId = targetId;
    this.listenerId = listenerId;
    this.dispatcher = dispatcher;
    this.isClosed = false;
  }

  @Override
  public void send(final T message) {
    if (isClosed) {
      throw new RuntimeException("Closed");
    }
    dispatcher.dispatchSendMessage(targetId, listenerId, message);
  }

  @Override
  public <U> CompletableFuture<U> request(final T message) {
    if (isClosed) {
      throw new RuntimeException("Closed");
    }
    return dispatcher.dispatchRequestMessage(senderId, targetId, listenerId, message);
  }

  @Override
  public void close() throws Exception {
    isClosed = true;
  }
}
