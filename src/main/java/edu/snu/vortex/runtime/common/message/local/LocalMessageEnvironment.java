package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.common.message.MessageSender;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A simple {@link MessageEnvironment} implementation that works on a single node.
 * @param <T> The type of the message to be sent in the environment.
 */
public final class LocalMessageEnvironment<T> implements MessageEnvironment {

  private final String currentNodeId;
  private final LocalMessageDispatcher dispatcher;

  public LocalMessageEnvironment(final String currentNodeId,
                                 final LocalMessageDispatcher dispatcher) {
    this.currentNodeId = currentNodeId;
    this.dispatcher = dispatcher;
  }

  @Override
  public MessageSender<T> setupListener(
      final String messageTypeId, final MessageListener listener) {
    return dispatcher.setupListener(currentNodeId, messageTypeId, listener);
  }

  @Override
  public Future<MessageSender<T>> asyncConnect(
      final String targetId, final String messageTypeId) {
    return CompletableFuture.completedFuture(new LocalMessageSender<T>(
        currentNodeId, targetId, messageTypeId, dispatcher));
  }

  @Override
  public String getCurrentId() {
    return currentNodeId;
  }
}
