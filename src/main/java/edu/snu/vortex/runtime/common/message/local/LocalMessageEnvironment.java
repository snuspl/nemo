package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.*;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A simple {@link MessageEnvironment} implementation that works on a single node.
 */
public final class LocalMessageEnvironment implements MessageEnvironment {

  private final String currentNodeName;
  private final LocalMessageDispatcher dispatcher;

  public LocalMessageEnvironment(final String currentNodeName, final LocalMessageDispatcher dispatcher) {
    this.currentNodeName = currentNodeName;
    this.dispatcher = dispatcher;
  }

  @Override
  public <T extends Serializable> MessageSender<T> setupListener(
      final String messageTypeName, final MessageListener<T> listener) {
    return dispatcher.setupListener(currentNodeName, messageTypeName, listener);
  }

  @Override
  public <T extends Serializable> Future<MessageSender<T>> asyncConnect(
      final String targetNodeName, final String messageTypeName) {
    return CompletableFuture.completedFuture(new LocalMessageSender<T>(
        currentNodeName, targetNodeName, messageTypeName, dispatcher));
  }

  @Override
  public String getCurrentNodeName() {
    return currentNodeName;
  }
}
