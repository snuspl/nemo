package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.*;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A simple {@link MessageEnvironment} implementation that works on a single node.
 */
public final class LocalMessageEnvironment implements MessageEnvironment {

  private final EndpointAddress endpointAddress;
  private final LocalMessageDispatcher dispatcher;

  public LocalMessageEnvironment(final EndpointAddress endpointAddress, final LocalMessageDispatcher dispatcher) {
    this.endpointAddress = endpointAddress;
    this.dispatcher = dispatcher;
  }

  @Override
  public <T extends Serializable> MessageSender<T> setupListener(
      final String name, final MessageListener<T> listener) {
    return dispatcher.setupListener(endpointAddress, name, listener);
  }

  @Override
  public <T extends Serializable> Future<MessageSender<T>> asyncConnect(final MessageAddress targetAddress) {
    return CompletableFuture.completedFuture(new LocalMessageSender<T>(endpointAddress, targetAddress, dispatcher));
  }

  @Override
  public EndpointAddress getCurrentAddress() {
    return endpointAddress;
  }
}
