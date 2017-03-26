package edu.snu.vortex.runtime.common.message.local;

import edu.snu.vortex.runtime.common.message.EndpointAddress;
import edu.snu.vortex.runtime.common.message.MessageAddress;
import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.common.message.MessageSender;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

/**
 * Dispatch messages on a single machine.
 */
final class LocalMessageDispatcher {

  private final ConcurrentMap<MessageAddress, MessageListener> messageListenerMap;

  LocalMessageDispatcher() {
    this.messageListenerMap = new ConcurrentHashMap<>();
  }

  <T extends Serializable> MessageSender<T> setupListener(
      final EndpointAddress endpointAddress, final String name, final MessageListener<T> listener) {
    final MessageAddress messageAddress = new MessageAddress(endpointAddress, name);
    if (messageListenerMap.putIfAbsent(messageAddress, listener) != null) {
      throw new RuntimeException(messageAddress + " was already used");
    }

    return new LocalMessageSender<>(endpointAddress, messageAddress, this);
  }

  <T extends Serializable> void dispatchSendMessage(final MessageAddress targetAddress, final T message) {
    final MessageListener listener = messageListenerMap.get(targetAddress);
    if (listener == null) {
      throw new RuntimeException("There was no set up listener for " + targetAddress);
    }
    listener.onSendMessage(message);
  }

  <T extends Serializable, U extends Serializable> Future<U> dispatchAskMessage(
      final EndpointAddress senderAddress, final MessageAddress targetAddress, final T message) {
    final MessageListener listener = messageListenerMap.get(targetAddress);
    if (listener == null) {
      throw new RuntimeException("There was no set up listener for " + targetAddress);
    }

    final LocalMessageContext context = new LocalMessageContext(senderAddress);
    listener.onAskMessage(message, context);

    final Optional<Throwable> throwable = context.getThrowable();
    final Optional<Object> replyMessage = context.getReplyMessage();

    final CompletableFuture future = new CompletableFuture();
    if (throwable.isPresent()) {
      future.completeExceptionally(throwable.get());
    } else {
      future.complete(replyMessage.orElse(null));
    }

    return future;
  }
}
