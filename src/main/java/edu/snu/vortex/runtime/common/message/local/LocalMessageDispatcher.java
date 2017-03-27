package edu.snu.vortex.runtime.common.message.local;

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

  private final ConcurrentMap<String, ConcurrentMap<String, MessageListener>> nodeNameToMessageListenersMap;

  LocalMessageDispatcher() {
    this.nodeNameToMessageListenersMap = new ConcurrentHashMap<>();
  }

  <T extends Serializable> MessageSender<T> setupListener(
      final String currentNodeName, final String messageTypeName, final MessageListener<T> listener) {

    ConcurrentMap<String, MessageListener> messageTypeToListenerMap = nodeNameToMessageListenersMap
        .get(currentNodeName);

    if (messageTypeToListenerMap == null) {
      messageTypeToListenerMap = new ConcurrentHashMap<>();
      final ConcurrentMap<String, MessageListener> map = nodeNameToMessageListenersMap.putIfAbsent(
          currentNodeName, messageTypeToListenerMap);
      if (map != null) {
        messageTypeToListenerMap = map;
      }
    }

    if (messageTypeToListenerMap.putIfAbsent(messageTypeName, listener) != null) {
      throw new RuntimeException(messageTypeName + " was already used in " + currentNodeName);
    }

    return new LocalMessageSender<>(currentNodeName, currentNodeName, messageTypeName, this);
  }

  <T extends Serializable> void dispatchSendMessage(
      final String targetNodeName, final String messageTypeName, final T message) {
    final MessageListener listener = nodeNameToMessageListenersMap.get(targetNodeName).get(messageTypeName);
    if (listener == null) {
      throw new RuntimeException("There was no set up listener for " + messageTypeName + " in " + targetNodeName);
    }
    listener.onSendMessage(message);
  }

  <T extends Serializable, U extends Serializable> Future<U> dispatchAskMessage(
      final String senderNodeName, final String targetNodeName, final String messageTypeName, final T message) {

    final MessageListener listener = nodeNameToMessageListenersMap.get(targetNodeName).get(messageTypeName);
    if (listener == null) {
      throw new RuntimeException("There was no set up listener for " + messageTypeName + " in " + targetNodeName);
    }

    final LocalMessageContext context = new LocalMessageContext(senderNodeName);
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
