package edu.snu.vortex.runtime.common.message;

import java.io.Serializable;

/**
 * Handles messages from {@link MessageSender}. Multiple MessageListeners can be setup using {@link MessageEnvironment}
 * while they are identified by their unique names.
 *
 * @param <T> message type
 */
public interface MessageListener<T extends Serializable> {

  /**
   * Received a message.
   * @param message a message
   */
  void onSendMessage(T message);

  /**
   * Received a message, and return a response with {@link MessageContext}.
   * @param message a message
   * @param messageContext a message context
   */
  void onAskMessage(T message, MessageContext messageContext);

}
