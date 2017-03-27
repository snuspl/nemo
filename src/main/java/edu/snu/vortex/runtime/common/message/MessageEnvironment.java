package edu.snu.vortex.runtime.common.message;

import java.io.Serializable;
import java.util.concurrent.Future;

/**
 * Set up {@link MessageListener}s to handle incoming messages on this node, and connect to remote nodes and return
 * {@link MessageSender}s to send message to them.
 */
public interface MessageEnvironment {

  /**
   * Set up a {@link MessageListener} with a message type name.
   *
   * @param messageTypeName name of the message type name which would be handled by message listener
   * @param listener a message listener
   * @param <T> message type
   * @return a message sender to the locally set up listener.
   */
  <T extends Serializable> MessageSender<T> setupListener(String messageTypeName, MessageListener<T> listener);

  /**
   * Asynchronously connect to the node named 'targetNodeName' and return a future of {@link MessageSender} that sends
   * messages with 'messageTypeName'.
   *
   * @param targetNodeName a target node name
   * @param messageTypeName a message type name
   * @param <T> message type
   * @return a message sender
   */
  <T extends Serializable> Future<MessageSender<T>> asyncConnect(String targetNodeName, String messageTypeName);

  /**
   * Return a name of current node.
   *
   * @return a name
   */
  String getCurrentNodeName();

}
