package edu.snu.vortex.runtime.common.message;

import java.io.Serializable;
import java.util.concurrent.Future;

/**
 * Set up {@link MessageListener}s to handle incoming messages on this node, and connect to remote nodes and return
 * {@link MessageSender}s to send message to them.
 */
public interface MessageEnvironment {

  /**
   * Set up a {@link MessageListener} with a name.
   *
   * @param name name of the message listener
   * @param listener a message listener
   * @param <T> message type
   * @return a message sender to the locally set up listener.
   */
  <T extends Serializable> MessageSender<T> setupListener(String name, MessageListener<T> listener);

  /**
   * Asynchronously connect to the {@link MessageAddress} and return a future.
   *
   * @param targetAddress a target address
   * @param <T> message type
   * @return a message sender
   */
  <T extends Serializable> Future<MessageSender<T>> asyncConnect(MessageAddress targetAddress);

  /**
   * Return an endpoint of current node.
   *
   * @return an endpoint
   */
  EndpointAddress getCurrentAddress();

}
