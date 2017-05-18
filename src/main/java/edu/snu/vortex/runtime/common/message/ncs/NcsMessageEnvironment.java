package edu.snu.vortex.runtime.common.message.ncs;

import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageContext;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.common.message.MessageSender;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

/**
 * Message environment for NCS.
 */
public final class NcsMessageEnvironment implements MessageEnvironment {

  private static final String NCS_CONN_FACTORY_ID = "NCS_CONN_FACTORY_ID";

  private final NetworkConnectionService networkConnectionService;
  private final IdentifierFactory idFactory;
  private final String senderId;

  private final ReplyWaitingLock replyWaitingLock;
  private final ConcurrentMap<String, MessageListener> listenerConcurrentMap;
  private final ConnectionFactory<ControlMessage.Message> connectionFactory;


  @Inject
  private NcsMessageEnvironment(
      final NetworkConnectionService networkConnectionService,
      final IdentifierFactory idFactory,
      @Parameter(NcsParameters.SenderId.class) final String senderId) {
    this.networkConnectionService = networkConnectionService;
    this.idFactory = idFactory;
    this.senderId = senderId;
    this.replyWaitingLock = new ReplyWaitingLock();
    this.listenerConcurrentMap = new ConcurrentHashMap<>();
    this.connectionFactory = networkConnectionService.registerConnectionFactory(
        idFactory.getNewInstance(NCS_CONN_FACTORY_ID),
        new ControlMessageCodec(),
        new NcsMessageHandler(),
        new NcsLinkListener(),
        idFactory.getNewInstance(senderId));
  }

  @Override
  public <T> void setupListener(final String messageTypeId, final MessageListener<T> listener) {
    if (listenerConcurrentMap.putIfAbsent(messageTypeId, listener) != null) {
      throw new RuntimeException("A listener for " + messageTypeId + " was already setup");
    }
  }

  @Override
  public <T> Future<MessageSender<T>> asyncConnect(final String receiverId, final String messageTypeId) {
    try {
      final Connection<ControlMessage.Message> connection = connectionFactory.newConnection(
          idFactory.getNewInstance(receiverId));
      connection.open();
      return CompletableFuture.completedFuture((MessageSender) new NcsMessageSender(connection, replyWaitingLock));
    } catch (final Exception e) {
      final CompletableFuture<MessageSender<T>> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(e);
      return failedFuture;
    }
  }

  @Override
  public String getId() {
    return senderId;
  }

  @Override
  public void close() throws Exception {
    networkConnectionService.close();
  }

  /**
   * Message handler for NCS.
   */
  private final class NcsMessageHandler implements EventHandler<Message<ControlMessage.Message>> {

    public void onNext(final Message<ControlMessage.Message> messages) {
      final ControlMessage.Message controlMessage = extractSingleMessage(messages);
      final boolean isSendMessage = getIsSendMessage(controlMessage);
      if (isSendMessage) {
        processSendMessage(controlMessage);
        return;
      }

      final boolean isRequestMessage = getIsRequestMessage(controlMessage);
      if (isRequestMessage) {
        processRequestMessage(controlMessage);
        return;
      }

      final boolean isReplyMessage = getIsReplyMessage(controlMessage);
      if (isReplyMessage) {
        processReplyMessage(controlMessage);
      }
    }

    private void processSendMessage(final ControlMessage.Message controlMessage) {
      final String messageType = getMessageType(controlMessage);
      listenerConcurrentMap.get(messageType).onMessage(controlMessage);
    }

    private void processRequestMessage(final ControlMessage.Message controlMessage) {
      final String messageType = getMessageType(controlMessage);
      final String executorId = getExecutorId(controlMessage);
      final MessageContext messageContext = new NcsMessageContext(executorId, connectionFactory, idFactory);
      listenerConcurrentMap.get(messageType).onMessageWithContext(controlMessage, messageContext);
    }

    private void processReplyMessage(final ControlMessage.Message controlMessage) {
      replyWaitingLock.onSuccessMessage(controlMessage);
    }
  }

  /**
   * LinkListener for NCS.
   */
  private final class NcsLinkListener implements LinkListener<Message<ControlMessage.Message>> {

    public void onSuccess(final Message<ControlMessage.Message> messages) {
      // No-ops.
    }

    public void onException(final Throwable throwable,
                            final SocketAddress socketAddress,
                            final Message<ControlMessage.Message> messages) {
      final ControlMessage.Message controlMessage = extractSingleMessage(messages);
      if (getIsReplyMessage(controlMessage)) {
        throw new RuntimeException("An exception occurred with replying a message " + controlMessage.getType().name(),
            throwable);
      }

      final boolean isRequestMessage = getIsRequestMessage(controlMessage);
      if (!isRequestMessage) {
        return;
      }

      replyWaitingLock.onFailedMessage(controlMessage, throwable);
    }
  }

  private ControlMessage.Message extractSingleMessage(final Message<ControlMessage.Message> messages) {
    return messages.getData().iterator().next();
  }

  private boolean getIsSendMessage(final ControlMessage.Message controlMessage) {
    return true;
  }

  private boolean getIsRequestMessage(final ControlMessage.Message controlMessage) {
    return true;
  }

  private boolean getIsReplyMessage(final ControlMessage.Message controlMessage) {
    return true;
  }

  private String getMessageType(final ControlMessage.Message controlMessage) {
    return "";
  }

  private String getExecutorId(final ControlMessage.Message controlMessage) {
    return "";
  }
}
