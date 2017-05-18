package edu.snu.vortex.runtime.common.message.ncs;

import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageSender;
import org.apache.reef.io.network.Connection;

import java.util.concurrent.Future;

/**
 * MessageSender for NCS.
 */
final class NcsMessageSender implements MessageSender<ControlMessage.Message> {

  private final Connection<ControlMessage.Message> connection;
  private final ReplyWaitingLock replyWaitingLock;

  NcsMessageSender(
      final Connection<ControlMessage.Message> connection,
      final ReplyWaitingLock replyWaitingLock) {
    this.connection = connection;
    this.replyWaitingLock = replyWaitingLock;
  }

  @Override
  public void send(final ControlMessage.Message message) {
    connection.write(message);
  }

  @Override
  public <U> Future<U> request(final ControlMessage.Message message) {
    connection.write(message);
    return (Future) replyWaitingLock.waitingReply(message.getId());
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }
}
