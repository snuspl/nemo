package edu.snu.vortex.runtime.common.message.ncs;

import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.common.message.MessageSender;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.concurrent.Future;

public class NcsMessageEnvironment implements MessageEnvironment {

  private final NetworkConnectionService networkConnectionService;
  private final String senderId;

  @Inject
  private NcsMessageEnvironment(
      final NetworkConnectionService networkConnectionService,
      @Parameter(NcsParameters.SenderId.class)final String senderId,
      final IdentifierFactory identifierFactory) {
    this.networkConnectionService = networkConnectionService;
    this.senderId = senderId;
    identifierFactory.getNewInstance(senderId);
  }

  @Override
  public <T> MessageSender<T> setupListener(final String messageTypeId, final MessageListener<T> listener) {
    return null;
  }

  @Override
  public <T> Future<MessageSender<T>> asyncConnect(final String receiverId, final String messageTypeId) {
    return null;
  }

  @Override
  public String getId() {
    return senderId;
  }
}
