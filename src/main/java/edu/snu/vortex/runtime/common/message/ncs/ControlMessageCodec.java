package edu.snu.vortex.runtime.common.message.ncs;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;

public class ControlMessageCodec implements Codec<ControlMessage.Message>,
    org.apache.reef.wake.remote.Codec<ControlMessage.Message> {

  @Inject
  private ControlMessageCodec() {
  }

  @Override
  public byte[] encode(final ControlMessage.Message obj) {
    return obj.toByteArray();
  }

  @Override
  public ControlMessage.Message decode(byte[] buf) {
    try {
      return ControlMessage.Message.parseFrom(buf);
    } catch (final InvalidProtocolBufferException e) {
      throw new Runtim
    }
  }
}
