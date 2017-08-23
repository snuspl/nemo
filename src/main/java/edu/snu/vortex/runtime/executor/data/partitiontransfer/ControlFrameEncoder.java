package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.MessageToMessageEncoder;

/**
 * Encodes a control frame into bytes.
 *
 * @see FrameDecoder for the frame specification.
 */
@ChannelHandler.Sharable
final class ControlFrameEncoder extends MessageToMessageEncoder<PartitionTransferControlFrame> {
}
