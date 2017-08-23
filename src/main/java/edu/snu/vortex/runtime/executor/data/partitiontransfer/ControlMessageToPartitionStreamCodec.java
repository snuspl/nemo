package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import io.netty.handler.codec.MessageToMessageCodec;

final class ControlMessageToPartitionStreamCodec
    extends MessageToMessageCodec<PartitionTransferControlFrame, PartitionStream> {

  interface PartitionStream {

  }
}
