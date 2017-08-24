/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.executor.data.PartitionManagerWorker;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Responses to control message by emitting a new {@link PartitionTransfer.PartitionStream},
 * and responses to {@link PartitionTransfer.PartitionStream} by emitting a new control message.
 *
 * <h3>Type of partition transfer:</h3>
 * <ul>
 *   <li>In push-based transfer, the sender initiates partition transfer and issues transfer id.</li>
 *   <li>In pull-based transfer, the receiver initiates partition transfer and issues transfer id.</li>
 * </ul>
 *
 * @see ChannelInitializer
 */
final class ControlMessageToPartitionStreamCodec
    extends MessageToMessageCodec<ControlMessage.PartitionTransferControlMessage, PartitionTransfer.PartitionStream> {

  private final Map<Short, PartitionInputStream> pullTransferIdToInputStream = new HashMap<>();
  private final Map<Short, PartitionInputStream> pushTransferIdToInputStream = new HashMap<>();
  private final Map<Short, PartitionOutputStream> pullTransferIdToOutputStream = new HashMap<>();
  private final Map<Short, PartitionOutputStream> pushTransferIdToOutputStream = new HashMap<>();

  private final String localExecutorId;
  private final PartitionManagerWorker partitionManagerWorker;

  private short nextOutboundPullTransferId = 0;
  private short nextOutboundPushTransferId = 0;

  /**
   * Creates a {@link ControlMessageToPartitionStreamCodec}.
   *
   * @param localExecutorId         the id of this executor
   * @param partitionManagerWorker  needed to get {@link edu.snu.vortex.common.coder.Coder} from runtimeEdgeId
   */
  ControlMessageToPartitionStreamCodec(final String localExecutorId,
                                       final PartitionManagerWorker partitionManagerWorker) {
    this.localExecutorId = localExecutorId;
    this.partitionManagerWorker = partitionManagerWorker;
  }

  @Override
  protected void encode(final ChannelHandlerContext ctx,
                        final PartitionTransfer.PartitionStream in,
                        final List<Object> out) {
    if (in instanceof PartitionInputStream) {
      onOutboundPullRequest(ctx, (PartitionInputStream) in, out);
    } else {
      onOutboundPushNotification(ctx, (PartitionOutputStream) in, out);
    }
  }

  /**
   * Respond to {@link PartitionInputStream} by emitting outbound pull request.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the {@link PartitionInputStream}
   * @param out the {@link List} into which the created control message is added
   */
  private void onOutboundPullRequest(final ChannelHandlerContext ctx,
                                     final PartitionInputStream in,
                                     final List<Object> out) {
    final short transferId = nextOutboundPullTransferId++;
    pullTransferIdToInputStream.put(transferId, in);
    final ControlMessage.PartitionTransferControlMessage controlMessage
        = ControlMessage.PartitionTransferControlMessage.newBuilder()
        .setControlMessageSourceId(localExecutorId)
        .setType(ControlMessage.PartitionTransferType.PULL)
        .setTransferId(transferId)
        .setPartitionId(in.getPartitionId())
        .setRuntimeEdgeId(in.getRuntimeEdgeId())
        .build();
    out.add(controlMessage);
  }

  /**
   * Respond to {@link PartitionOutputStream} by emitting outbound push notification.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the {@link PartitionOutputStream}
   * @param out the {@link List} into which the created control message is added
   */
  private void onOutboundPushNotification(final ChannelHandlerContext ctx,
                                          final PartitionOutputStream in,
                                          final List<Object> out) {
    final short transferId = nextOutboundPushTransferId++;
    pushTransferIdToOutputStream.put(transferId, in);
    in.setTransferId(ControlMessage.PartitionTransferType.PUSH, transferId);
    final ControlMessage.PartitionTransferControlMessage controlMessage
        = ControlMessage.PartitionTransferControlMessage.newBuilder()
        .setControlMessageSourceId(localExecutorId)
        .setType(ControlMessage.PartitionTransferType.PUSH)
        .setTransferId(transferId)
        .setPartitionId(in.getPartitionId())
        .setRuntimeEdgeId(in.getRuntimeEdgeId())
        .build();
    out.add(controlMessage);
  }

  @Override
  protected void decode(final ChannelHandlerContext ctx,
                        final ControlMessage.PartitionTransferControlMessage in,
                        final List<Object> out) {
    if (in.getType() == ControlMessage.PartitionTransferType.PULL) {
      onInboundPullRequest(ctx, in, out);
    } else {
      onInboundPushNotification(ctx, in, out);
    }
  }

  /**
   * Respond to pull request by other executors by emitting a new {@link PartitionOutputStream}.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the control message
   * @param out the {@link List} into which the created {@link PartitionOutputStream} is added
   */
  private void onInboundPullRequest(final ChannelHandlerContext ctx,
                                    final ControlMessage.PartitionTransferControlMessage in,
                                    final List<Object> out) {
    final short transferId = (short) in.getTransferId();
    final PartitionOutputStream outputStream = new PartitionOutputStream(in.getControlMessageSourceId(),
        in.getPartitionId(), in.getRuntimeEdgeId(), partitionManagerWorker.getCoder(in.getRuntimeEdgeId()));
    pullTransferIdToOutputStream.put(transferId, outputStream);
    outputStream.setTransferId(ControlMessage.PartitionTransferType.PULL, transferId);
    out.add(outputStream);
  }

  /**
   * Respond to push notification by other executors by emitting a new {@link PartitionInputStream}.
   *
   * @param ctx the {@link ChannelHandlerContext} which this handler belongs to
   * @param in  the control message
   * @param out the {@link List} into which the created {@link PartitionInputStream} is added
   */
  private void onInboundPushNotification(final ChannelHandlerContext ctx,
                                         final ControlMessage.PartitionTransferControlMessage in,
                                         final List<Object> out) {
    final short transferId = (short) in.getTransferId();
    final PartitionInputStream inputStream = new PartitionInputStream(in.getControlMessageSourceId(),
        in.getPartitionId(), in.getRuntimeEdgeId(), partitionManagerWorker.getCoder(in.getRuntimeEdgeId()));
    pushTransferIdToInputStream.put(transferId, inputStream);
    out.add(inputStream);
  }
}
