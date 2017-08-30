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

import edu.snu.vortex.client.JobConf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.SocketChannel;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Sets up {@link io.netty.channel.ChannelPipeline} for {@link PartitionTransport}.
 *
 * <h3>Inbound pipeline:</h3>
 * <pre>
 * {@literal
 *                                         Pull       +--------------------------------------+    A new
 *                                    +== request ==> | ControlMessageToPartitionStreamCodec | => PartitionOutputStream
 *                                    |               +--------------------------------------+
 *                        += Control =|
 *      +--------------+  |           |               +--------------------------------------+
 *   => | FrameDecoder | =|           += Push      => | ControlMessageToPartitionStreamCodec | => A new
 *      +--------------+  |             notification  +--------------------------------------+    PartitionInputStream
 *                        |
 *                        += Data ====================> Add data to an existing PartitionInputStream
 * }
 * </pre>
 *
 * <h3>Outbound pipeline:</h3>
 * <pre>
 * {@literal
 *      +---------------------+                   +--------------------------------------+    Pull request with a
 *   <= | ControlFrameEncoder | <= Pull request = | ControlMessageToPartitionStreamCodec | <= new PartitionInputStream
 *      +---------------------+                   +--------------------------------------+
 *      +---------------------+    Push           +--------------------------------------+    Push notification with a
 *   <= | ControlFrameEncoder | <= notification = | ControlMessageToPartitionStreamCodec | <= new PartitionOutputStream
 *      +---------------------+                   +--------------------------------------+
 *
 *      +------------------+
 *   <= | DataFrameEncoder | <=== ByteBuf === PartitionOutputStream buffer flush
 *      +------------------+
 *      +------------------+
 *   <= | DataFrameEncoder | <== FileArea === A FileArea added to PartitionOutputStream
 *      +------------------+
 * }
 * </pre>
 */
final class ChannelInitializer extends io.netty.channel.ChannelInitializer<SocketChannel> {

  private final InjectionFuture<PartitionTransfer> partitionTransfer;
  private final ControlFrameEncoder controlFrameEncoder;
  private final DataFrameEncoder dataFrameEncoder;
  private final String localExecutorId;

  /**
   * Creates a netty channel initializer.
   *
   * @param partitionTransfer   provides handler for inbound control messages
   * @param controlFrameEncoder encodes control frames
   * @param dataFrameEncoder    encodes data frames
   * @param localExecutorId     the id of this executor
   */
  @Inject
  private ChannelInitializer(final InjectionFuture<PartitionTransfer> partitionTransfer,
                             final ControlFrameEncoder controlFrameEncoder,
                             final DataFrameEncoder dataFrameEncoder,
                             @Parameter(JobConf.ExecutorId.class) final String localExecutorId) {
    this.partitionTransfer = partitionTransfer;
    this.controlFrameEncoder = controlFrameEncoder;
    this.dataFrameEncoder = dataFrameEncoder;
    this.localExecutorId = localExecutorId;
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    ch.pipeline()
        // management
        .addLast(new ChannelActivationReporter())
        // inbound
        .addLast(new FrameDecoder())
        // outbound
        .addLast(controlFrameEncoder)
        .addLast(dataFrameEncoder)
        // both
        .addLast(new ControlMessageToPartitionStreamCodec(localExecutorId))
        // inbound
        .addLast(partitionTransfer.get());
  }

  /**
   * Reports channel activation event to {@link PartitionTransfer}.
   *
   * Channels should be cached by {@link PartitionTransfer} for better performance.
   * {@link PartitionTransfer} watches inbound control messages and caches the corresponding channel automatically.
   * However, channels with no inbound control messages (i.e. this executor only has served as a client), channels are
   * not going to be cached. This handler emits a user event that forces channel caching.
   */
  private static final class ChannelActivationReporter extends ChannelInboundHandlerAdapter {
    private String localExecutorId = null;
    private String remoteExecutorId = null;

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) {
      if (evt instanceof SetExecutorIdEvent) {
        final SetExecutorIdEvent event = (SetExecutorIdEvent) evt;
        localExecutorId = event.getLocalExecutorId();
        remoteExecutorId = event.getRemoteExecutorId();
        if (ctx.channel().isActive()) {
          ctx.fireUserEventTriggered(new ChannelActiveEvent(localExecutorId, remoteExecutorId));
        }
      } else {
        ctx.fireUserEventTriggered(evt);
      }
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
      if (remoteExecutorId != null) {
        ctx.fireUserEventTriggered(new ChannelActiveEvent(localExecutorId, remoteExecutorId));
      }
      ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
      if (remoteExecutorId != null) {
        ctx.fireUserEventTriggered(new ChannelInactiveEvent(localExecutorId, remoteExecutorId));
      }
      ctx.fireChannelInactive();
    }
  }

  /**
   * Event for setting executor id.
   */
  static final class SetExecutorIdEvent {
    private final String localExecutorId;
    private final String remoteExecutorId;

    SetExecutorIdEvent(final String localExecutorId, final String remoteExecutorId) {
      this.localExecutorId = localExecutorId;
      this.remoteExecutorId = remoteExecutorId;
    }

    String getLocalExecutorId() {
      return localExecutorId;
    }

    String getRemoteExecutorId() {
      return remoteExecutorId;
    }
  }

  /**
   * Event for channel activation.
   */
  static final class ChannelActiveEvent {
    private final String localExecutorId;
    private final String remoteExecutorId;

    ChannelActiveEvent(final String localExecutorId, final String remoteExecutorId) {
      this.localExecutorId = localExecutorId;
      this.remoteExecutorId = remoteExecutorId;
    }

    String getLocalExecutorId() {
      return localExecutorId;
    }

    String getRemoteExecutorId() {
      return remoteExecutorId;
    }
  }


  /**
   * Event for channel deactivation.
   */
  static final class ChannelInactiveEvent {
    private final String localExecutorId;
    private final String remoteExecutorId;

    ChannelInactiveEvent(final String localExecutorId, final String remoteExecutorId) {
      this.localExecutorId = localExecutorId;
      this.remoteExecutorId = remoteExecutorId;
    }

    String getLocalExecutorId() {
      return localExecutorId;
    }

    String getRemoteExecutorId() {
      return remoteExecutorId;
    }
  }
}
