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

import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;

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
 *      +--------------------------+
 *   <= | (DataFrameHeaderEncoder) | <= DataFrame header =+
 *      +--------------------------+                      |== PartitionOutputStream buffer flush
 *   <=== ByteBuf ========================================+
 *
 *      +--------------------------+
 *   <= | (DataFrameHeaderEncoder) | <= DataFrame header =+
 *      +--------------------------+                      |== A FileRegion added to PartitionOutputStream
 *   <= FileRegion =======================================+
 * }
 * </pre>
 */
final class ChannelInitializer extends io.netty.channel.ChannelInitializer<SocketChannel> {

  private static final ControlFrameEncoder CONTROL_FRAME_ENCODER = new ControlFrameEncoder();

  private final ChannelLifecycleTracker channelLifecycleTracker;
  private final String localExecutorId;

  /**
   * Creates a netty channel initializer.
   *
   * @param channelGroup            the {@link ChannelGroup} to which active channels are added
   * @param channelMap              the map to which active channels are added
   * @param localExecutorId         the id of this executor
   */
  ChannelInitializer(final ChannelGroup channelGroup,
                     final ConcurrentMap<SocketAddress, Channel> channelMap,
                     final String localExecutorId) {
    this.channelLifecycleTracker = new ChannelLifecycleTracker(channelGroup, channelMap);
    this.localExecutorId = localExecutorId;
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    ch.pipeline()
        // inbound
        .addLast(new FrameDecoder())
        // outbound
        .addLast(CONTROL_FRAME_ENCODER)
        // duplex
        .addLast(new ControlMessageToPartitionStreamCodec(localExecutorId))
        // channel management
        .addLast(channelLifecycleTracker);
  }

  /**
   * Manages {@link Channel} registration to the channel group and the channel map, and handles exception.
   */
  @ChannelHandler.Sharable
  private static final class ChannelLifecycleTracker extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelLifecycleTracker.class);

    private final ChannelGroup channelGroup;
    private final ConcurrentMap<SocketAddress, Channel> channelMap;

    /**
     * Creates a channel lifecycle handler.
     *
     * @param channelGroup the {@link ChannelGroup} to which active channels are added
     * @param channelMap    the map to which active channels are added
     */
    private ChannelLifecycleTracker(final ChannelGroup channelGroup,
                            final ConcurrentMap<SocketAddress, Channel> channelMap) {
      this.channelGroup = channelGroup;
      this.channelMap = channelMap;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
      channelGroup.add(ctx.channel());
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
      final SocketAddress address = ctx.channel().remoteAddress();
      channelMap.remove(address);
      LOG.warn("A channel with remote address {} is now inactive", address);
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      LOG.error(String.format("Exception caught in the channel to %s", ctx.channel().remoteAddress()), cause);
      ctx.close();
    }
  }
}
