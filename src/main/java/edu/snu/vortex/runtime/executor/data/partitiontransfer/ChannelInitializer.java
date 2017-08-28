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
import org.apache.reef.tang.InjectionFuture;
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

  private static final OutboundExceptionHandler OUTBOUND_EXCEPTION_HANDLER = new OutboundExceptionHandler();
  private static final ControlFrameEncoder CONTROL_FRAME_ENCODER = new ControlFrameEncoder();
  private static final DataFrameEncoder DATA_FRAME_ENCODER = new DataFrameEncoder();

  private final ChannelLifecycleTracker channelLifecycleTracker;
  private final InjectionFuture<PartitionTransfer> partitionTransfer;
  private final String localExecutorId;

  /**
   * Creates a netty channel initializer.
   *
   * @param channelGroup      the {@link ChannelGroup} to which active channels are added
   * @param channelFutureMap  the map to which promises for connections are added
   * @param partitionTransfer provides handler for inbound control messages
   * @param localExecutorId   the id of this executor
   */
  ChannelInitializer(final ChannelGroup channelGroup,
                     final ConcurrentMap<SocketAddress, ChannelFuture> channelFutureMap,
                     final InjectionFuture<PartitionTransfer> partitionTransfer,
                     final String localExecutorId) {
    this.channelLifecycleTracker = new ChannelLifecycleTracker(channelGroup, channelFutureMap);
    this.partitionTransfer = partitionTransfer;
    this.localExecutorId = localExecutorId;
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    ch.pipeline()
        // channel management
        .addLast(OUTBOUND_EXCEPTION_HANDLER)
        // inbound
        .addLast(new FrameDecoder())
        // outbound
        .addLast(CONTROL_FRAME_ENCODER)
        .addLast(DATA_FRAME_ENCODER)
        // duplex
        .addLast(new ControlMessageToPartitionStreamCodec(localExecutorId))
        // inbound
        .addLast(partitionTransfer.get())
        // channel management
        .addLast(channelLifecycleTracker);
  }

  /**
   * Manages {@link Channel} registration to the channel group and the channel map, and handles inbound exception.
   */
  @ChannelHandler.Sharable
  private static final class ChannelLifecycleTracker extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelLifecycleTracker.class);

    private final ChannelGroup channelGroup;
    private final ConcurrentMap<SocketAddress, ChannelFuture> channelFutureMap;

    /**
     * Creates a channel lifecycle handler.
     *
     * @param channelGroup      the {@link ChannelGroup} to which active channels are added
     * @param channelFutureMap  the map to which promises for connections are added
     */
    private ChannelLifecycleTracker(final ChannelGroup channelGroup,
                                    final ConcurrentMap<SocketAddress, ChannelFuture> channelFutureMap) {
      this.channelGroup = channelGroup;
      this.channelFutureMap = channelFutureMap;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
      channelGroup.add(ctx.channel());
      channelFutureMap.compute(ctx.channel().remoteAddress(), (address, future) -> {
        if (future == null) {
          // probably a channel which this executor has not initiated (i.e. a remote executor connected to this server)
          return ctx.channel().newSucceededFuture();
        } else if (future.channel() == ctx.channel()) {
          // leave unchanged
          return future;
        } else {
          LOG.warn("A channel to {} is active while another channel to the same remote address is in the cache",
              address);
          return ctx.channel().newSucceededFuture();
        }
      });
      LOG.debug("A channel with local address {} and remote address {} is now active",
          ctx.channel().localAddress(), ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
      final SocketAddress address = ctx.channel().remoteAddress();
      channelFutureMap.remove(address);
      LOG.warn("A channel with local address {} and remote address {} is now inactive",
          ctx.channel().localAddress(), address);
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      LOG.error(String.format("An inbound exception caught in the channel with local address %s and"
          + "remote address %s", ctx.channel().localAddress(), ctx.channel().remoteAddress()), cause);
      ctx.close();
    }
  }

  /**
   * Handles outbound exception.
   */
  @ChannelHandler.Sharable
  private static final class OutboundExceptionHandler extends ChannelOutboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(OutboundExceptionHandler.class);

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
      LOG.error(String.format("An outbound exception caught in the channel with local address %s and"
          + "remote address %s", ctx.channel().localAddress(), ctx.channel().remoteAddress()), cause);
      ctx.close();
    }
  }
}
