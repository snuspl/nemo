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
 * Inbound pipeline:
 * <pre>
 *                                         Pull       +--------------------------------------+    A new
 *                                    +== request ==> | ControlMessageToPartitionStreamCodec | => PartitionOutputStream
 *                                    |               +--------------------------------------+
 *                        += Control =|
 *      +--------------+  |           |               +--------------------------------------+
 *   => | FrameDecoder | =|           += Push      => | ControlMessageToPartitionStreamCodec | => A new
 *      +--------------+  |             notification  +--------------------------------------+    PartitionInputStream
 *                        |
 *                        += Data ====================> Add data to an existing PartitionInputStream
 * </pre>
 *
 * Outbound pipeline:
 * <pre>
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
 * </pre>
 */
final class ChannelInitializer extends io.netty.channel.ChannelInitializer<SocketChannel> {

  private final ChannelActiveHandler channelActiveHandler;

  /**
   * Creates a netty channel initializer.
   *
   * @param channelGroup  the {@link ChannelGroup} to which active channels are added
   * @param channelMap    the map to which active channels are added
   */
  ChannelInitializer(final ChannelGroup channelGroup,
                     final ConcurrentMap<SocketAddress, Channel> channelMap) {
    channelActiveHandler = new ChannelActiveHandler(channelGroup, channelMap);
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    // TODO add more handlers
    // TODO ControlFrameEncoder is sharable! create just once
    // TODO DataFrameHeaderEncoder is not a netty encoder. just a static function wrapper class.
    ch.pipeline()
        // inbound
        .addLast(FrameDecoder.class.getName(), new FrameDecoder())

        // outbound

        // duplex

        // channel management
        .addLast(ChannelActiveHandler.class.getName(), channelActiveHandler);
  }

  /**
   * Registers a {@link Channel} to the channel group and the channel map when it becomes active.
   */
  @ChannelHandler.Sharable
  private static final class ChannelActiveHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelActiveHandler.class);

    private final ChannelGroup channelGroup;
    private final ConcurrentMap<SocketAddress, Channel> channelMap;

    /**
     * Creates a netty channel active handler.
     *
     * @param channelGroup the {@link ChannelGroup} to which active channels are added
     * @param channelMap    the map to which active channels are added
     */
    ChannelActiveHandler(final ChannelGroup channelGroup,
                         final ConcurrentMap<SocketAddress, Channel> channelMap) {
      this.channelGroup = channelGroup;
      this.channelMap = channelMap;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
      final Channel channel = ctx.channel();
      channelGroup.add(channel);
      if (channelMap.put(channel.remoteAddress(), channel) != null) {
        LOG.warn("Multiple channels with remote address {} are active", channel.remoteAddress());
      }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
      final Channel channel = ctx.channel();
      LOG.warn("A channel with remote address {} is now inactive", channel.remoteAddress());
      channelMap.computeIfPresent(channel.remoteAddress(), (address, mappedChannel) -> {
        if (mappedChannel == channel) {
          // If the inactive channel is in the map, remove it
          return null;
        } else {
          // Otherwise, leave the map untouched
          return mappedChannel;
        }
      });
    }
  }
}
