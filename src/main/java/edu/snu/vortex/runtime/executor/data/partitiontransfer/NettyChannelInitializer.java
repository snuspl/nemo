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
 * {@link ChannelInitializer} implementation for {@link PartitionTransport}.
 */
final class NettyChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final NettyChannelActiveHandler nettyChannelActiveHandler;

  /**
   * Creates netty channel active handler.
   *
   * Inbound pipeline:
   * <pre>
   *                                           +-----------------------+
   *                            |== Control => | ControlMessageHandler | ==> (PartitionTransferRequest)
   *       +----------------+   |              +-----------------------+
   *   ==> | MessageDecoder | ==|
   *       +----------------+   |              +---------------------+
   *                            |=== Data ===> | RequestReplyMatcher | ==> fetch response
   *                                           +---------------------+
   * </pre>
   *
   * Outbound pipeline:
   * <pre>
   *       +-----------------------+     +---------------------+
   *   <== | ControlMessageEncoder | <== | RequestReplyMatcher | <== fetch request
   *       +-----------------------+     +---------------------+
   *          +--------------------+
   *   <===== | DataMessageEncoder | <== PartitionOutputStream (for MemoryPartition) <== (PartitionTransferRequest)
   *          +--------------------+        or PartitionRegion (for FilePartition)
   * </pre>
   *
   * @param channelGroup  the {@link ChannelGroup} to which active channels are added
   * @param channelMap    the map to which active channels are added
   */
  NettyChannelInitializer(final ChannelGroup channelGroup,
                          final ConcurrentMap<SocketAddress, Channel> channelMap) {
    nettyChannelActiveHandler = new NettyChannelActiveHandler(channelGroup, channelMap);
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    // TODO add more handlers
    ch.pipeline()
        // inbound
        .addLast(new MessageDecoder())

        // outbound

        // duplex

        // channel management
        .addLast(nettyChannelActiveHandler);
  }

  /**
   * Registers a {@link Channel} to the channel group and the channel map when it becomes active.
   */
  @ChannelHandler.Sharable
  private static final class NettyChannelActiveHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(NettyChannelActiveHandler.class);

    private final ChannelGroup channelGroup;
    private final ConcurrentMap<SocketAddress, Channel> channelMap;

    /**
     * Creates netty channel active handler.
     *
     * @param channelGroup the {@link ChannelGroup} to which active channels are added
     * @param channelMap    the map to which active channels are added
     */
    NettyChannelActiveHandler(final ChannelGroup channelGroup,
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
