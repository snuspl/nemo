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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;

/**
 * {@link ChannelInitializer} implementation for {@link PartitionTransport}.
 */
final class NettyChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final NettyChannelActiveHandler nettyChannelActiveHandler;

  /**
   * Creates netty channel active handler.
   *
   * @param channelGroup the {@link ChannelGroup} to which active channels are added
   */
  public NettyChannelInitializer(final ChannelGroup channelGroup) {
    nettyChannelActiveHandler = new NettyChannelActiveHandler(channelGroup);
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    // TODO add more handlers
    ch.pipeline()
        .addLast(nettyChannelActiveHandler);
  }

  /**
   * Registers a {@link io.netty.channel.Channel} when it becomes active.
   */
  @ChannelHandler.Sharable
  private static final class NettyChannelActiveHandler extends ChannelInboundHandlerAdapter {

    private final ChannelGroup channelGroup;

    /**
     * Creates netty channel active handler.
     *
     * @param channelGroup the {@link ChannelGroup} to which active channels are added
     */
    public NettyChannelActiveHandler(final ChannelGroup channelGroup) {
      this.channelGroup = channelGroup;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
      channelGroup.add(ctx.channel());
    }
  }
}
