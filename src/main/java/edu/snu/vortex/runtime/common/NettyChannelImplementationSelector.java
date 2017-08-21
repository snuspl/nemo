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
package edu.snu.vortex.runtime.common;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.function.Function;

/**
 * Selects appropriate {@link io.netty.channel.Channel} implementation, depending on implementation availabilities.
 * Uses {@link Epoll} if possible (on Linux).
 */
public final class NettyChannelImplementationSelector {

  /**
   * Private constructor.
   */
  private NettyChannelImplementationSelector() {
  }

  // We may want to add selection of KQueue (for BSD)

  /**
   * {@link Function} that takes the number of threads and returns a new {@link EventLoopGroup} instance.
   */
  public static final Function<Integer, EventLoopGroup> EVENT_LOOP_GROUP_FUNCTION =
      Epoll.isAvailable() ? numThreads -> new EpollEventLoopGroup(numThreads)
          : numThreads -> new NioEventLoopGroup(numThreads);

  /**
   * Selects {@link ServerChannel} implementation.
   */
  public static final Class<? extends ServerChannel> SERVER_CHANNEL_CLASS =
      Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class;

  /**
   * Selects {@link Channel} implementation.
   */
  public static final Class<? extends Channel> CHANNEL_CLASS =
      Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class;
}
