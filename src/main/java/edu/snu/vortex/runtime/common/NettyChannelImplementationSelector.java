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
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.function.Supplier;

/**
 * Selects appropriate {@link io.netty.channel.Channel} implementation, depending on implementation availabilities.
 */
@DefaultImplementation(NettyChannelImplementationSelector.NetworkChannelImplementationSelector.class)
public interface NettyChannelImplementationSelector {
  /**
   * Constructs a new {@link EventLoopGroup} instance.
   *
   * @return a new {@link EventLoopGroup} instance
   */
  EventLoopGroup newEventLoopGroup();

  /**
   * Selects {@link ServerChannel} implementation.
   *
   * @return {@link Class} of {@link ServerChannel}
   */
  Class<? extends ServerChannel> getServerChannelClass();

  /**
   * Selects {@link Channel} implementation.
   *
   * @return {@link Class} of {@link Channel}
   */
  Class<? extends Channel> getChannelClass();

  /**
   * A {@link NettyChannelImplementationSelector} implementation for communication between nodes.
   * Uses {@link Epoll} if possible (on Linux).
   */
  class NetworkChannelImplementationSelector implements NettyChannelImplementationSelector {

    private static final Supplier<EventLoopGroup> EVENT_LOOP_GROUP_SUPPLIER =
        Epoll.isAvailable() ? () -> new EpollEventLoopGroup() : () -> new NioEventLoopGroup();
    private static final Class<? extends ServerChannel> SERVER_CHANNEL_CLASS =
        Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
    private static final Class<? extends Channel> CHANNEL_CLASS =
        Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class;

    @Override
    public EventLoopGroup newEventLoopGroup() {
      return EVENT_LOOP_GROUP_SUPPLIER.get();
    }

    @Override
    public Class<? extends ServerChannel> getServerChannelClass() {
      return SERVER_CHANNEL_CLASS;
    }

    @Override
    public Class<? extends Channel> getChannelClass() {
      return CHANNEL_CLASS;
    }
  }

  /**
   * A {@link NettyChannelImplementationSelector} implementation for local communication and unit tests.
   */
  class LocalChannelImplementationSelector implements NettyChannelImplementationSelector {

    @Override
    public EventLoopGroup newEventLoopGroup() {
      return new LocalEventLoopGroup();
    }

    @Override
    public Class<? extends ServerChannel> getServerChannelClass() {
      return LocalServerChannel.class;
    }

    @Override
    public Class<? extends Channel> getChannelClass() {
      return LocalChannel.class;
    }
  }
}
