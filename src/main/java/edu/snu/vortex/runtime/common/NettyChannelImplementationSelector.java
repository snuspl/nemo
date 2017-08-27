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
import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.inject.Inject;
import java.util.function.Function;

/**
 * Selects appropriate {@link io.netty.channel.Channel} implementation, depending on implementation availabilities.
 * Uses {@link Epoll} if possible (on Linux).
 */
@DefaultImplementation(NettyChannelImplementationSelector.NativeChannelImplementationSelector.class)
public interface NettyChannelImplementationSelector {

  /**
   * Creates a new {@link EventLoopGroup}.
   * @param numThreads the number of threads
   * @return a new {@link EventLoopGroup}
   */
  EventLoopGroup newEventLoopGroup(int numThreads);

  /**
   * @return class for server channel
   */
  Class<? extends ServerChannel> getServerChannelClass();

  /**
   * @return class for channel
   */
  Class<? extends Channel> getChannelClass();

  /**
   * A {@link NettyChannelImplementationSelector} implementation that prefers native transport if possible.
   * Uses {@link Epoll} if possible (on Linux).
   */
  final class NativeChannelImplementationSelector implements NettyChannelImplementationSelector {

    /**
     * Private constructor.
     */
    @Inject
    private NativeChannelImplementationSelector() {
    }

    // We may want to add selection of KQueue (for BSD). This requires higher version of netty.

    private static final Function<Integer, EventLoopGroup> EVENT_LOOP_GROUP_FUNCTION =
        Epoll.isAvailable() ? numThreads -> new EpollEventLoopGroup(numThreads)
        : numThreads -> new NioEventLoopGroup(numThreads);
    private static final Class<? extends ServerChannel> SERVER_CHANNEL_CLASS =
        Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
    private static final Class<? extends Channel> CHANNEL_CLASS =
        Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class;

    @Override
    public EventLoopGroup newEventLoopGroup(final int numThreads) {
      return EVENT_LOOP_GROUP_FUNCTION.apply(numThreads);
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
   * A {@link NettyChannelImplementationSelector} implementation that prefers nio-based channel implementation.
   */
  final class NioChannelImplementationSelector implements NettyChannelImplementationSelector {

    /**
     * Private constructor.
     */
    @Inject
    private NioChannelImplementationSelector() {
    }

    @Override
    public EventLoopGroup newEventLoopGroup(final int numThreads) {
      return new NioEventLoopGroup(numThreads);
    }

    @Override
    public Class<? extends ServerChannel> getServerChannelClass() {
      return NioServerSocketChannel.class;
    }

    @Override
    public Class<? extends Channel> getChannelClass() {
      return NioSocketChannel.class;
    }
  }
}
