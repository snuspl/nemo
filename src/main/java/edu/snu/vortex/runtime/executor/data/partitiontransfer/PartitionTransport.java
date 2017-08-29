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
import edu.snu.vortex.runtime.common.NettyChannelImplementationSelector;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.ports.TcpPortProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Transport implementation for peer-to-peer {@link edu.snu.vortex.runtime.executor.data.partition.Partition} transfer.
 */
final class PartitionTransport implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionTransport.class);
  private static final String SERVER_LISTENING = "partition:server:listening";
  private static final String SERVER_WORKING = "partition:server:working";
  private static final String CLIENT = "partition:client";

  private final InetSocketAddress serverListeningAddress;
  private final EventLoopGroup serverListeningGroup;
  private final EventLoopGroup serverWorkingGroup;
  private final EventLoopGroup clientGroup;
  private final Bootstrap clientBootstrap;
  private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private final ConcurrentMap<SocketAddress, ChannelFuture> channelFutureMap = new ConcurrentHashMap<>();

  /**
   * Constructs a partition transport and starts listening.
   *
   * @param partitionTransfer     provides handler for inbound control messages
   * @param localExecutorId       the id of this executor
   * @param channelImplSelector   provides implementation for netty channel
   * @param tcpPortProvider       provides an iterator of random tcp ports
   * @param localAddressProvider  provides the local address of the node to bind to
   * @param port                  the listening port; 0 means random assign using {@code tcpPortProvider}
   * @param serverBacklog         the maximum number of pending connections to the server
   * @param numListeningThreads   the number of listening threads of the server
   * @param numWorkingThreads     the number of working threads of the server
   * @param numClientThreads      the number of client threads
   */
  @Inject
  private PartitionTransport(
      final InjectionFuture<PartitionTransfer> partitionTransfer,
      @Parameter(JobConf.ExecutorId.class) final String localExecutorId,
      final NettyChannelImplementationSelector channelImplSelector,
      final TcpPortProvider tcpPortProvider,
      final LocalAddressProvider localAddressProvider,
      @Parameter(JobConf.PartitionTransportServerPort.class) final int port,
      @Parameter(JobConf.PartitionTransportServerBacklog.class) final int serverBacklog,
      @Parameter(JobConf.PartitionTransportServerNumListeningThreads.class) final int numListeningThreads,
      @Parameter(JobConf.PartitionTransportServerNumWorkingThreads.class) final int numWorkingThreads,
      @Parameter(JobConf.PartitionTransportClientNumThreads.class) final int numClientThreads) {

    if (port < 0) {
      throw new IllegalArgumentException(String.format("Invalid PartitionTransportPort: %d", port));
    }

    final String host = localAddressProvider.getLocalAddress();

    serverListeningGroup = channelImplSelector.newEventLoopGroup(numListeningThreads,
        new DefaultThreadFactory(SERVER_LISTENING));
    serverWorkingGroup = channelImplSelector.newEventLoopGroup(numWorkingThreads,
        new DefaultThreadFactory(SERVER_WORKING));
    clientGroup = channelImplSelector.newEventLoopGroup(numClientThreads, new DefaultThreadFactory(CLIENT));

    final ChannelInitializer channelInitializer
        = new ChannelInitializer(channelGroup, channelFutureMap, partitionTransfer, localExecutorId);

    clientBootstrap = new Bootstrap();
    clientBootstrap
        .group(clientGroup)
        .channel(channelImplSelector.getChannelClass())
        .handler(channelInitializer)
        .option(ChannelOption.SO_REUSEADDR, true);

    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap
        .group(serverListeningGroup, serverWorkingGroup)
        .channel(channelImplSelector.getServerChannelClass())
        .childHandler(channelInitializer)
        .option(ChannelOption.SO_BACKLOG, serverBacklog)
        .option(ChannelOption.SO_REUSEADDR, true);

    Channel listeningChannel = null;
    if (port == 0) {
      for (final int candidatePort : tcpPortProvider) {
        try {
          listeningChannel = serverBootstrap.bind(host, candidatePort).sync().channel();
          channelGroup.add(listeningChannel);
          break;
        } catch (final InterruptedException e) {
          LOG.debug(String.format("Cannot bind to %s:%d", host, candidatePort), e);
        }
      }
      if (listeningChannel == null) {
        serverListeningGroup.shutdownGracefully();
        serverWorkingGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
        throw new RuntimeException(String.format("Cannot bind to %s with tcpPortProvider", host));
      }
    } else {
      try {
        listeningChannel = serverBootstrap.bind(host, port).sync().channel();
        channelGroup.add(listeningChannel);
      } catch (final InterruptedException e) {
        serverListeningGroup.shutdownGracefully();
        serverWorkingGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
        throw new RuntimeException(String.format("Cannot bind to %s:%d", host, port), e);
      }
    }

    serverListeningAddress = (InetSocketAddress) listeningChannel.localAddress();
    LOG.info("PartitionTransport server in {} is listening at {}", localExecutorId, serverListeningAddress);
  }

  /**
   * Closes all channels and releases all resources.
   */
  @Override
  public void close() {
    LOG.info("Stopping listening at {} and closing", serverListeningAddress);

    final ChannelGroupFuture channelGroupCloseFuture = channelGroup.close();
    final Future serverListeningGroupCloseFuture = serverListeningGroup.shutdownGracefully();
    final Future serverWorkingGroupCloseFuture = serverWorkingGroup.shutdownGracefully();
    final Future clientGroupCloseFuture = clientGroup.shutdownGracefully();

    channelGroupCloseFuture.awaitUninterruptibly();
    serverListeningGroupCloseFuture.awaitUninterruptibly();
    serverWorkingGroupCloseFuture.awaitUninterruptibly();
    clientGroupCloseFuture.awaitUninterruptibly();
  }

  /**
   * Gets server listening address.
   *
   * @return server listening address
   */
  InetSocketAddress getServerListeningAddress() {
    return serverListeningAddress;
  }

  /**
   * Writes to the specified remote address. Creates a connection to the remote transport if needed.
   *
   * @param remoteAddress the socket address to write to
   * @param thing         the object to write
   * @param onError       the {@link Consumer} to be invoked on an error during setting up a channel
   *                      or writing to the channel
   */
  void writeTo(final SocketAddress remoteAddress, final Object thing, final Consumer<Throwable> onError) {
    final ChannelFuture channelFuture = channelFutureMap
        .computeIfAbsent(remoteAddress, address -> clientBootstrap.connect(address));
    final Channel channel = channelFuture.channel();
    if (channelFuture.isSuccess()) {
      channel.writeAndFlush(thing).addListener(new WriteFutureListener(channel, onError));
    } else {
      channelFuture.addListener(future -> {
        if (future.isSuccess()) {
          channel.writeAndFlush(thing).addListener(new WriteFutureListener(channel, onError));
        } else if (future.cause() == null) {
          LOG.error("Failed to set up a channel");
        } else {
          LOG.error("Failed to set up a channel", future.cause());
          onError.accept(future.cause());
        }
      });
    }
  }

  /**
   * {@link ChannelFutureListener} for handling outbound exceptions.
   */
  public static final class WriteFutureListener implements ChannelFutureListener {

    private final Channel channel;
    private final Consumer<Throwable> onError;

    /**
     * Creates a {@link ChannelFutureListener}.
     *
     * @param channel the channel
     * @param onError the {@link Consumer} to be invoked on an error during writing to the channel
     */
    private WriteFutureListener(final Channel channel, final Consumer<Throwable> onError) {
      this.channel = channel;
      this.onError = onError;
    }

    @Override
    public void operationComplete(final ChannelFuture future) {
      if (future.isSuccess()) {
        return;
      }
      channel.close();
      if (future.cause() == null) {
        LOG.error("Failed to write to the channel");
      } else {
        onError.accept(future.cause());
        LOG.error("Failed to write to the channel", future.cause());
      }
    }
  }
}