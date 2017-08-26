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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
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

/**
 * Transport implementation for peer-to-peer {@link edu.snu.vortex.runtime.executor.data.partition.Partition} transfer.
 */
final class PartitionTransport implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionTransport.class);

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

    serverListeningGroup = NettyChannelImplementationSelector.EVENT_LOOP_GROUP_FUNCTION.apply(numListeningThreads);
    serverWorkingGroup = NettyChannelImplementationSelector.EVENT_LOOP_GROUP_FUNCTION.apply(numWorkingThreads);
    clientGroup = NettyChannelImplementationSelector.EVENT_LOOP_GROUP_FUNCTION.apply(numClientThreads);

    final ChannelInitializer channelInitializer
        = new ChannelInitializer(channelGroup, channelFutureMap, partitionTransfer, localExecutorId);

    clientBootstrap = new Bootstrap();
    clientBootstrap
        .group(clientGroup)
        .channel(NettyChannelImplementationSelector.CHANNEL_CLASS)
        .handler(channelInitializer)
        .option(ChannelOption.SO_REUSEADDR, true);

    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap
        .group(serverListeningGroup, serverWorkingGroup)
        .channel(NettyChannelImplementationSelector.SERVER_CHANNEL_CLASS)
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
   */
  void writeTo(final SocketAddress remoteAddress, final Object thing) {
    final ChannelFuture channelFuture = channelFutureMap
        .computeIfAbsent(remoteAddress, address -> clientBootstrap.connect(address));
    if (channelFuture.isSuccess()) {
      channelFuture.channel().writeAndFlush(thing);
    } else {
      channelFuture.addListener(x -> channelFuture.channel().writeAndFlush(thing));
    }
  }
}
