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
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.executor.data.HashRange;
import edu.snu.vortex.runtime.executor.data.PartitionManagerWorker;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Manages channels and exposes an interface for {@link PartitionManagerWorker}.
 */
@ChannelHandler.Sharable
public final class PartitionTransfer extends SimpleChannelInboundHandler<PartitionStream> {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionTransfer.class);
  private static final String INBOUND = "partition:inbound";
  private static final String OUTBOUND = "partition:outbound";

  private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;
  private final PartitionTransport partitionTransport;
  private final String localExecutorId;
  private final int bufferSize;

  private final ConcurrentMap<String, Channel> channelMap = new ConcurrentHashMap<>();
  private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private final ExecutorService inboundExecutorService;
  private final ExecutorService outboundExecutorService;

  /**
   * Creates a partition transfer and registers this transfer to the name server.
   *
   * @param partitionManagerWorker  provides {@link edu.snu.vortex.common.coder.Coder}s
   * @param partitionTransport      provides {@link io.netty.channel.Channel}
   * @param localExecutorId         the id of this executor
   * @param inboundThreads          the number of threads in thread pool for inbound partition transfer
   * @param outboundThreads         the number of threads in thread pool for outbound partition transfer
   * @param bufferSize              the size of outbound buffers
   */
  @Inject
  private PartitionTransfer(
      final InjectionFuture<PartitionManagerWorker> partitionManagerWorker,
      final PartitionTransport partitionTransport,
      @Parameter(JobConf.ExecutorId.class) final String localExecutorId,
      @Parameter(JobConf.PartitionTransferInboundNumThreads.class) final int inboundThreads,
      @Parameter(JobConf.PartitionTransferOutboundNumThreads.class) final int outboundThreads,
      @Parameter(JobConf.PartitionTransferOutboundBufferSize.class) final int bufferSize) {

    this.partitionManagerWorker = partitionManagerWorker;
    this.partitionTransport = partitionTransport;
    this.localExecutorId = localExecutorId;
    this.bufferSize = bufferSize;

    // Inbound thread pool can be easily saturated with multiple data transfers with the encodePartialPartition option
    // enabled. We may consider other solutions than using fixed thread pool.
    this.inboundExecutorService = Executors.newFixedThreadPool(inboundThreads, new DefaultThreadFactory(INBOUND));
    this.outboundExecutorService = Executors.newFixedThreadPool(outboundThreads, new DefaultThreadFactory(OUTBOUND));
  }

  /**
   * Initiate a pull-based partition transfer.
   *
   * @param executorId              the id of the source executor
   * @param encodePartialPartition  whether the sender should start encoding even though the whole partition
   *                                has not been written yet
   * @param partitionStore          the partition store
   * @param partitionId             the id of the partition to transfer
   * @param runtimeEdgeId           the runtime edge id
   * @param hashRange               the hash range
   * @return a {@link PartitionInputStream} from which the received
   *         {@link edu.snu.vortex.compiler.ir.Element}s can be read
   */
  public PartitionInputStream initiatePull(final String executorId,
                                           final boolean encodePartialPartition,
                                           final Attribute partitionStore,
                                           final String partitionId,
                                           final String runtimeEdgeId,
                                           final HashRange hashRange) {
    final PartitionInputStream stream = new PartitionInputStream(executorId, encodePartialPartition,
        Optional.of(partitionStore), partitionId, runtimeEdgeId, hashRange);
    stream.setCoderAndExecutorService(partitionManagerWorker.get().getCoder(runtimeEdgeId), inboundExecutorService);
    write(executorId, stream, cause -> stream.onExceptionCaught(cause));
    return stream;
  }

  /**
   * Initiate a push-based partition transfer.
   *
   * @param executorId              the id of the destination executor
   * @param encodePartialPartition  whether to start encoding even though the whole partition has not been written yet
   * @param partitionId             the id of the partition to transfer
   * @param runtimeEdgeId           the runtime edge id
   * @param hashRange               the hash range
   * @return a {@link PartitionOutputStream} to which {@link edu.snu.vortex.compiler.ir.Element}s can be written
   */
  public PartitionOutputStream initiatePush(final String executorId,
                                            final boolean encodePartialPartition,
                                            final String partitionId,
                                            final String runtimeEdgeId,
                                            final HashRange hashRange) {
    final PartitionOutputStream stream = new PartitionOutputStream(executorId, encodePartialPartition, Optional.empty(),
        partitionId, runtimeEdgeId, hashRange);
    stream.setCoderAndExecutorServiceAndBufferSize(partitionManagerWorker.get().getCoder(runtimeEdgeId),
        outboundExecutorService, bufferSize);
    write(executorId, stream, cause -> stream.onExceptionCaught(cause));
    return stream;
  }

  /**
   * Gets a {@link ChannelFuture} for connecting to the {@link PartitionTransport} server of the specified executor.
   *
   * @param remoteExecutorId  the id of the remote executor
   * @param thing             the object to write
   * @param onError           the {@link Consumer} to be invoked on an error during setting up a channel
   *                          or writing to the channel
   */
  private void write(final String remoteExecutorId, final Object thing, final Consumer<Throwable> onError) {
    final Channel cachedChannel = channelMap.get(remoteExecutorId);
    if (cachedChannel == null) {
      final ChannelFuture channelFuture = partitionTransport.connectTo(remoteExecutorId);
      final Channel channel = channelFuture.channel();
      channel.pipeline().fireUserEventTriggered(new ChannelInitializer.SetExecutorIdEvent(localExecutorId,
          remoteExecutorId));
      channelFuture.addListener(connectionFuture -> {
        if (connectionFuture.isSuccess()) {
          channel.writeAndFlush(thing).addListener(new ControlMessageWriteFutureListener(channel, onError));
        } else if (connectionFuture.cause() == null) {
          LOG.error("Failed to connect to {}", remoteExecutorId);
        } else {
          onError.accept(connectionFuture.cause());
          LOG.error(String.format("Failed to connect to %s", remoteExecutorId), connectionFuture.cause());
        }
      });
    } else {
      cachedChannel.writeAndFlush(thing).addListener(new ControlMessageWriteFutureListener(cachedChannel, onError));
    }
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final PartitionStream stream) {
    // process the inbound control message
    if (stream instanceof PartitionInputStream) {
      onPushNotification((PartitionInputStream) stream);
    } else {
      onPullRequest((PartitionOutputStream) stream);
    }
  }

  /**
   * Respond to a new pull request.
   *
   * @param stream  {@link PartitionOutputStream}
   */
  private void onPullRequest(final PartitionOutputStream stream) {
    stream.setCoderAndExecutorServiceAndBufferSize(partitionManagerWorker.get().getCoder(stream.getRuntimeEdgeId()),
        outboundExecutorService, bufferSize);
    partitionManagerWorker.get().onPullRequest(stream);
  }

  /**
   * Respond to a new push notification.
   *
   * @param stream  {@link PartitionInputStream}
   */
  private void onPushNotification(final PartitionInputStream stream) {
    stream.setCoderAndExecutorService(partitionManagerWorker.get().getCoder(stream.getRuntimeEdgeId()),
        inboundExecutorService);
    partitionManagerWorker.get().onPushNotification(stream);
  }

  @Override
  public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) {
    final Channel channel = ctx.channel();
    if (evt instanceof ChannelInitializer.ChannelActiveEvent) {
      final ChannelInitializer.ChannelActiveEvent event = (ChannelInitializer.ChannelActiveEvent) evt;
      channelMap.compute(event.getRemoteExecutorId(), (remoteAddress, cachedChannel) -> {
        if (cachedChannel == null) {
          LOG.info("Channel established between local {}({}) and remote {}({})",
              new Object[]{event.getLocalExecutorId(), channel.localAddress(), remoteAddress, channel.remoteAddress()});
          return channel;
        } else if (channel == cachedChannel) {
          return cachedChannel;
        } else {
          LOG.warn("Multiple channel established between local {}({}) and remote {}({})",
              new Object[]{event.getLocalExecutorId(), channel.localAddress(), remoteAddress, channel.remoteAddress()});
          return channel;
        }
      });
    } else if (evt instanceof ChannelInitializer.ChannelInactiveEvent) {
      final ChannelInitializer.ChannelInactiveEvent event = (ChannelInitializer.ChannelInactiveEvent) evt;
      channelMap.remove(event.getRemoteExecutorId());
    }
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    channelGroup.add(ctx.channel());
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) {
    channelGroup.remove(ctx.channel());
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
    LOG.error(String.format("Exception caught in the channel with local address %s and remote address %s",
        ctx.channel().localAddress(), ctx.channel().remoteAddress()), cause);
    ctx.close();
  }

  /**
   * Gets the channel group.
   *
   * @return the channel group
   */
  ChannelGroup getChannelGroup() {
    return channelGroup;
  }

  /**
   * {@link ChannelFutureListener} for handling outbound exceptions on writing control messages.
   */
  public static final class ControlMessageWriteFutureListener implements ChannelFutureListener {

    private final Channel channel;
    private final Consumer<Throwable> onError;

    /**
     * Creates a {@link ControlMessageWriteFutureListener}.
     *
     * @param channel the channel
     * @param onError the {@link Consumer} to be invoked on an error during writing to the channel
     */
    private ControlMessageWriteFutureListener(final Channel channel, final Consumer<Throwable> onError) {
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
        LOG.error("Failed to write a control message");
      } else {
        onError.accept(future.cause());
        LOG.error("Failed to write a control message", future.cause());
      }
    }
  }
}
