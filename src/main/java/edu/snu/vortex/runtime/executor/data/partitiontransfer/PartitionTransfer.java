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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Interface for interplay with {@link PartitionManagerWorker}.
 */
@ChannelHandler.Sharable
public final class PartitionTransfer extends SimpleChannelInboundHandler<PartitionStream> {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionTransfer.class);

  private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;
  private final PartitionTransport partitionTransport;
  private final NameResolver nameResolver;
  private final ExecutorService inboundExecutorService;
  private final ExecutorService outboundExecutorService;
  private final int bufferSize;
  private final int dataFrameSize;

  /**
   * Creates a partition transfer and registers this transfer to the name server.
   *
   * @param partitionManagerWorker  provides {@link edu.snu.vortex.common.coder.Coder}s
   * @param partitionTransport      provides {@link io.netty.channel.Channel}
   * @param nameResolver            provides naming registry
   * @param executorId              the id of this executor
   * @param inboundThreads          the number of threads in thread pool for inbound partition transfer
   * @param outboundThreads         the number of threads in thread pool for outbound partition transfer
   * @param bufferSize              the size of outbound buffers
   * @param dataFrameSize           the soft limit of the size of data frames
   */
  @Inject
  private PartitionTransfer(
      final InjectionFuture<PartitionManagerWorker> partitionManagerWorker,
      final PartitionTransport partitionTransport,
      final NameResolver nameResolver,
      @Parameter(JobConf.ExecutorId.class) final String executorId,
      @Parameter(JobConf.PartitionTransferInboundNumThreads.class) final int inboundThreads,
      @Parameter(JobConf.PartitionTransferOutboundNumThreads.class) final int outboundThreads,
      @Parameter(JobConf.PartitionTransferOutboundBufferSize.class) final int bufferSize,
      @Parameter(JobConf.PartitionTransferDataFrameSize.class) final int dataFrameSize) {
    this.partitionManagerWorker = partitionManagerWorker;
    this.partitionTransport = partitionTransport;
    this.nameResolver = nameResolver;
    // Inbound thread pool can be easily saturated with multiple incremental data transfers.
    // We may consider other solutions than using fixed thread pool.
    this.inboundExecutorService = Executors.newFixedThreadPool(inboundThreads);
    this.outboundExecutorService = Executors.newFixedThreadPool(outboundThreads);
    this.bufferSize = bufferSize;
    this.dataFrameSize = dataFrameSize;

    try {
      final PartitionTransferIdentifier identifier = new PartitionTransferIdentifier(executorId);
      nameResolver.register(identifier, partitionTransport.getServerListeningAddress());
    } catch (final Exception e) {
      LOG.error("Cannot register PartitionTransport listening address to the naming registry", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Initiate a pull-based partition transfer.
   *
   * @param executorId      the id of the source executor
   * @param partitionStore  the partition store
   * @param partitionId     the id of the partition to transfer
   * @param runtimeEdgeId   the runtime edge id
   * @param hashRange       the hash range
   * @return a {@link PartitionInputStream} from which the received
   *         {@link edu.snu.vortex.compiler.ir.Element}s can be read
   */
  public PartitionInputStream initiatePull(final String executorId,
                                           final Attribute partitionStore,
                                           final String partitionId,
                                           final String runtimeEdgeId,
                                           final HashRange hashRange) {
    final PartitionInputStream stream = new PartitionInputStream(executorId, Optional.of(partitionStore),
        partitionId, runtimeEdgeId, hashRange);
    stream.setCoderAndExecutorService(partitionManagerWorker.get().getCoder(runtimeEdgeId), inboundExecutorService);
    partitionTransport.writeTo(lookup(executorId), stream);
    return stream;
  }

  /**
   * Initiate a push-based partition transfer.
   *
   * @param executorId    the id of the destination executor
   * @param partitionId   the id of the partition to transfer
   * @param runtimeEdgeId the runtime edge id
   * @param hashRange     the hash range
   * @return a {@link PartitionOutputStream} to which {@link edu.snu.vortex.compiler.ir.Element}s can be written
   */
  public PartitionOutputStream initiatePush(final String executorId,
                                            final String partitionId,
                                            final String runtimeEdgeId,
                                            final HashRange hashRange) {
    final PartitionOutputStream stream = new PartitionOutputStream(executorId, Optional.empty(), partitionId,
        runtimeEdgeId, hashRange);
    stream.setCoderAndExecutorServiceAndSizes(partitionManagerWorker.get().getCoder(runtimeEdgeId),
        outboundExecutorService, bufferSize, dataFrameSize);
    partitionTransport.writeTo(lookup(executorId), stream);
    return stream;
  }

  /**
   * Lookup {@link PartitionTransport} listening address.
   *
   * @param executorId  the executor id
   * @return            the listening address of the {@link PartitionTransport} of the specified executor
   */
  private InetSocketAddress lookup(final String executorId) {
    try {
      final PartitionTransferIdentifier identifier = new PartitionTransferIdentifier(executorId);
      final InetSocketAddress address = nameResolver.lookup(identifier);
      return address;
    } catch (final Exception e) {
      LOG.error(String.format("Cannot lookup PartitionTransport listening address of executor %s", executorId), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final PartitionStream stream) {
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
    stream.setCoderAndExecutorServiceAndSizes(partitionManagerWorker.get().getCoder(stream.getRuntimeEdgeId()),
        outboundExecutorService, bufferSize, dataFrameSize);
    partitionManagerWorker.get().onPullRequest(stream);
    stream.start();
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
    stream.start();
  }

  /**
   * {@link Identifier} for {@link PartitionTransfer}.
   */
  private static final class PartitionTransferIdentifier implements Identifier {

    private final String executorId;

    /**
     * Creates a {@link PartitionTransferIdentifier}.
     *
     * @param executorId id of the {@link edu.snu.vortex.runtime.executor.Executor}
     *                   which this {@link PartitionTransfer} belongs to
     */
    private PartitionTransferIdentifier(final String executorId) {
      this.executorId = executorId;
    }

    @Override
    public String toString() {
      return "partition://" + executorId;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final PartitionTransferIdentifier that = (PartitionTransferIdentifier) o;
      return executorId.equals(that.executorId);
    }

    @Override
    public int hashCode() {
      return executorId.hashCode();
    }
  }
}
