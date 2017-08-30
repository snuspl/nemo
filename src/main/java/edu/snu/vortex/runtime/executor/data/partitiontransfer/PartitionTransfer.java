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
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Interface for interplay with {@link PartitionManagerWorker}.
 */
@ChannelHandler.Sharable
public final class PartitionTransfer extends SimpleChannelInboundHandler<PartitionStream> {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionTransfer.class);
  private static final String INBOUND = "partition:inbound";
  private static final String OUTBOUND = "partition:outbound";

  private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;
  private final PartitionTransport partitionTransport;
  private final ExecutorService inboundExecutorService;
  private final ExecutorService outboundExecutorService;
  private final int bufferSize;

  /**
   * Creates a partition transfer and registers this transfer to the name server.
   *
   * @param partitionManagerWorker  provides {@link edu.snu.vortex.common.coder.Coder}s
   * @param partitionTransport      provides {@link io.netty.channel.Channel}
   * @param executorId              the id of this executor
   * @param inboundThreads          the number of threads in thread pool for inbound partition transfer
   * @param outboundThreads         the number of threads in thread pool for outbound partition transfer
   * @param bufferSize              the size of outbound buffers
   */
  @Inject
  private PartitionTransfer(
      final InjectionFuture<PartitionManagerWorker> partitionManagerWorker,
      final PartitionTransport partitionTransport,
      @Parameter(JobConf.ExecutorId.class) final String executorId,
      @Parameter(JobConf.PartitionTransferInboundNumThreads.class) final int inboundThreads,
      @Parameter(JobConf.PartitionTransferOutboundNumThreads.class) final int outboundThreads,
      @Parameter(JobConf.PartitionTransferOutboundBufferSize.class) final int bufferSize) {

    this.partitionManagerWorker = partitionManagerWorker;
    this.partitionTransport = partitionTransport;
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
    partitionTransport.writeTo(executorId, stream, cause -> stream.onExceptionCaught(cause));
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
    partitionTransport.writeTo(executorId, stream, cause -> stream.onExceptionCaught(cause));
    return stream;
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
}
