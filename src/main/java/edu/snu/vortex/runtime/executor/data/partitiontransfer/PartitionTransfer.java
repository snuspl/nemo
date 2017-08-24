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
import edu.snu.vortex.runtime.executor.data.PartitionManagerWorker;
import io.netty.util.Recycler;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Interface for {@link edu.snu.vortex.runtime.executor.data.PartitionManagerWorker} to initiate a partition transfer.
 */
public final class PartitionTransfer {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionTransfer.class);

  private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;
  private final PartitionTransport partitionTransport;
  private final NameResolver nameResolver;
  private final ExecutorService inboundExecutorService;
  private final ExecutorService outboundExecutorService;

  /**
   * Creates a partition transfer and registers this transfer to the name server.
   *
   * @param partitionManagerWorker  provides {@link edu.snu.vortex.common.coder.Coder}s
   * @param partitionTransport      provides {@link io.netty.channel.Channel}
   * @param nameResolver            provides naming registry
   * @param executorId              the id of this executor
   * @param inboundThreads          the number of threads in thread pool for inbound partition transfer
   * @param outboundThreads         the nubmer of threads in thread pool for outbound partition transfer
   */
  @Inject
  private PartitionTransfer(
      final InjectionFuture<PartitionManagerWorker> partitionManagerWorker,
      final PartitionTransport partitionTransport,
      final NameResolver nameResolver,
      @Parameter(JobConf.ExecutorId.class) final String executorId,
      @Parameter(JobConf.PartitionTransferInboundNumThreads.class) final int inboundThreads,
      @Parameter(JobConf.PartitionTransferOutboundNumThreads.class) final int outboundThreads) {
    this.partitionManagerWorker = partitionManagerWorker;
    this.partitionTransport = partitionTransport;
    this.nameResolver = nameResolver;
    this.inboundExecutorService = Executors.newFixedThreadPool(inboundThreads);
    this.outboundExecutorService = Executors.newFixedThreadPool(outboundThreads);

    try {
      final PartitionTransferIdentifier identifier = PartitionTransferIdentifier.newInstance(executorId);
      nameResolver.register(identifier, partitionTransport.getServerListeningAddress());
      identifier.recycle();
    } catch (final Exception e) {
      LOG.error("Cannot register PartitionTransport listening address to the naming registry", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Initiate a pull-based partition transfer.
   *
   * @param executorId    the id of the source executor
   * @param partitionId   the id of the partition to transfer
   * @param runtimeEdgeId the runtime edge id
   * @return a {@link PartitionInputStream} from which the received
   *         {@link edu.snu.vortex.compiler.ir.Element}s can be read
   */
  public PartitionInputStream pull(final String executorId, final String partitionId, final String runtimeEdgeId) {
    return null;
  }

  /**
   * Initiate a push-based partition transfer.
   *
   * @param executorId    the id of the destination executor
   * @param partitionId   the id of the partition to transfer
   * @param runtimeEdgeId the runtime edge id
   * @return a {@link PartitionOutputStream} to which {@link edu.snu.vortex.compiler.ir.Element}s can be written
   */
  public PartitionOutputStream push(final String executorId, final String partitionId, final String runtimeEdgeId) {
    return null;
  }

  /**
   * {@link Identifier} for {@link PartitionTransfer}.
   */
  private static final class PartitionTransferIdentifier implements Identifier {

    private final Recycler.Handle handle;

    private static final Recycler<PartitionTransferIdentifier> RECYCLER = new Recycler<PartitionTransferIdentifier>() {
      @Override
      protected PartitionTransferIdentifier newObject(final Recycler.Handle handle) {
        return new PartitionTransferIdentifier(handle);
      }
    };

    /**
     * Creates a {@link PartitionTransferIdentifier}.
     *
     * @param handle  the recycler handle
     */
    private PartitionTransferIdentifier(final Recycler.Handle handle) {
      this.handle = handle;
    }

    private String executorId;

    /**
     * Returns a {@link PartitionTransferIdentifier}.
     *
     * @param executorId id of the {@link edu.snu.vortex.runtime.executor.Executor}
     *                   which this {@link PartitionTransfer} belongs to
     * @return a {@link PartitionTransferIdentifier}
     */
    private static PartitionTransferIdentifier newInstance(final String executorId) {
      final PartitionTransferIdentifier identifier = RECYCLER.get();
      identifier.executorId = executorId;
      return identifier;
    }

    /**
     * Recycles this object.
     */
    public void recycle() {
      executorId = null;
      RECYCLER.recycle(this, handle);
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
