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

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import io.netty.channel.Channel;
import io.netty.util.Recycler;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * Output stream for partition transfer.
 *
 * @param <T> the type of element
 */
public final class PartitionOutputStream<T> implements Closeable, Flushable, PartitionStream {
  // internally store requestId
  // internally store ConcurrentQueue and encoder for EncodingThread to encode data

  // may write FileRegion

  private final String receiverExecutorId;
  private final String partitionId;
  private final String runtimeEdgeId;
  private ControlMessage.PartitionTransferType transferType;
  private short transferId;
  private Channel channel;
  private Coder<T, ?, ?> coder;
  private ExecutorService executorService;

  /**
   * Creates a partition output stream.
   *
   * @param receiverExecutorId  the id of the remote executor
   * @param partitionId         the partition id
   * @param runtimeEdgeId       the runtime edge id
   */
  PartitionOutputStream(final String receiverExecutorId,
                        final String partitionId,
                        final String runtimeEdgeId) {
    this.receiverExecutorId = receiverExecutorId;
    this.partitionId = partitionId;
    this.runtimeEdgeId = runtimeEdgeId;
  }

  /**
   * Sets transfer type, transfer id, and {@link io.netty.channel.Channel}.
   *
   * @param type  the transfer type
   * @param id    the transfer id
   * @param ch    the channel
   */
  void setTransferIdAndChannel(final ControlMessage.PartitionTransferType type, final short id, final Channel ch) {
    this.transferType = type;
    this.transferId = id;
    this.channel = ch;
  }

  /**
   * Sets {@link Coder} and {@link ExecutorService} to serialize bytes into partition.
   *
   * @param cdr     the coder
   * @param service the executor service
   */
  void setCoderAndExecutorService(final Coder<T, ?, ?> cdr, final ExecutorService service) {
    this.coder = cdr;
    this.executorService = service;
  }

  @Override
  public String getRemoteExecutorId() {
    return receiverExecutorId;
  }

  @Override
  public String getPartitionId() {
    return partitionId;
  }

  @Override
  public String getRuntimeEdgeId() {
    return runtimeEdgeId;
  }

  @Override
  public void close() throws IOException {
    channel.pipeline().fireUserEventTriggered(EndOfOutputStreamEvent.newInstance(transferType, transferId));
  }

  @Override
  public void flush() throws IOException {
  }

  /**
   * An event meaning the end of a {@link PartitionOutputStream}.
   */
  static final class EndOfOutputStreamEvent {

    private static final Recycler<EndOfOutputStreamEvent> RECYCLER = new Recycler<EndOfOutputStreamEvent>() {
      @Override
      protected EndOfOutputStreamEvent newObject(final Recycler.Handle handle) {
        return new EndOfOutputStreamEvent(handle);
      }
    };

    private final Recycler.Handle handle;

    /**
     * Creates a {@link EndOfOutputStreamEvent}.
     *
     * @param handle  the recycler handle
     */
    private EndOfOutputStreamEvent(final Recycler.Handle handle) {
      this.handle = handle;
    }

    private ControlMessage.PartitionTransferType transferType;
    private short transferId;

    /**
     * Creates an {@link EndOfOutputStreamEvent}.
     *
     * @param transferType  the transfer type
     * @param transferId    the transfer id
     * @return an {@link EndOfOutputStreamEvent}
     */
    private static EndOfOutputStreamEvent newInstance(final ControlMessage.PartitionTransferType transferType,
                                                      final short transferId) {
      final EndOfOutputStreamEvent event = RECYCLER.get();
      event.transferType = transferType;
      event.transferId = transferId;
      return event;
    }

    /**
     * Recycles this object.
     */
    void recycle() {
      RECYCLER.recycle(this, handle);
    }

    /**
     * Gets the transfer type.
     *
     * @return the transfer type
     */
    ControlMessage.PartitionTransferType getTransferType() {
      return transferType;
    }

    /**
     * Gets the transfer id.
     *
     * @return the transfer id
     */
    short getTransferId() {
      return transferId;
    }
  }
}
