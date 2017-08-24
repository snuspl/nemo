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

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * Output stream for partition transfer.
 *
 * @param <T> the type of element
 */
public final class PartitionOutputStream<T> implements Closeable, Flushable, PartitionTransfer.PartitionStream {
  // internally store requestId
  // internally store ConcurrentQueue and encoder for EncodingThread to encode data

  // may write FileRegion

  private final String receiverExecutorId;
  private final String partitionId;
  private final String runtimeEdgeId;
  private final Coder<T, ?, ?> coder;
  private ControlMessage.PartitionTransferType transferType;
  private short transferId;

  /**
   * Creates a partition output stream.
   *
   * @param receiverExecutorId  the id of the remote executor
   * @param partitionId         the partition id
   * @param runtimeEdgeId       the runtime edge id
   * @param coder               the coder
   */
  PartitionOutputStream(final String receiverExecutorId,
                        final String partitionId,
                        final String runtimeEdgeId,
                        final Coder<T, ?, ?> coder) {
    this.receiverExecutorId = receiverExecutorId;
    this.partitionId = partitionId;
    this.runtimeEdgeId = runtimeEdgeId;
    this.coder = coder;
  }

  @Override
  public String getPartitionId() {
    return partitionId;
  }

  @Override
  public String getRuntimeEdgeId() {
    return runtimeEdgeId;
  }

  /**
   * Sets transfer type and transfer id.
   *
   * @param type  the transfer type
   * @param id    the transfer id
   */
  void setTransferId(final ControlMessage.PartitionTransferType type, final short id) {
    this.transferType = type;
    this.transferId = id;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void flush() throws IOException {
  }
}
