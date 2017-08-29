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
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.executor.data.HashRange;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * A placeholder implementation.
 *
 * @param <T> the type of element
 */
public final class PartitionOutputStream<T> implements PartitionStream {
  /**
   * Creates a partition output stream.
   *
   * @param receiverExecutorId      the id of the remote executor
   * @param encodePartialPartition  whether to start encoding even when the whole partition has not been written
   * @param partitionStore          the partition store
   * @param partitionId             the partition id
   * @param runtimeEdgeId           the runtime edge id
   * @param hashRange               the hash range
   */
  PartitionOutputStream(final String receiverExecutorId,
                        final boolean encodePartialPartition,
                        final Optional<Attribute> partitionStore,
                        final String partitionId,
                        final String runtimeEdgeId,
                        final HashRange hashRange) {
  }

  /**
   * Sets {@link Coder}, {@link ExecutorService} and sizes to serialize bytes into partition.
   *
   * @param cdr     the coder
   * @param service the executor service
   * @param bSize   the outbound buffer size
   */
  void setCoderAndExecutorServiceAndBufferSize(final Coder<T, ?, ?> cdr,
                                               final ExecutorService service,
                                               final int bSize) {
  }

  @Override
  public String getRuntimeEdgeId() {
    return "";
  }

  /**
   * Sets a channel exception.
   *
   * @param cause the cause of exception handling
   */
  void onExceptionCaught(final Throwable cause) {
  }
}
