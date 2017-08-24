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
import edu.snu.vortex.compiler.ir.Element;
import io.netty.buffer.ByteBuf;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Input stream for partition transfer.
 *
 * @param <T> the type of element
 */
public final class PartitionInputStream<T> implements Iterable<Element<T, ?, ?>>, PartitionStream {
  // internally store ByteBufInputStream and decoder for DecodingThread to decode data
  // internally store requestId

  // some methods are package scope

  private final String senderExecutorId;
  private final String partitionId;
  private final String runtimeEdgeId;
  private final Coder<T, ?, ?> coder;

  /**
   * Creates a partition input stream.
   *
   * @param senderExecutorId  the id of the remote executor
   * @param partitionId       the partition id
   * @param runtimeEdgeId     the runtime edge id
   * @param coder             the coder
   */
  PartitionInputStream(final String senderExecutorId,
                       final String partitionId,
                       final String runtimeEdgeId,
                       final Coder<T, ?, ?> coder) {
    this.senderExecutorId = senderExecutorId;
    this.partitionId = partitionId;
    this.runtimeEdgeId = runtimeEdgeId;
    this.coder = coder;
  }

  /**
   * Supply {@link ByteBuf} to this stream.
   *
   * @param byteBuf the {@link ByteBuf} to supply
   */
  void addByteBuf(final ByteBuf byteBuf) {
    // Make sure to call release after using this byteBuf
  }

  /**
   * Mark as {@link #addByteBuf(ByteBuf)} event is no longer expected.
   */
  void close() {
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
  public Iterator<Element<T, ?, ?>> iterator() {
    return null;
  }

  @Override
  public void forEach(final Consumer<? super Element<T, ?, ?>> consumer) {
    // use default?

  }

  @Override
  public Spliterator<Element<T, ?, ?>> spliterator() {
    // use default?
    return null;
  }
}
