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
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.Channel;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
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
  private Coder<T, ?, ?> coder;
  private ExecutorService executorService;

  private final BlockingQueue<Element<T, ?, ?>> elementQueue = new LinkedBlockingQueue<>();
  private CompositeByteBuf compositeByteBuf;
  private InputStream inputStream;

  /**
   * Creates a partition input stream.
   *
   * @param senderExecutorId  the id of the remote executor
   * @param partitionId       the partition id
   * @param runtimeEdgeId     the runtime edge id
   */
  PartitionInputStream(final String senderExecutorId,
                       final String partitionId,
                       final String runtimeEdgeId) {
    this.senderExecutorId = senderExecutorId;
    this.partitionId = partitionId;
    this.runtimeEdgeId = runtimeEdgeId;
  }

  /**
   * Sets {@link Coder} and {@link ExecutorService} to de-serialize bytes into partition.
   *
   * @param cdr     the coder
   * @param service the executor service
   */
  void setCoderAndExecutorService(final Coder<T, ?, ?> cdr, final ExecutorService service) {
    this.coder = cdr;
    this.executorService = service;
  }

  /**
   * Starts this stream.
   *
   * @param channel the corresponding {@link Channel} to this stream
   */
  void start(final Channel channel) {
    // make sure decoder thread calls compositeByteBuf.release on completion
    compositeByteBuf = channel.alloc().compositeBuffer();
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
  public String getRemoteExecutorId() {
    return senderExecutorId;
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
    return elementQueue.iterator();
  }

  @Override
  public void forEach(final Consumer<? super Element<T, ?, ?>> consumer) {
    elementQueue.forEach(consumer);
  }

  @Override
  public Spliterator<Element<T, ?, ?>> spliterator() {
    return elementQueue.spliterator();
  }
}
