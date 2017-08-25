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
import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Input stream for partition transfer.
 *
 * @param <T> the type of element
 */
public final class PartitionInputStream<T> implements Iterable<Element<T, ?, ?>>, PartitionStream {

  private static final ByteBuf END_OF_STREAM_EVENT = new EmptyByteBuf(UnpooledByteBufAllocator.DEFAULT);

  private final String senderExecutorId;
  private final String partitionId;
  private final String runtimeEdgeId;
  private Coder<T, ?, ?> coder;
  private ExecutorService executorService;

  private final ByteBufInputStream byteBufInputStream = new ByteBufInputStream();
  private final BlockingQueue<Element<T, ?, ?>> elementQueue = new LinkedBlockingQueue<>();

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
   * Supply {@link ByteBuf} to this stream.
   *
   * @param byteBuf the {@link ByteBuf} to supply
   * @throws InterruptedException when interrupted while adding to {@link ByteBuf} queue
   */
  void append(final ByteBuf byteBuf) throws InterruptedException {
    if (byteBuf.readableBytes() > 0) {
      byteBufInputStream.byteBufDeque.putLast(byteBuf);
    } else {
      byteBuf.release();
    }
  }

  /**
   * Mark as {@link #append(ByteBuf)} event is no longer expected.
   *
   * @throws InterruptedException when interrupted while adding to {@link ByteBuf} queue
   */
  void end() throws InterruptedException {
    byteBufInputStream.byteBufDeque.putLast(END_OF_STREAM_EVENT);
  }

  /**
   * Start decoding {@link ByteBuf}s into {@link Element}s.
   */
  void start() {
    executorService.submit(() -> {
      try {
        while (!byteBufInputStream.isEnded()) {
          elementQueue.put(coder.decode(byteBufInputStream));
        }
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
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

  /**
   * An {@link InputStream} implementation that reads data from a composition of {@link ByteBuf}s.
   */
  private static final class ByteBufInputStream extends InputStream {

    private final BlockingDeque<ByteBuf> byteBufDeque = new LinkedBlockingDeque<>();

    @Override
    public int read() throws IOException {
      try {
        final ByteBuf head = byteBufDeque.takeFirst();
        if (head.readableBytes() == 0) {
          // end of stream event
          byteBufDeque.putFirst(head);
          return -1;
        }
        final byte b = head.readByte();
        if (head.readableBytes() == 0) {
          // release header if no longer required
          head.release();
        } else {
          // if not, add to deque
          byteBufDeque.putFirst(head);
        }
        return b;
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public int read(final byte[] bytes, final int baseOffset, final int maxLength) throws IOException {
      if (bytes == null) {
        throw new NullPointerException();
      }
      if (baseOffset < 0 || maxLength < 0 || maxLength > bytes.length - baseOffset) {
        throw new IndexOutOfBoundsException();
      }
      try {
        // the number of bytes that has been read so far
        int readBytes = 0;
        // the number of bytes to read
        int capacity = maxLength;
        while (capacity > 0) {
          final ByteBuf head = byteBufDeque.takeFirst();
          if (head.readableBytes() == 0) {
            // end of stream event
            byteBufDeque.putFirst(head);
            return readBytes == 0 ? -1 : readBytes;
          }
          final int toRead = Math.min(head.readableBytes(), capacity);
          head.readBytes(bytes, baseOffset + readBytes, toRead);
          if (head.readableBytes() == 0) {
            head.release();
          } else {
            byteBufDeque.putFirst(head);
          }
          readBytes += toRead;
          capacity -= toRead;
        }
        return readBytes;
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public long skip(final long n) throws IOException {
      if (n <= 0) {
        return 0;
      }
      try {
        // the number of bytes that has been skipped so far
        long skippedBytes = 0;
        // the number of bytes to skip
        long toSkip = n;
        while (toSkip > 0) {
          final ByteBuf head = byteBufDeque.takeFirst();
          if (head.readableBytes() == 0) {
            // end of stream event
            byteBufDeque.putFirst(head);
            return skippedBytes;
          }
          if (head.readableBytes() > toSkip) {
            head.skipBytes((int) toSkip);
            skippedBytes += toSkip;
            byteBufDeque.putFirst(head);
            return skippedBytes;
          } else {
            // discard the whole ByteBuf
            skippedBytes += head.readableBytes();
            toSkip -= head.readableBytes();
            head.release();
          }
        }
        return skippedBytes;
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public int available() {
      final ByteBuf head = byteBufDeque.peekFirst();
      if (head == null) {
        return 0;
      } else {
        return head.readableBytes();
      }
    }

    /**
     * Returns whether or not the end of this stream is reached.
     *
     * @return whether or not the end of this stream is reached
     */
    private boolean isEnded() {
      return byteBufDeque.peekFirst().readableBytes() == 0;
    }
  }
}
