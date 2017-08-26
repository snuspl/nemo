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
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.executor.data.HashRange;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.FileRegion;
import io.netty.util.Recycler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Output stream for partition transfer. {@link #close()} must be called after finishing write.
 *
 * @param <T> the type of element
 */
public final class PartitionOutputStream<T> implements Closeable, PartitionStream {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionInputStream.class);

  private final String receiverExecutorId;
  private final Optional<Attribute> partitionStore;
  private final String partitionId;
  private final String runtimeEdgeId;
  private final HashRange hashRange;
  private ControlMessage.PartitionTransferType transferType;
  private short transferId;
  private Channel channel;
  private Coder<T, ?, ?> coder;
  private ExecutorService executorService;
  private int bufferSize;
  private int dataFrameSize;

  private ByteBufOutputStream byteBufOutputStream;
  private final BlockingQueue<Object> elementQueue = new LinkedBlockingQueue<>();
  private ByteBuf nonEndingFrameHeader;
  private volatile boolean closed = false;
  private volatile Throwable streamException = null;

  /**
   * Creates a partition output stream.
   *
   * @param receiverExecutorId  the id of the remote executor
   * @param partitionStore      the partition store
   * @param partitionId         the partition id
   * @param runtimeEdgeId       the runtime edge id
   * @param hashRange           the hash range
   */
  PartitionOutputStream(final String receiverExecutorId,
                        final Optional<Attribute> partitionStore,
                        final String partitionId,
                        final String runtimeEdgeId,
                        final HashRange hashRange) {
    this.receiverExecutorId = receiverExecutorId;
    this.partitionStore = partitionStore;
    this.partitionId = partitionId;
    this.runtimeEdgeId = runtimeEdgeId;
    this.hashRange = hashRange;
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
    this.nonEndingFrameHeader = ch.alloc().directBuffer(DataFrameHeaderEncoder.TYPE_AND_TRANSFERID_LENGTH,
        DataFrameHeaderEncoder.TYPE_AND_TRANSFERID_LENGTH);
    DataFrameHeaderEncoder.encodeTypeAndTransferId(type, id, nonEndingFrameHeader);
  }

  /**
   * Sets {@link Coder}, {@link ExecutorService} and sizes to serialize bytes into partition.
   *
   * @param cdr     the coder
   * @param service the executor service
   * @param bSize   the outbound buffer size
   * @param dfSize  the data frame size
   */
  void setCoderAndExecutorServiceAndSizes(final Coder<T, ?, ?> cdr,
                                          final ExecutorService service,
                                          final int bSize,
                                          final int dfSize) {
    this.coder = cdr;
    this.executorService = service;
    this.bufferSize = bSize;
    this.dataFrameSize = dfSize;
  }

  @Override
  public String getRemoteExecutorId() {
    return receiverExecutorId;
  }

  @Override
  public Optional<Attribute> getPartitionStore() {
    return partitionStore;
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
  public HashRange getHashRange() {
    return hashRange;
  }

  /**
   * Starts the encoding and writing to the channel.
   */
  void start() {
    assert (channel != null);
    assert (coder != null);
    byteBufOutputStream = new ByteBufOutputStream();
    executorService.submit(() -> {
      try {
        while (true) {
          final Object thing = elementQueue.take();
          if (thing == EndOfPartitionEvent.END_OF_PARTITION) {
            channel.pipeline().fireUserEventTriggered(EndOfOutputStreamEvent.newInstance(transferType, transferId));
            byteBufOutputStream.close();
            break;
          } else if (thing instanceof Iterable) {
            final Iterable<Element> iterable = (Iterable<Element>) thing;
            for (final Element element : iterable) {
              coder.encode(element, byteBufOutputStream);
            }
          } else if (thing instanceof FileRegion) {
            byteBufOutputStream.writeFileRegion(false, (FileRegion) thing);
          } else {
            coder.encode((Element) thing, byteBufOutputStream);
          }
        }
      } catch (final Exception e) {
        LOG.error("An exception in PartitionOutputStream thread", e);
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Writes an {@link Element}.
   *
   * @param element the {@link Element} to write
   * @return {@link PartitionOutputStream} (i.e. {@code this})
   * @throws IOException if an exception was set
   */
  public PartitionOutputStream write(final Element<T, ?, ?> element) throws IOException {
    throwStreamExceptionIfNeeded();
    assert (!closed);
    try {
      elementQueue.put(element);
      return this;
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Writes a {@link Iterable} of {@link Element}s.
   *
   * @param iterable  the {@link Iterable} to write
   * @return {@link PartitionOutputStream} (i.e. {@code this})
   * @throws IOException if an exception was set
   */
  public PartitionOutputStream write(final Iterable<Element<T, ?, ?>> iterable) throws IOException {
    throwStreamExceptionIfNeeded();
    assert (!closed);
    try {
      elementQueue.put(iterable);
      return this;
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Writes a {@link FileRegion}. Zero-copy transfer is used if possible.
   * The number of bytes should be within the range of {@link Integer}.
   *
   * @param fileRegion  provides the descriptor of the file to write
   * @return {@link PartitionOutputStream} (i.e. {@code this})
   * @throws IOException if an exception was set
   */
  public PartitionOutputStream write(final FileRegion fileRegion) throws IOException {
    throwStreamExceptionIfNeeded();
    assert (!closed);
    try {
      elementQueue.put(fileRegion);
      return this;
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    throwStreamExceptionIfNeeded();
    closed = true;
    try {
      elementQueue.put(EndOfPartitionEvent.END_OF_PARTITION);
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Throws an {@link IOException} if needed.
   *
   * @throws IOException if an exception was set
   */
  private void throwStreamExceptionIfNeeded() throws IOException {
    if (streamException != null) {
      throw new IOException(streamException);
    }
  }

  /**
   * Sets an stream exception.
   *
   * @param cause the cause of exception handling
   */
  void onExceptionCaught(final Throwable cause) {
    this.streamException = cause;
  }

  /**
   * An {@link OutputStream} implementation which buffers data to {@link ByteBuf}s.
   */
  private final class ByteBufOutputStream extends OutputStream {

    private ByteBuf byteBuf;
    private CompositeByteBuf compositeByteBuf;

    /**
     * Creates a {@link ByteBufOutputStream}.
     */
    private ByteBufOutputStream() {
      createByteBuf();
      createCompositeByteBuf();
    }

    @Override
    public void write(final int i) {
      flushCompositeByteBuf();
      byteBuf.writeByte(i);
      if (byteBuf.writableBytes() == 0) {
        flushByteBuf();
      }
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) {
      flushByteBuf();
      compositeByteBuf.addComponent(Unpooled.wrappedBuffer(bytes, offset, length));
      compositeByteBuf.writerIndex(compositeByteBuf.writerIndex() + length);
      if (compositeByteBuf.readableBytes() >= dataFrameSize) {
        flushCompositeByteBuf();
      }
    }

    @Override
    public void flush() {
      flushByteBuf();
      flushCompositeByteBuf();
    }

    @Override
    public void close() {
      nonEndingFrameHeader.release();
      boolean sentEndingFrame = false;
      if (byteBuf.readableBytes() > 0) {
        writeByteBuf(true);
        sentEndingFrame = true;
      } else {
        byteBuf.release();
      }
      if (compositeByteBuf.readableBytes() > 0) {
        writeCompositeByteBuf(true);
        sentEndingFrame = true;
      } else {
        compositeByteBuf.release();
      }
      if (!sentEndingFrame) {
        // should send an ending frame to indicate the end of the partition stream
        writeDataFrameHeader(true, 0);
      }
    }

    /**
     * Writes a data frame from {@link ByteBuf} and creates a new one.
     */
    private void flushByteBuf() {
      if (byteBuf.readableBytes() > 0) {
        writeByteBuf(false);
        createByteBuf();
      }
    }

    /**
     * Writes a data frame from {@link ByteBuf}.
     *
     * @param ending  whether or not the frame is an ending frame
     */
    private void writeByteBuf(final boolean ending) {
      writeDataFrameHeader(ending, byteBuf.readableBytes());
      channel.writeAndFlush(byteBuf);
    }

    /**
     * Creates {@link ByteBuf}.
     */
    private void createByteBuf() {
      byteBuf = channel.alloc().ioBuffer(bufferSize, bufferSize);
    }

    /**
     * Writes a data frame from {@link CompositeByteBuf} and creates a new one.
     */
    private void flushCompositeByteBuf() {
      if (compositeByteBuf.readableBytes() > 0) {
        writeCompositeByteBuf(false);
        createCompositeByteBuf();
      }
    }

    /**
     * Writes a data frame from {@link CompositeByteBuf}.
     *
     * @param ending  whether or not the frame is an ending frame
     */
    private void writeCompositeByteBuf(final boolean ending) {
      writeDataFrameHeader(ending, compositeByteBuf.readableBytes());
      channel.writeAndFlush(compositeByteBuf);
    }

    /**
     * Creates {@link CompositeByteBuf}.
     */
    private void createCompositeByteBuf() {
      compositeByteBuf = channel.alloc().compositeDirectBuffer();
    }

    /**
     * Writes the header of a data frame.
     *
     * @param ending  whether or not the frame is an ending frame
     * @param length  the length of the data frame body
     */
    private void writeDataFrameHeader(final boolean ending, final int length) {
      if (ending) {
        final ByteBuf endingFrameHeader = channel.alloc().ioBuffer(DataFrameHeaderEncoder.HEADER_LENGTH,
            DataFrameHeaderEncoder.HEADER_LENGTH);
        DataFrameHeaderEncoder.encodeLastFrame(transferType, transferId, length, endingFrameHeader);
        channel.write(endingFrameHeader);
      } else {
        channel.write(nonEndingFrameHeader.retain());
        channel.write(channel.alloc().ioBuffer(DataFrameHeaderEncoder.LENGTH_LENGTH,
            DataFrameHeaderEncoder.LENGTH_LENGTH).writeInt(length));
      }
    }

    /**
     * Writes a data frame from {@link FileRegion}.
     * The number of bytes should be within the range of {@link Integer}.
     *
     * @param ending      whether or not the frame is an ending frame
     * @param fileRegion  the {@link FileRegion} to transfer
     */
    private void writeFileRegion(final boolean ending, final FileRegion fileRegion) {
      if (fileRegion.count() > Integer.MAX_VALUE) {
        throw new IllegalArgumentException(String.format("Too big count of the FileRegion to send: %d",
            fileRegion.count()));
      }
      flush();
      writeDataFrameHeader(ending, (int) fileRegion.count());
      channel.writeAndFlush(fileRegion);
    }
  }

  /**
   * An event meaning the end of outbound {@link edu.snu.vortex.compiler.ir.Element}s.
   */
  private static final class EndOfPartitionEvent {
    static final EndOfPartitionEvent END_OF_PARTITION = new EndOfPartitionEvent();

    /**
     * Private constructor.
     */
    private EndOfPartitionEvent() {
    }
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
