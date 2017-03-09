package edu.snu.vortex.runtime.executor;


import edu.snu.vortex.runtime.common.DataBuffer;
import edu.snu.vortex.runtime.common.DataBufferAllocator;
import edu.snu.vortex.runtime.common.DataBufferType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A data container that stores serialized output records, backed by {@link DataBuffer}.
 */
public final class SerializedOutputContainer extends OutputStream {
  private static final long DEFAULT_INTERNAL_BUFFER_SIZE = 0x8000; // 32KB.
  private DataBufferAllocator bufferAllocator;
  private final List<DataBuffer> internalBuffers;
  private final DataBufferType internalBufferType;
  private final long internalBufferSize;
  private DataBuffer currentWriteBuffer;
  private final byte[] data;


  public SerializedOutputContainer(final DataBufferAllocator bufferAllocator,
                                   final DataBufferType internalBufferType,
                                   final long internalBufferSize) {
    this.bufferAllocator = bufferAllocator;
    this.internalBuffers = new ArrayList<>();
    this.internalBufferSize = internalBufferSize;
    this.internalBufferType = internalBufferType;
    this.data = new byte[1];
  }

  public SerializedOutputContainer(final DataBufferAllocator bufferAllocator,
                            final DataBufferType internalBufferType) {
    this(bufferAllocator, internalBufferType, DEFAULT_INTERNAL_BUFFER_SIZE);
  }

  private DataBuffer allocateInternalBuffer() {
    final DataBuffer newBuffer = bufferAllocator.allocateBuffer(internalBufferType, internalBufferSize);
    if (newBuffer == null) {
      throw new OutOfMemoryError("Failed to allocate an internal buffer.");
    }

    internalBuffers.add(newBuffer);

    return newBuffer;
  }

  @Override
  public synchronized void write(final int b) throws IOException {
    data[0] = (byte) b;

    if (currentWriteBuffer == null) {
      currentWriteBuffer = allocateInternalBuffer();
    }

    int writeSize = currentWriteBuffer.writeNext(data, 1);
    if (writeSize < 1) { // The current buffer is full. Allocate another one.
      currentWriteBuffer = allocateInternalBuffer();
      writeSize = currentWriteBuffer.writeNext(data, 1);
      if (writeSize != 1) {
        throw new IOException("Unable to write data to an internal buffer.");
      }
    }
  }

  public synchronized int copySingleDataBufferTo(final byte[] outputBuffer, final int outputBufferSize) {
    if (internalBuffers.isEmpty()) {
      return 0;
    }


    if (internalBuffers.get(0).getRemainingDataSize() > outputBufferSize) {
      return -1;
    }

    final DataBuffer buffer = internalBuffers.remove(0);
    if (internalBuffers.isEmpty()) {
      currentWriteBuffer = null;
    }

    buffer.seekFirst();
    final int readSize = buffer.readNext(outputBuffer, outputBufferSize);

    bufferAllocator.releaseBuffer(buffer);
    return readSize;
  }

/*
  public synchronized DataBuffer getSingleDataBuffer() {
    if (internalBuffers.isEmpty()) {
      return null;
    }


    final DataBuffer buffer = internalBuffers.remove(0);
    if (internalBuffers.isEmpty()) {
      currentWriteBuffer = null;
    }

    buffer.seekFirst();

    bufferAllocator.releaseBuffer(buffer);
    return buffer;
  }
*/
  public synchronized void clear() {
    currentWriteBuffer = null;
    internalBuffers.forEach(buf -> bufferAllocator.releaseBuffer(buf));
    internalBuffers.clear();
  }


  @Override
  public synchronized void close() {

  }
}
