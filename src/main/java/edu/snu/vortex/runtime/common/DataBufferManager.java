package edu.snu.vortex.runtime.common;

import edu.snu.vortex.runtime.exception.ItemNotFoundException;
import edu.snu.vortex.runtime.exception.NotImplementedException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manager, as a wrapping of {@link BufferAllocator} implementations, allocates data buffers with different types.
 * According to {@link DataBufferType},
 * it returns a certain type of {@link DataBuffer} backed by {@link ReadWriteBuffer}.
 */
public final class DataBufferManager {
  private static AtomicInteger idFactory = new AtomicInteger(0);
  private final MemoryBufferAllocator memBufAllocator;
  private final LocalFileBufferAllocator fileBufAllocator;
  private final Map<String, ReadWriteBuffer> bufIdToRWBufMap;

  DataBufferManager(final MemoryBufferAllocator memBufAlloc,
                    final LocalFileBufferAllocator fileBufAlloc) {
    this.memBufAllocator = memBufAlloc;
    this.fileBufAllocator = fileBufAlloc;
    this.bufIdToRWBufMap = new HashMap<>();
  }

  private String generateBufferId() {
    return "datbuf-" + idFactory.getAndIncrement();
  }

  public DataBuffer allocateBuffer(final DataBufferType bufferType) {
    long defaultBufSize;

    switch (bufferType) {
    case LOCAL_MEMORY:
      defaultBufSize = memBufAllocator.getDefaultBufSize();
      break;
    case LOCAL_FILE:
      defaultBufSize = fileBufAllocator.getDefaultBufSize();
      break;
    default:
      throw new NotImplementedException("Allocating a " + bufferType + " type buffer is not available.");
    }

    return allocateBuffer(bufferType, defaultBufSize);
  }

  public DataBuffer allocateBuffer(final DataBufferType bufferType, final long bufferSize) {
    ReadWriteBuffer internalBuffer;
    DataBuffer buffer;

    switch (bufferType) {
    case LOCAL_MEMORY:
      internalBuffer = memBufAllocator.allocateBuffer(bufferSize);
      buffer = new DataBuffer(generateBufferId(),
                            DataBufferType.LOCAL_MEMORY,
                            internalBuffer);

      bufIdToRWBufMap.put(buffer.getId(), internalBuffer);
      return buffer;

    case LOCAL_FILE:
      internalBuffer = fileBufAllocator.allocateBuffer(bufferSize);
      buffer = new DataBuffer(generateBufferId(),
                            DataBufferType.LOCAL_FILE,
                            internalBuffer);

      bufIdToRWBufMap.put(buffer.getId(), internalBuffer);
      return buffer;

    default:
      throw new NotImplementedException("Allocating a " + bufferType + " type buffer is not available.");
    }
  }

  public void releaseBuffer(final DataBuffer buffer) {
    final ReadWriteBuffer internalBuffer = bufIdToRWBufMap.remove(buffer.getId());
    if (internalBuffer == null) {
      throw new ItemNotFoundException("Buffer to be destructed has an invalid buffer id.");
    }

    switch (buffer.getType()) {
    case LOCAL_MEMORY:
      memBufAllocator.releaseBuffer(internalBuffer);
      break;
    case LOCAL_FILE:
      fileBufAllocator.releaseBuffer(internalBuffer);
      break;
    default:
      throw new NotImplementedException("releasing a " + buffer.getType() + " type buffer is not available.");
    }
  }


}
