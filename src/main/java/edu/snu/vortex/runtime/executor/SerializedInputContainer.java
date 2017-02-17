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
package edu.snu.vortex.runtime.executor;

import edu.snu.vortex.runtime.common.DataBuffer;
import edu.snu.vortex.runtime.common.DataBufferAllocator;
import edu.snu.vortex.runtime.common.DataBufferType;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A data container that stores serialized input records, backed by {@link DataBuffer}.
 */
public class SerializedInputContainer extends InputStream {
  private DataBufferAllocator bufferAllocator;
  private final DataBufferType internalBufferType;
  private final byte[] data;
  private List<DataBuffer> internalBuffers;
  private DataBuffer currentReadBuffer;
  private boolean isClosed;

  public SerializedInputContainer(final DataBufferAllocator bufferAllocator,
                           final DataBufferType internalBufferType) {
    this.bufferAllocator = bufferAllocator;
    this.data = new byte[1];
    this.currentReadBuffer = null;
    this.internalBuffers = new ArrayList<>();
    this.internalBufferType = internalBufferType;
    this.isClosed = false;
  }

  @Override
  public synchronized int read() throws IOException {
    if (isClosed()) {
      throw new IOException("This container has already been closed.");
    } else if (currentReadBuffer == null) {
      if (internalBuffers.isEmpty()) {
        return -1;
      }
      currentReadBuffer = internalBuffers.remove(0);
    }

    int readSize = currentReadBuffer.readNext(data, 1);
    if (readSize != 1) {
      if (internalBuffers.isEmpty()) {
        return -1;
      } else {
        bufferAllocator.releaseBuffer(currentReadBuffer);
        currentReadBuffer = internalBuffers.remove(0);

        readSize = currentReadBuffer.readNext(data, 1);
        if (readSize != 1) {
          throw new IOException("Failed to read data from buffer");
        }
      }
    }

    return (int) data[0];
  }

  public synchronized boolean isClosed() {
    return isClosed;
  }

  /**
   * Add new input data into this container.
   * Internally, the container appends a new {@link DataBuffer} to the internal buffer list.
   * @param inputData Input data.
   * @param dataSize The size of the input data.
   */
  public synchronized void addInputData(final byte[] inputData, final int dataSize) {
    final DataBuffer buffer = bufferAllocator.allocateBuffer(internalBufferType, dataSize);
    if (buffer == null) {
      throw new RuntimeException("Failed to allocate an internal buffer.");
    }

    buffer.writeNext(inputData, dataSize);
    buffer.seekFirst();
    internalBuffers.add(buffer);
  }

  public synchronized void clear() {
    internalBuffers.forEach(buf -> bufferAllocator.releaseBuffer(buf));
    internalBuffers.clear();
  }

  @Override
  public synchronized void close() {
    isClosed = true;
    clear();
  }
}
