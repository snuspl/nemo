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
package edu.snu.vortex.runtime.common;

import edu.snu.vortex.runtime.exception.InvalidParameterException;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Allocates {@link MemoryBuffer} which can be used as {@link ReadWriteBuffer}.
 */
public final class MemoryBufferAllocator implements BufferAllocator {
  private final int defaultBufSize;
  private final AtomicInteger idFactory = new AtomicInteger(0);


  MemoryBufferAllocator(final int defaultBufSize) {
    this.defaultBufSize = defaultBufSize;
  }

  public long getDefaultBufSize() {
    return (long) defaultBufSize;
  }

  public MemoryBuffer allocateBuffer(final long requiredBufSize) {
    final int bufferId = idFactory.getAndIncrement();
    if (requiredBufSize > Integer.MAX_VALUE) {
      throw new InvalidParameterException("the required buffer size is too large.");
    }

    final int bufferSize = (int) requiredBufSize;
    final ByteBuffer internalBuffer = ByteBuffer.allocate(bufferSize);

    final MemoryBuffer memoryBuffer = new MemoryBuffer(bufferId,
                                                      internalBuffer,
                                                      bufferSize);

    return memoryBuffer;
  }

  public void releaseBuffer(final ReadWriteBuffer buffer) {
    if (buffer instanceof MemoryBuffer) {
      releaseBuffer((MemoryBuffer) buffer);
    }
  }

  private void releaseBuffer(final MemoryBuffer memoryBuffer) {
    memoryBuffer.clear();
  }
}
