/*
 * Copyright (C) 2016 Seoul National University
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

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class BufferAllocatorCommonTest {
  private final ReadWriteBufferAllocator bufferAllocator;
  private final ReadWriteBufferCommonTest rwBufferTests;

  BufferAllocatorCommonTest(final ReadWriteBufferAllocator bufferAllocator) {
    this.bufferAllocator = bufferAllocator;
    this.rwBufferTests = new ReadWriteBufferCommonTest();
  }

  public void testAllocateSingleBuffer(final long bufferSize) {
    final int rwChunkSize = 0x1000;

    // Constraint on parameters.
    assertTrue(bufferSize % rwChunkSize == 0);
    final ReadWriteBuffer buffer = bufferAllocator.allocateBuffer(bufferSize);
    assertTrue(buffer != null);

    rwBufferTests.testMultipleRead(buffer, rwChunkSize);
    buffer.clear();
    rwBufferTests.testMultipleWrite(buffer, rwChunkSize);
    bufferAllocator.releaseBuffer(buffer);
  }

  public void testAllocateMultipleBuffers(final long bufferSize, final long numBuffer) {
    final int rwChunkSize = 0x1000;
    final List<ReadWriteBuffer> buffers = new ArrayList<>();

    // Constraint on parameters.
    assertTrue(bufferSize % rwChunkSize == 0);

    for (long i = 0; i < numBuffer; i++) {
      final ReadWriteBuffer rwBuffer = bufferAllocator.allocateBuffer(bufferSize);
      assertTrue(rwBuffer != null);

      buffers.add(rwBuffer);
    }

    buffers.forEach(buffer -> {
      rwBufferTests.testMultipleRead(buffer, rwChunkSize);
      buffer.clear();
      rwBufferTests.testMultipleWrite(buffer, rwChunkSize);
    });

    buffers.forEach(buffer -> bufferAllocator.releaseBuffer(buffer));
  }


}
