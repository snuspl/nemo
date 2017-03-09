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


import java.nio.ByteBuffer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link MemoryBuffer}.
 */
public final class MemoryBufferTest {
  private final ReadWriteBufferCommonTest rwBufferTest = new ReadWriteBufferCommonTest();

  private MemoryBuffer allocateBuffer(final int bufferId, final int bufferSize) {
    return new MemoryBuffer(bufferId, ByteBuffer.allocate(bufferSize), bufferSize);
  }

  @Test
  public void testInitializeMemBuf() {
    final int bufferId = 0xCAFE;
    final int bufferSize = 0x1000;
    final MemoryBuffer memBuffer = allocateBuffer(bufferId, bufferSize);

    assertEquals(bufferId, memBuffer.getId());
    assertEquals(bufferSize, memBuffer.getBufferSize());
    assertEquals(0, memBuffer.getRemainingDataSize());
  }

  @Test
  public void testSingleReadWrite() {
    final int bufferId = 0xCAFE;
    final int bufferSize = 0x100000;

    rwBufferTest.testSingleReadWrite(allocateBuffer(bufferId, bufferSize));
  }

  @Test
  public void testMultipleRead() {
    final int bufferId = 0xCAFE;
    final int bufferSize = 0x100000;
    final int readChunkSize = 0x1000;

    rwBufferTest.testMultipleRead(allocateBuffer(bufferId, bufferSize), readChunkSize);
  }

  @Test
  public void testMultipleWrite() {
    final int bufferId = 0xCAFE;
    final int bufferSize = 0x100000;
    final int writeChunkSize = 0x1000;

    rwBufferTest.testMultipleWrite(allocateBuffer(bufferId, bufferSize), writeChunkSize);
  }

  @Test
  public void testSeekFirst() {
    final int bufferId = 0xCAFE;
    final int bufferSize = 0x100000;

    rwBufferTest.testSeekFirst(allocateBuffer(bufferId, bufferSize));
  }

}
