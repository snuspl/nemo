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


import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;

import static org.junit.Assert.assertEquals;

public final class LocalFileBufferTest {
  private final ReadWriteBufferCommonTest rwBufferTest = new ReadWriteBufferCommonTest();

  private LocalFileBuffer allocateBuffer(final int bufferId, final long bufferSize) {
    try {
      final File file = new File(this.getClass().getSimpleName());
      file.createNewFile();
      file.deleteOnExit();

      return new LocalFileBuffer(bufferId, file, bufferSize);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testInitializeMemBuf() {
    final int bufferId = 0xBFFE;
    final int bufferSize = 0x1000;
    final LocalFileBuffer fileBuffer = allocateBuffer(bufferId, bufferSize);

    assertEquals(bufferId, fileBuffer.getId());
    assertEquals(bufferSize, fileBuffer.getBufferSize());
    assertEquals(0, fileBuffer.getRemainingDataSize());
  }

  @Test
  public void testSingleReadWrite() {
    final int bufferId = 0xBFFE;
    final int bufferSize = 0x1000;

    rwBufferTest.testSingleReadWrite(allocateBuffer(bufferId, bufferSize));
  }

  @Test
  public void testMultipleRead() {
    final int bufferId = 0xBFFE;
    final int bufferSize = 0x8000;
    final int readChunkSize = 0x1000;

    rwBufferTest.testMultipleRead(allocateBuffer(bufferId, bufferSize), readChunkSize);
  }

  @Test
  public void testMultipleWrite() {
    final int bufferId = 0xBFFE;
    final int bufferSize = 0x8000;
    final int writeChunkSize = 0x1000;

    rwBufferTest.testMultipleWrite(allocateBuffer(bufferId, bufferSize), writeChunkSize);
  }

  @Test
  public void testSeekFirst() {
    final int bufferId = 0xBFFE;
    final int bufferSize = 0x8000;

    rwBufferTest.testSeekFirst(allocateBuffer(bufferId, bufferSize));
  }


}
