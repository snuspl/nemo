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
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class ReadWriteBufferCommonTest {

  private boolean compareByteBufs(final ByteBuffer buffer1, final ByteBuffer buffer2, final int bufferSize) {
    for (int idx = 0; idx < bufferSize; idx++) {
      final byte value1 = buffer1.get(idx);
      final byte value2 = buffer2.get(idx);
      if (value1 != value2) {
        return false;
      }
    }

    return true;
  }

  public void testSingleReadWrite(ReadWriteBuffer rwBuffer) {
    // Constraint upon parameters.
    assertTrue(rwBuffer.getBufferSize() <= Integer.MAX_VALUE);

    final int bufferId = rwBuffer.getId();
    final int bufferSize = (int) rwBuffer.getBufferSize();
    final ByteBuffer writeBuffer = ByteBuffer.allocate(bufferSize);
    final ByteBuffer readBuffer = ByteBuffer.allocate(bufferSize);
    final Random rand = new Random(bufferId);

    for (int idx = 0; idx < bufferSize; idx++) {
      writeBuffer.put(idx, ((byte) rand.nextInt()));
    }

    final int writeSize = rwBuffer.writeNext(writeBuffer.array(), bufferSize);
    assertEquals("1", bufferSize, writeSize);

    // Reset the buffer seek to zero to read from the beginning.
    rwBuffer.seekFirst();

    final int readSize = rwBuffer.readNext(readBuffer.array(), bufferSize);
    assertEquals("2", bufferSize, readSize);

    assertTrue("3", compareByteBufs(writeBuffer, readBuffer, bufferSize));
  }

  public void testMultipleRead(final ReadWriteBuffer rwBuffer, final int readChunkSize) {
    // Constraint upon parameters.
    assertTrue(rwBuffer.getBufferSize() <= Integer.MAX_VALUE);
    assertTrue((rwBuffer.getBufferSize() % readChunkSize) == 0);

    final int bufferId = rwBuffer.getId();
    final int bufferSize = (int) rwBuffer.getBufferSize();
    final int numRounds = (bufferSize / readChunkSize);
    final ByteBuffer writeBuffer = ByteBuffer.allocate(bufferSize);
    final ByteBuffer readBuffer = ByteBuffer.allocate(bufferSize);
    final ByteBuffer readChunkBuffer = ByteBuffer.allocate(readChunkSize);
    final Random rand = new Random(bufferId);

    assertTrue(bufferSize % readChunkSize == 0);
    for (int idx = 0; idx < bufferSize; idx++) {
      writeBuffer.put(idx, ((byte) rand.nextInt()));
    }

    final int writeSize = rwBuffer.writeNext(writeBuffer.array(), bufferSize);
    assertEquals(writeSize, bufferSize);

    // Reset the buffer seek to zero to read from the beginning.
    rwBuffer.seekFirst();

    for (int round = 0; round < numRounds; round++) {
      final int readSize = rwBuffer.readNext(readChunkBuffer.array(), readChunkSize);
      assertEquals(readChunkSize, readSize);

      // Copy the chunk into the read buffer repeatedly as many as the number of rounds.
      // And the buffer will be used for evaluation.
      final int baseIdx = round * readChunkSize;
      for (int idx = 0; idx < readChunkSize; idx++) {
        readBuffer.put(baseIdx + idx, readChunkBuffer.get(idx));
      }
    }

    assertEquals(0, rwBuffer.getRemainingDataSize());
    assertTrue(compareByteBufs(writeBuffer, readBuffer, bufferSize));
  }

  public void testMultipleWrite(final ReadWriteBuffer rwBuffer, final int writeChunkSize) {
    // Constraint upon parameters.
    assertTrue(rwBuffer.getBufferSize() <= Integer.MAX_VALUE);
    assertTrue("1", (rwBuffer.getBufferSize() % writeChunkSize) == 0);

    final int bufferId = rwBuffer.getId();
    final int bufferSize = (int) rwBuffer.getBufferSize();
    final int numRounds = (bufferSize / writeChunkSize);
    final ByteBuffer writeBuffer = ByteBuffer.allocate(bufferSize);
    final ByteBuffer readBuffer = ByteBuffer.allocate(bufferSize);
    final ByteBuffer writeChunkBuffer = ByteBuffer.allocate(writeChunkSize);
    final Random rand = new Random(bufferId);

    for (int idx = 0; idx < writeChunkSize; idx++) {
      writeChunkBuffer.put(idx, ((byte) rand.nextInt()));
    }

    // Copy the chunk into the write buffer repeatedly as many as the number of rounds.
    // And the buffer will be used for evaluation.
    for (int round = 0; round < numRounds; round++) {
      final int baseIdx = round * writeChunkSize;
      for (int idx = 0; idx < writeChunkSize; idx++) {
        writeBuffer.put(baseIdx + idx, writeChunkBuffer.get(idx));
      }
    }

    for (int round = 0; round < numRounds; round++) {
      final int writeSize = rwBuffer.writeNext(writeChunkBuffer.array(), writeChunkSize);
      assertEquals("2", writeChunkSize, writeSize);
    }

    // Reset the buffer seek to zero to read from the beginning.
    rwBuffer.seekFirst();

    final int readSize = rwBuffer.readNext(readBuffer.array(), bufferSize);
    assertEquals("3", bufferSize, readSize);

    assertEquals(0, rwBuffer.getRemainingDataSize());
    assertTrue("4", compareByteBufs(writeBuffer, readBuffer, bufferSize));
  }

  public void testSeekFirst(final ReadWriteBuffer rwBuffer) {
    // Constraint upon parameters.
    assertTrue(rwBuffer.getBufferSize() <= Integer.MAX_VALUE);

    final int bufferId = rwBuffer.getId();
    final int bufferSize = (int) rwBuffer.getBufferSize();
    final ByteBuffer writeBuffer = ByteBuffer.allocate(bufferSize);
    final ByteBuffer readBuffer = ByteBuffer.allocate(bufferSize);
    final Random rand = new Random(bufferId);

    for (int idx = 0; idx < bufferSize; idx++) {
      writeBuffer.put(idx, ((byte) rand.nextInt()));
    }

    final int writeSize = rwBuffer.writeNext(writeBuffer.array(), bufferSize);
    assertEquals(bufferSize, writeSize);

    // Reset the buffer seek to zero to read from the beginning.
    rwBuffer.seekFirst();

    // Read for the first time.
    int readSize = rwBuffer.readNext(readBuffer.array(), bufferSize);
    assertEquals(bufferSize, readSize);

    assertEquals(0, rwBuffer.getRemainingDataSize());
    assertTrue(compareByteBufs(writeBuffer, readBuffer, bufferSize));

    // Reset the buffer seek to zero to read from the beginning.
    rwBuffer.seekFirst();

    // Read second time.
    readSize = rwBuffer.readNext(readBuffer.array(), bufferSize);
    assertEquals(bufferSize, readSize);

    assertEquals(0, rwBuffer.getRemainingDataSize());
    assertTrue(compareByteBufs(writeBuffer, readBuffer, bufferSize));
  }

}
