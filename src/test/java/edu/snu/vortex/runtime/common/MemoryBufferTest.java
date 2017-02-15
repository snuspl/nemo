package edu.snu.vortex.runtime.common;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link MemoryBuffer}.
 */
public class MemoryBufferTest {

  private MemoryBuffer allocateBuffer(final int bufferId, final int bufferSize) {
    return new MemoryBuffer(bufferId, ByteBuffer.allocate(bufferSize), bufferSize);
  }

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

  @Test
  public void testInitializeMemBuf() {
    final int bufferId = 0xBFFE;
    final int bufferSize = 0x1000;
    final MemoryBuffer buffer = allocateBuffer(bufferId, bufferSize);

    assertEquals(bufferId, buffer.getId());
    assertEquals(bufferSize, buffer.getBufferSize());
    assertEquals(0, buffer.getRemainingDataSize());
  }

  @Test
  public void testSingleReadWrite() {
    final int bufferId = 0xBFFE;
    final int bufferSize = 0x1000;
    final ByteBuffer writeBuffer = ByteBuffer.allocate(bufferSize);
    final ByteBuffer readBuffer = ByteBuffer.allocate(bufferSize);
    final MemoryBuffer memBuffer = allocateBuffer(bufferId, bufferSize);
    final Random rand = new Random(bufferId);

    for (int idx = 0; idx < bufferSize; idx++) {
      writeBuffer.put(idx, ((byte) rand.nextInt()));
    }

    final int writeSize = memBuffer.writeNext(writeBuffer.array(), bufferSize);
    assertEquals(bufferSize, writeSize);

    final int readSize = memBuffer.readNext(readBuffer.array(), bufferSize);
    assertEquals(bufferSize, readSize);

    assertTrue(compareByteBufs(writeBuffer, readBuffer, bufferSize));
  }

  @Test
  public void testMultipleRead() {
    final int bufferId = 0xBFFE;
    final int bufferSize = 0x8192;
    final int readChunkSize = 0x1024;
    final int numRounds = (bufferSize / readChunkSize);
    final ByteBuffer writeBuffer = ByteBuffer.allocate(bufferSize);
    final ByteBuffer readBuffer = ByteBuffer.allocate(bufferSize);
    final ByteBuffer readChunkBuffer = ByteBuffer.allocate(readChunkSize);
    final MemoryBuffer memBuffer = allocateBuffer(bufferId, bufferSize);
    final Random rand = new Random(bufferId);

    assertTrue(bufferSize % readChunkSize == 0);
    for (int idx = 0; idx < bufferSize; idx++) {
      writeBuffer.put(idx, ((byte) rand.nextInt()));
    }

    final int writeSize = memBuffer.writeNext(writeBuffer.array(), bufferSize);
    assertEquals(writeSize, bufferSize);

    for (int round = 0; round < numRounds; round++) {
      final int readSize = memBuffer.readNext(readChunkBuffer.array(), readChunkSize);
      readBuffer.put(readChunkBuffer);
    }

    assertTrue(compareByteBufs(writeBuffer, readBuffer, bufferSize));
  }

  @Test
  public void testMultipleWrite() {
    final int bufferId = 0xBFFE;
    final int bufferSize = 0x8192;
    final int writeChunkSize = 0x1024;
    final int numRounds = (bufferSize / writeChunkSize);
    final ByteBuffer writeBuffer = ByteBuffer.allocate(bufferSize);
    final ByteBuffer readBuffer = ByteBuffer.allocate(bufferSize);
    final ByteBuffer writeChunkBuffer = ByteBuffer.allocate(writeChunkSize);
    final MemoryBuffer memBuffer = allocateBuffer(bufferId, bufferSize);
    final Random rand = new Random(bufferId);

    assertTrue(bufferSize % writeChunkSize == 0);
    for (int idx = 0; idx < writeChunkSize; idx++) {
      writeChunkBuffer.put(idx, ((byte) rand.nextInt()));
    }

    // Copy the chunk into the write buffer repeatedly as many as the number of rounds.
    // And the buffer will be used for evaluation.
    for (int round = 0; round < numRounds; round++) {
      writeBuffer.put(writeChunkBuffer);
    }

    for (int round = 0; round < numRounds; round++) {
      final int writeSize = memBuffer.writeNext(writeChunkBuffer.array(), writeChunkSize);
      assertEquals(writeChunkSize, writeSize);
    }

    final int readSize = memBuffer.readNext(readBuffer.array(), bufferSize);
    assertEquals(bufferSize, readSize);

    assertTrue(compareByteBufs(writeBuffer, readBuffer, bufferSize));
  }

}
