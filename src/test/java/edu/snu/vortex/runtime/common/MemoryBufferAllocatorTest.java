package edu.snu.vortex.runtime.common;


import org.junit.Before;
import org.junit.Test;

public class MemoryBufferAllocatorTest {
  private static final int DEFAULT_BUF_SIZE = 0x8000;
  private BufferAllocatorCommonTest bufAllocTest;
  private MemoryBufferAllocator memBufAllocator;

  @Before
  public void setup() {
    memBufAllocator = new MemoryBufferAllocator(DEFAULT_BUF_SIZE);
    bufAllocTest = new BufferAllocatorCommonTest(memBufAllocator);
  }

  @Test
  public void testAllocateSingleBuffer() {
    final long bufferSize = 0x10000;

    bufAllocTest.testAllocateSingleBuffer(bufferSize);
  }

  @Test
  public void testAllocateMultipleBuffers() {
    final long bufferSize = 0x10000;
    final long numBuffer = 0x400; // 1K.

    bufAllocTest.testAllocateMultipleBuffers(bufferSize, numBuffer);
  }
}
