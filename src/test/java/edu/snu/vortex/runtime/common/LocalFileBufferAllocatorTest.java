package edu.snu.vortex.runtime.common;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class LocalFileBufferAllocatorTest {
  private static final String ROOT_FILE_SPACE = "./";
  private static final String FILE_SPACE_PREFIX = "file-space-";
  private static final int NUM_FILE_SPACE = 0x10;
  private static final int NUM_SUB_DIRS_PER_FSPACE = 0x40;
  private BufferAllocatorCommonTest bufAllocTest;
  private LocalFileBufferAllocator fileBufAllocator;
  private LocalFileManager fileManager;


  private List<File> createFileSpaces(final int numSubFileSpaces) {
    final List<File> fileSpaces = new ArrayList<>(numSubFileSpaces);

    for (int i = 0; i < numSubFileSpaces; i++) {
      final File fileSpace = new File(ROOT_FILE_SPACE + FILE_SPACE_PREFIX + i);
      fileSpace.mkdir();
      fileSpace.deleteOnExit();
      fileSpaces.add(fileSpace);
    }

    return fileSpaces;
  }

  @Before
  public void setup() throws IOException {
    fileManager = new LocalFileManager(createFileSpaces(NUM_FILE_SPACE), NUM_SUB_DIRS_PER_FSPACE);
    fileBufAllocator = new LocalFileBufferAllocator(fileManager);
    bufAllocTest = new BufferAllocatorCommonTest(fileBufAllocator);
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
