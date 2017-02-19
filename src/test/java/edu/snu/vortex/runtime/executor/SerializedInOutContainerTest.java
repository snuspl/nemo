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

import edu.snu.vortex.runtime.common.*;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public final class SerializedInOutContainerTest {
  private static final String ROOT_FILE_SPACE = "./";
  private static final String FILE_SPACE_PREFIX = "file-space-";
  private static final int NUM_DATA_STRINGS_PER_DATA_SET = 0x1000;
  private static final int NUM_DATA_SET = 0x10;
  private static final int NUM_FILE_SPACE = 0x10;
  private static final int NUM_SUB_DIRS_PER_FSPACE = 0x40;
  private static final int DEFAULT_BUF_SIZE = 0x400;
  private static final DataBufferType DATA_BUFFER_TYPE = DataBufferType.LOCAL_MEMORY;

  private SerializedOutputContainer serOutputContainer;
  private SerializedInputContainer serInputContainer;

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

  private List<String> generateDataset(final int numData) {
    final Random rand = new Random();
    final List<String> data = new ArrayList<>();

    for (int i = 0; i < numData; i++) {
      data.add(Integer.toHexString(rand.nextInt()));
    }

    return data;
  }

  private boolean compareStringLists(final List<String> list1, final List<String> list2) {
    if (list1.size() != list2.size()) {
      return false;
    }

    for (int idx = 0; idx < list1.size(); idx++) {
      if (list1.get(idx).compareTo(list2.get(idx)) != 0) {
        return false;
      }
    }

    return true;
  }

  @Before
  public void setup() throws IOException {
    final LocalFileManager localFileManager = new LocalFileManager(createFileSpaces(NUM_FILE_SPACE),
                                                                  NUM_SUB_DIRS_PER_FSPACE);
    final MemoryBufferAllocator memBufAllocator = new MemoryBufferAllocator(DEFAULT_BUF_SIZE);
    final LocalFileBufferAllocator fileBufAllocator = new LocalFileBufferAllocator(localFileManager);
    final DataBufferAllocator bufferAllocator = new DataBufferAllocator(memBufAllocator, fileBufAllocator);

    serOutputContainer = new SerializedOutputContainer(bufferAllocator, DATA_BUFFER_TYPE, DEFAULT_BUF_SIZE);
    serInputContainer = new SerializedInputContainer(bufferAllocator, DATA_BUFFER_TYPE);
  }

  @Test
  public void testSerializeAndDeserializeSingleListOfStrings() {
    final List<String> originalData = generateDataset(NUM_DATA_STRINGS_PER_DATA_SET);
    byte[] byteBuffer = new byte[DEFAULT_BUF_SIZE];

    try {
      final ObjectOutputStream out = new ObjectOutputStream(serOutputContainer);
      out.writeObject(originalData);
      out.close();

      transferDataBetweenInOutContainers(serOutputContainer, serInputContainer);

      final ObjectInputStream in = new ObjectInputStream(serInputContainer);
      final List<String> deserializedData = (List<String>) in.readObject();
      in.close();

      serOutputContainer.clear();
      serInputContainer.clear();

      assertTrue(compareStringLists(originalData, deserializedData)); // Evaluation.

    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }

  }

  private void transferDataBetweenInOutContainers(final SerializedOutputContainer srcContainer,
                                                  final SerializedInputContainer dstContainer) {
    byte[] byteBuffer = new byte[DEFAULT_BUF_SIZE];

    int numChunks = 0;
    long totalDataSize = 0;
    while (true) {
      final int chunkSize = srcContainer.copySingleDataBufferTo(byteBuffer, byteBuffer.length);
      if (chunkSize == -1) { // The output buffer is smaller than the size of output.
        byteBuffer = new byte[2 * byteBuffer.length];
        continue;
      } else if (chunkSize == 0) { // No more data to read in the output container.
        break;
      }

      dstContainer.copyInputDataFrom(byteBuffer, chunkSize);

      numChunks++;
      totalDataSize += chunkSize;
    }

    System.out.println("totalDataSize = " + totalDataSize + " / numChunks = " + numChunks);
    dstContainer.printStatistics();
  }

  @Test
  public void testSerializeAndDeserializeMultipleListsOfStrings() {
    final List<List<String>> originalDataSets = new ArrayList<>();
    final List<List<String>> deserializedDataSets = new ArrayList<>();

    for (int i = 0; i < NUM_DATA_SET; i++) {
      originalDataSets.add(generateDataset(NUM_DATA_STRINGS_PER_DATA_SET));
    }

    try {
      final ObjectOutputStream out = new ObjectOutputStream(serOutputContainer);
      for (int idx = 0; idx < NUM_DATA_SET; idx++) {
        out.writeObject(originalDataSets.get(idx));
      }
      out.close();

      transferDataBetweenInOutContainers(serOutputContainer, serInputContainer);

      final ObjectInputStream in = new ObjectInputStream(serInputContainer);
      for (int idx = 0; idx < NUM_DATA_SET; idx++) {
        deserializedDataSets.add((List<String>) in.readObject());
      }
      in.close();

      serOutputContainer.clear();
      serInputContainer.clear();

      for (int idx = 0; idx < NUM_DATA_SET; idx++) {
        assertTrue(compareStringLists(originalDataSets.get(idx), deserializedDataSets.get(idx)));
      }

    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }

  }
}
