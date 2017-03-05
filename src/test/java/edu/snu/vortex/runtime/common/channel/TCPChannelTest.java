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
package edu.snu.vortex.runtime.common.channel;


import edu.snu.vortex.runtime.common.*;
import edu.snu.vortex.runtime.executor.DataTransferManager;
import edu.snu.vortex.runtime.master.transfer.DataTransferManagerMaster;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertTrue;

public final class TCPChannelTest {
  private static final Logger LOG = Logger.getLogger( TCPChannelTest.class.getName() );
  private static final long DEFAULT_BUF_SIZE = 0x8000;
  private static final String SENDER_SIDE_TRANSFER_MANAGER_ID = "SenderSideTransferManager";
  private static final String RECEIVER_SIDE_TRANSFER_MANAGER_ID = "ReceiverSideTransferManager";
  private static final String SENDER_TASK_ID = "SenderTask";
  private static final String RECEIVER_TASK_ID = "ReceiverTask";
  private static final String CHANNEL_ID = SENDER_TASK_ID + "-" + RECEIVER_TASK_ID;

  private DataTransferManagerMaster transferManagerMaster;
  private DataTransferManager senderSideTransferManager;
  private DataTransferManager receiverSideTransferManager;
  private LocalFileManager localFileManager;
  private MemoryBufferAllocator memoryBufferAllocator;
  private LocalFileBufferAllocator localFileBufferAllocator;
  private DataBufferAllocator bufferAllocator;

  private static final String ROOT_FILE_SPACE = "./";
  private static final String FILE_SPACE_PREFIX = "file-space-";
  private static final int NUM_FILE_SPACE = 0x10;
  private static final int NUM_SUB_DIRS_PER_FSPACE = 0x40;

  private static final int NUM_DATA_STRINGS_PER_DATA_SET = 0x10000;
  private static final int NUM_DATA_SET = 0x1;


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
    this.transferManagerMaster = new DataTransferManagerMaster();
    this.senderSideTransferManager = new DataTransferManager(SENDER_SIDE_TRANSFER_MANAGER_ID,
        transferManagerMaster);
    this.receiverSideTransferManager = new DataTransferManager(RECEIVER_SIDE_TRANSFER_MANAGER_ID,
        transferManagerMaster);
    this.memoryBufferAllocator = new MemoryBufferAllocator((int) DEFAULT_BUF_SIZE);
    this.localFileManager = new LocalFileManager(createFileSpaces(NUM_FILE_SPACE), NUM_SUB_DIRS_PER_FSPACE);
    this.localFileBufferAllocator = new LocalFileBufferAllocator(localFileManager);
    this.bufferAllocator = new DataBufferAllocator(memoryBufferAllocator, localFileBufferAllocator);
  }

  @Test
  public void testTransferSingleDataSetViaTCPChannel() {

    final MemoryChannelWriter<String> channelWriter = new MemoryChannelWriter<>(CHANNEL_ID, SENDER_TASK_ID, RECEIVER_TASK_ID);
    final MemoryChannelReader<String> channelReader = new MemoryChannelReader<>(CHANNEL_ID, SENDER_TASK_ID, RECEIVER_TASK_ID);
    channelWriter.initialize(bufferAllocator,
        DataBufferType.LOCAL_FILE,
        DEFAULT_BUF_SIZE,
        senderSideTransferManager);
    channelReader.initialize(this.bufferAllocator,
        DataBufferType.LOCAL_FILE,
        DEFAULT_BUF_SIZE,
        receiverSideTransferManager);

    try {
      final List<String> originalData = generateDataset(NUM_DATA_STRINGS_PER_DATA_SET);
      LOG.log(Level.INFO, "[" + this.getClass().getSimpleName() + "] write " + originalData.size() + " strings");
      channelWriter.write(originalData);
      channelWriter.flush();

      final List<String> receivedData = channelReader.read();
      LOG.log(Level.INFO, "[" + this.getClass().getSimpleName() + "] read " + receivedData.size() + " strings");
      assertTrue(compareStringLists(originalData, receivedData));
      LOG.log(Level.INFO, "[" + this.getClass().getSimpleName() + "] verified " + originalData.size() + " strings");
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

/*
  @Test
  public void testTransferMultipleDataSetViaTCPChannel() {
    final MemoryChannelWriter<String> channelWriter = new MemoryChannelWriter<>(CHANNEL_ID, SENDER_TASK_ID, RECEIVER_TASK_ID);
    final MemoryChannelReader<String> channelReader = new MemoryChannelReader<>(CHANNEL_ID, SENDER_TASK_ID, RECEIVER_TASK_ID);
    channelWriter.initialize(bufferAllocator,
        DataBufferType.LOCAL_FILE,
        DEFAULT_BUF_SIZE,
        senderSideTransferManager);
    channelReader.initialize(this.bufferAllocator,
        DataBufferType.LOCAL_FILE,
        DEFAULT_BUF_SIZE,
        receiverSideTransferManager);


    try {
      final List<String> answerDataSet = new ArrayList<>();

      for (int i = 0; i < NUM_DATA_SET; i++) {
        final List<String> originalData = generateDataset(NUM_DATA_STRINGS_PER_DATA_SET);
        channelWriter.write(originalData);
        answerDataSet.addAll(originalData);
      }

      channelWriter.flush();

      final List<String> receivedData = channelReader.read();
      assertTrue(compareStringLists(answerDataSet, receivedData));

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
*/
}
