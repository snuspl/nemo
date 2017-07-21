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
package edu.snu.vortex.runtime.executor.data;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.common.Pair;
import edu.snu.vortex.common.coder.BeamCoder;
import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.frontend.beam.BeamElement;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.executor.data.partition.Partition;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.io.FileUtils;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests write and read for {@link PartitionStore}s.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(PartitionManagerWorker.class)
public final class PartitionStoreTest {
  private static final String TMP_FILE_DIRECTORY = "./tmpFiles";
  private static final int BLOCK_SIZE = 1; // 1 KB
  private static final Coder CODER = new BeamCoder(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()));
  // Variables for scatter and gather test
  private static final int NUM_WRITE_TASKS = 3;
  private static final int NUM_READ_TASKS = 3;
  private static final int DATA_SIZE = 10000;
  private List<String> partitionIdList;
  private List<Iterable<Element>> dataInPartitionList;
  // Variables for concurrent read test
  private static final int NUM_CONC_READ_TASKS = 10;
  private static final int CONC_DATA_SIZE = 10000;
  private String concPartitionId;
  private Iterable<Element> dataInConcPartition;
  // Variables for scatter and gather in range test
  private static final int NUM_WRITE_HASH_TASKS = 2;
  private static final int NUM_READ_HASH_TASKS = 3;
  private static final int HASH_DATA_SIZE = 100;
  private static final int HASH_RANGE = 4;
  private List<String> sortedPartitionIdList;
  private List<List<Iterable<Element>>> sortedDataInPartitionList;
  private List<Pair<Integer, Integer>> readHashRangeList;
  private List<List<Iterable<Element>>> expectedDataInRange;

  @Before
  public void setUp() {
    // Generate ids and data for scatter and gather test
    final int numPartitions = NUM_WRITE_TASKS * NUM_READ_TASKS;
    final List<String> writeTaskIdList = new ArrayList<>(NUM_WRITE_TASKS);
    final List<String> readTaskIdList = new ArrayList<>(NUM_READ_TASKS);
    partitionIdList = new ArrayList<>(numPartitions);
    dataInPartitionList = new ArrayList<>(numPartitions);

    IntStream.range(0, NUM_WRITE_TASKS).forEach(
        number -> writeTaskIdList.add(RuntimeIdGenerator.generateTaskId()));
    IntStream.range(0, NUM_READ_TASKS).forEach(
        number -> readTaskIdList.add(RuntimeIdGenerator.generateTaskId()));

    IntStream.range(0, NUM_WRITE_TASKS).forEach(writeTaskNumber ->
        IntStream.range(0, NUM_READ_TASKS).forEach(readTaskNumber -> {
          final int currentNum = partitionIdList.size();
          partitionIdList.add(RuntimeIdGenerator.generatePartitionId(
              RuntimeIdGenerator.generateRuntimeEdgeId(String.valueOf(currentNum)), writeTaskNumber, readTaskNumber));
          dataInPartitionList.add(getRangedNumList(currentNum * DATA_SIZE, (currentNum + 1) * DATA_SIZE));
        }));

    // Generate ids and data for concurrent read test
    final String writeTaskId = RuntimeIdGenerator.generateTaskId();
    final List<String> concReadTaskIdList = new ArrayList<>(NUM_CONC_READ_TASKS);
    concPartitionId = RuntimeIdGenerator.generatePartitionId(
        RuntimeIdGenerator.generateRuntimeEdgeId("concurrent read"), NUM_WRITE_TASKS + NUM_READ_TASKS + 1);

    IntStream.range(0, NUM_CONC_READ_TASKS).forEach(
        number -> concReadTaskIdList.add(RuntimeIdGenerator.generateTaskId()));
    dataInConcPartition = getRangedNumList(0, CONC_DATA_SIZE);

    // Generate ids and data for scatter and gather in range test
    final int numSortedPartitions = NUM_WRITE_HASH_TASKS;
    final List<String> writeHashTaskIdList = new ArrayList<>(NUM_WRITE_HASH_TASKS);
    final List<String> readHashTaskIdList = new ArrayList<>(NUM_READ_HASH_TASKS);
    readHashRangeList = new ArrayList<>(NUM_READ_HASH_TASKS);
    sortedPartitionIdList = new ArrayList<>(numSortedPartitions);
    sortedDataInPartitionList = new ArrayList<>(numSortedPartitions);
    expectedDataInRange = new ArrayList<>(NUM_READ_HASH_TASKS);

    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(
        number -> writeHashTaskIdList.add(RuntimeIdGenerator.generateTaskId()));
    IntStream.range(0, NUM_READ_HASH_TASKS).forEach(
        number -> readHashTaskIdList.add(RuntimeIdGenerator.generateTaskId()));

    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(writeTaskNumber -> {
      sortedPartitionIdList.add(RuntimeIdGenerator.generatePartitionId(
          RuntimeIdGenerator.generateRuntimeEdgeId("scatter gather in range"),
          NUM_WRITE_TASKS + NUM_READ_TASKS + 1 + writeTaskNumber));
      final ArrayList<Iterable<Element>> sortedPartition = new ArrayList<>(HASH_RANGE);
      IntStream.range(0, HASH_RANGE).forEach(hashValue -> {
        sortedPartition.add(getFixedKeyRangedNumList(
            hashValue,
            writeTaskNumber * HASH_DATA_SIZE * HASH_RANGE + hashValue * HASH_DATA_SIZE,
            writeTaskNumber * HASH_DATA_SIZE * HASH_RANGE  + (hashValue + 1) * HASH_DATA_SIZE));
      });
      sortedDataInPartitionList.add(sortedPartition);
    });

    final int smallDataRangeEnd = 1 + NUM_READ_HASH_TASKS - NUM_WRITE_HASH_TASKS;
    readHashRangeList.add(Pair.of(0, smallDataRangeEnd));
    IntStream.range(0, NUM_READ_HASH_TASKS - 1).forEach(readTaskNumber -> {
      readHashRangeList.add(Pair.of(smallDataRangeEnd + readTaskNumber, smallDataRangeEnd + readTaskNumber + 1));
    });
    IntStream.range(0, NUM_READ_HASH_TASKS).forEach(readTaskNumber -> {
      final Pair<Integer, Integer> hashRange = readHashRangeList.get(readTaskNumber);
      final List<Iterable<Element>> expectedRangeBlocks = new ArrayList<>(NUM_WRITE_HASH_TASKS);
      IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(writeTaskNumber -> {
        final List<Iterable<Element>> appendingList = new ArrayList<>();
        IntStream.range(hashRange.left(), hashRange.right()).forEach(hashVal -> {
          appendingList.add(sortedDataInPartitionList.get(writeTaskNumber).get(hashVal));
        });
        final List<Element> concatStreamBase = new ArrayList<>();
        Stream<Element> concatStream = concatStreamBase.stream();
        for (final Iterable<Element> data : appendingList) {
          concatStream = Stream.concat(concatStream, StreamSupport.stream(data.spliterator(), false));
        }
        expectedRangeBlocks.add(concatStream.collect(Collectors.toList()));
      });
      expectedDataInRange.add(expectedRangeBlocks);
    });
  }

  /**
   * Test {@link MemoryStore}.
   */
  @Test(timeout = 10000)
  public void testLocalStore() throws Exception {
    final PartitionStore localStore = Tang.Factory.getTang().newInjector().getInstance(MemoryStore.class);
    scatterGather(localStore);
    concurrentRead(localStore);
  }

  /**
   * Test {@link LocalFileStore}.
   */
  @Test(timeout = 10000)
  public void testFileStore() throws Exception {
    final PartitionManagerWorker worker = mock(PartitionManagerWorker.class);
    when(worker.getCoder(any())).thenReturn(CODER);
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.FileDirectory.class, TMP_FILE_DIRECTORY);
    injector.bindVolatileParameter(JobConf.BlockSize.class, BLOCK_SIZE);
    injector.bindVolatileInstance(PartitionManagerWorker.class, worker);

    final PartitionStore fileStore = injector.getInstance(LocalFileStore.class);
    scatterGather(fileStore);
    concurrentRead(fileStore);
    scatterGatherInRange(fileStore);
    FileUtils.deleteDirectory(new File(TMP_FILE_DIRECTORY));
  }

  // TODO #181: Implement MemoryPartitionStore (add test for Memory, MemoryFile)
  // TODO #180: Implement DistributedStorageStore (add test for RemoteFile)

  /**
   * Tests scatter and gather for {@link PartitionStore}s.
   * Assumes following circumstances:
   * Task 1 (write)->         (read)-> Task 4
   * Task 2 (write)-> shuffle (read)-> Task 5
   * Task 3 (write)->         (read)-> Task 6
   * It checks that each writer and reader does not throw any exception
   * and the read data is identical with written data (including the order).
   */
  private void scatterGather(final PartitionStore store) {
    final ExecutorService writeExecutor = Executors.newFixedThreadPool(NUM_WRITE_TASKS);
    final ExecutorService readExecutor = Executors.newFixedThreadPool(NUM_READ_TASKS);
    final List<Future<Boolean>> writeFutureList = new ArrayList<>(NUM_WRITE_TASKS);
    final List<Future<Boolean>> readFutureList = new ArrayList<>(NUM_READ_TASKS);
    final long startNano = System.nanoTime();

    // Write concurrently
    IntStream.range(0, NUM_WRITE_TASKS).forEach(writeTaskNumber ->
        writeFutureList.add(writeExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              IntStream.range(writeTaskNumber * NUM_READ_TASKS, (writeTaskNumber + 1) * NUM_READ_TASKS).forEach(
                  partitionNumber -> store.putPartition(
                      partitionIdList.get(partitionNumber), dataInPartitionList.get(partitionNumber)));
              return true;
            } catch (final Exception e) {
              e.printStackTrace();
              return false;
            }
          }
        })));

    // Wait each writer to success
    IntStream.range(0, NUM_WRITE_TASKS).forEach(writer -> {
      try {
        assertTrue(writeFutureList.get(writer).get());
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
    final long writeEndNano = System.nanoTime();

    // Read concurrently and check whether the result is equal to the input
    IntStream.range(0, NUM_READ_TASKS).forEach(readTaskNumber ->
        readFutureList.add(readExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              IntStream.range(0, NUM_WRITE_TASKS).forEach(
                  writeTaskNumber -> {
                    final int partitionNumber = writeTaskNumber * NUM_READ_TASKS + readTaskNumber;
                    final Optional<Partition> partition = store.getPartition(partitionIdList.get(partitionNumber));
                    if (!partition.isPresent()) {
                      throw new RuntimeException("The result of getPartition(" +
                          partitionIdList.get(partitionNumber) + ") is empty");
                    }
                    final boolean exist = store.removePartition(partitionIdList.get(partitionNumber));
                    if (!exist) {
                      throw new RuntimeException("The result of removePartition(" +
                          partitionIdList.get(partitionNumber) + ") is false");
                    }
                    final Iterable<Element> getData = partition.get().asIterable();
                    assertEquals(dataInPartitionList.get(partitionNumber), getData);
                  });
              return true;
            } catch (final Exception e) {
              e.printStackTrace();
              return false;
            }
          }
        })));

    // Wait each reader to success
    IntStream.range(0, NUM_READ_TASKS).forEach(reader -> {
      try {
        assertTrue(readFutureList.get(reader).get());
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
    final long readEndNano = System.nanoTime();

    writeExecutor.shutdown();
    readExecutor.shutdown();

    System.out.println(
        "Scatter and gather - write time in millis: " + (writeEndNano - startNano) / 1000000 +
            ", Read time in millis: " + (readEndNano - writeEndNano) / 1000000 + " in store " +
            store.getClass().toString());
  }

  /**
   * Tests concurrent read for {@link PartitionStore}s.
   * Assumes following circumstances:
   *                                             -> Task 2
   * Task 1 (write)-> broadcast (concurrent read)-> ...
   *                                              -> Task 11
   * It checks that each writer and reader does not throw any exception
   * and the read data is identical with written data (including the order).
   */
  private void concurrentRead(final PartitionStore store) {
    final ExecutorService writeExecutor = Executors.newSingleThreadExecutor();
    final ExecutorService readExecutor = Executors.newFixedThreadPool(NUM_CONC_READ_TASKS);
    final Future<Boolean> writeFuture;
    final List<Future<Boolean>> readFutureList = new ArrayList<>(NUM_CONC_READ_TASKS);
    final long startNano = System.nanoTime();

    // Write a partition
    writeFuture = writeExecutor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        try {
          store.putPartition(concPartitionId, dataInConcPartition);
          return true;
        } catch (final Exception e) {
          e.printStackTrace();
          return false;
        }
      }
    });

    // Wait the writer to success
    try {
      assertTrue(writeFuture.get());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    final long writeEndNano = System.nanoTime();

    // Read the single partition concurrently and check whether the result is equal to the input
    IntStream.range(0, NUM_CONC_READ_TASKS).forEach(readTaskNumber ->
        readFutureList.add(readExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              final Optional<Partition> partition = store.getPartition(concPartitionId);
              if (!partition.isPresent()) {
                throw new RuntimeException("The result of getPartition(" +
                    concPartitionId + ") is empty");
              }
              final Iterable<Element> getData = partition.get().asIterable();
              assertEquals(dataInConcPartition, getData);
              return true;
            } catch (final Exception e) {
              e.printStackTrace();
              return false;
            }
          }
        })));

    // Wait each reader to success
    IntStream.range(0, NUM_CONC_READ_TASKS).forEach(reader -> {
      try {
        assertTrue(readFutureList.get(reader).get());
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Remove the partition
    final boolean exist = store.removePartition(concPartitionId);
    if (!exist) {
      throw new RuntimeException("The result of removePartition(" + concPartitionId + ") is false");
    }
    final long readEndNano = System.nanoTime();

    writeExecutor.shutdown();
    readExecutor.shutdown();

    System.out.println(
        "Concurrent read - write time in millis: " + (writeEndNano - startNano) / 1000000 +
            ", Read time in millis: " + (readEndNano - writeEndNano) / 1000000 + " in store " +
            store.getClass().toString());
  }

  /**
   * Tests scatter and gather in hash range for {@link PartitionStore}s.
   * Assumes following circumstances:
   * Task 1 (write (hash 0~3))->         (read (hash 0~1))-> Task 3
   * Task 2 (write (hash 0~3))-> shuffle (read (hash 2))-> Task 4
   *                                     (read (hash 3))-> Task 5
   * It checks that each writer and reader does not throw any exception
   * and the read data is identical with written data (including the order).
   */
  private void scatterGatherInRange(final PartitionStore store) {
    final ExecutorService writeExecutor = Executors.newFixedThreadPool(NUM_WRITE_HASH_TASKS);
    final ExecutorService readExecutor = Executors.newFixedThreadPool(NUM_READ_HASH_TASKS);
    final List<Future<Boolean>> writeFutureList = new ArrayList<>(NUM_WRITE_HASH_TASKS);
    final List<Future<Boolean>> readFutureList = new ArrayList<>(NUM_READ_HASH_TASKS);
    final long startNano = System.nanoTime();

    // Write concurrently
    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(writeTaskNumber ->
        writeFutureList.add(writeExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              store.putSortedPartition(
                  sortedPartitionIdList.get(writeTaskNumber), sortedDataInPartitionList.get(writeTaskNumber));
              return true;
            } catch (final Exception e) {
              e.printStackTrace();
              return false;
            }
          }
        })));

    // Wait each writer to success
    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(writer -> {
      try {
        assertTrue(writeFutureList.get(writer).get());
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
    final long writeEndNano = System.nanoTime();

    // Read concurrently and check whether the result is equal to the expected data
    IntStream.range(0, NUM_READ_HASH_TASKS).forEach(readTaskNumber ->
        readFutureList.add(readExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() {
            try {
              IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(
                  writeTaskNumber -> {
                    final Pair<Integer, Integer> hashRangeToRetrieve = readHashRangeList.get(readTaskNumber);
                    final Optional<Partition> partition = store.getPartitionInRange(
                        sortedPartitionIdList.get(writeTaskNumber),
                        hashRangeToRetrieve.left(), hashRangeToRetrieve.right());
                    if (!partition.isPresent()) {
                      throw new RuntimeException("The result of get partition" +
                          sortedPartitionIdList.get(writeTaskNumber) + " in range " + hashRangeToRetrieve.toString() +
                          " is empty");
                    }
                    final Iterable<Element> getData = partition.get().asIterable();
                    assertEquals(expectedDataInRange.get(readTaskNumber).get(writeTaskNumber), getData);
                  });
              return true;
            } catch (final Exception e) {
              e.printStackTrace();
              return false;
            }
          }
        })));

    // Wait each reader to success
    IntStream.range(0, NUM_READ_HASH_TASKS).forEach(reader -> {
      try {
        assertTrue(readFutureList.get(reader).get());
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
    final long readEndNano = System.nanoTime();

    // Remove stored partitions
    IntStream.range(0, NUM_WRITE_HASH_TASKS).forEach(writer -> {
          final boolean exist = store.removePartition(sortedPartitionIdList.get(writer));
          if (!exist) {
            throw new RuntimeException("The result of removePartition(" +
                sortedPartitionIdList.get(writer) + ") is false");
          }});

    writeExecutor.shutdown();
    readExecutor.shutdown();

    System.out.println(
        "Scatter and gather in range - write time in millis: " + (writeEndNano - startNano) / 1000000 +
            ", Read time in millis: " + (readEndNano - writeEndNano) / 1000000 + " in store " +
            store.getClass().toString());
  }

  private List<Element> getRangedNumList(final int start,
                                         final int end) {
    final List<Element> numList = new ArrayList<>(end - start);
    IntStream.range(start, end).forEach(number -> numList.add(new BeamElement<>(KV.of(number, number))));
    return numList;
  }

  private List<Element> getFixedKeyRangedNumList(final int key,
                                                 final int start,
                                                 final int end) {
    final List<Element> numList = new ArrayList<>(end - start);
    IntStream.range(start, end).forEach(number -> numList.add(new BeamElement<>(KV.of(key, number))));
    return numList;
  }
}