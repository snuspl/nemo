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
import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.exception.PartitionFetchException;
import edu.snu.vortex.runtime.exception.PartitionWriteException;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMasterMap;
import edu.snu.vortex.runtime.executor.data.metadata.LocalFileMetadata;
import edu.snu.vortex.runtime.executor.data.partition.FilePartition;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * Stores partitions in local files.
 */
@ThreadSafe
public final class LocalFileStore extends FileStore {
  public static final String SIMPLE_NAME = "LocalFileStore";

  private final Map<String, FilePartition> partitionIdToFilePartition;
  private final ExecutorService executorService; // Executor service for non-blocking computation.
  private final String executorId;
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  @Inject
  private LocalFileStore(@Parameter(JobConf.FileDirectory.class) final String fileDirectory,
                         @Parameter(JobConf.LocalFileStoreNumThreads.class) final int numThreads,
                         @Parameter(JobConf.ExecutorId.class) final String executorId,
                         final InjectionFuture<PartitionManagerWorker> partitionManagerWorker,
                         final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    super(fileDirectory, partitionManagerWorker);
    this.partitionIdToFilePartition = new ConcurrentHashMap<>();
    this.executorId = executorId;
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.executorService = Executors.newFixedThreadPool(numThreads);
    new File(fileDirectory).mkdirs();
  }

  /**
   * Retrieves data in a specific hash range from a partition.
   * @see PartitionStore#getElements(String, HashRange).
   */
  @Override
  public Optional<CompletableFuture<Iterable<Element>>> getElements(final String partitionId,
                                                                    final HashRange hashRange) {
    // Deserialize the target data in the corresponding file.
    final FilePartition partition = partitionIdToFilePartition.get(partitionId);
    if (partition == null) {
      return Optional.empty();
    } else {
      try {
        return Optional.of(partition.getElementsInRange(hashRange));
      } catch (final IOException retrievalException) {
        throw new PartitionFetchException(retrievalException);
      }
    }
  }

  /**
   * Saves an iterable of data blocks to a partition.
   * @see PartitionStore#putBlocks(String, Iterable, boolean).
   */
  @Override
  public CompletableFuture<Optional<List<Long>>> putBlocks(final String partitionId,
                                                           final Iterable<Block> blocks,
                                                           final boolean commitPerBlock) {
    final Supplier<Optional<List<Long>>> supplier = () -> {
      final Coder coder = getCoderFromWorker(partitionId);
      final List<Long> blockSizeList;
      final LocalFileMetadata metadata = new LocalFileMetadata(commitPerBlock);
      final FilePartition partition = partitionIdToFilePartition.computeIfAbsent(partitionId, absentPartitionId -> {
        // If this partition is newly created, report the creation to the master.
        reportPartitionCreation(partitionId, executorId, executorId, persistentConnectionToMasterMap);
        return new FilePartition(coder, partitionIdToFilePath(partitionId), metadata, executorService);
      });

      try {
        // Serialize and write the given blocks.
        blockSizeList = putBlocks(coder, partition, blocks);
      } catch (final IOException writeException) {
        partition.commit();
        throw new PartitionWriteException(writeException);
      }

      return Optional.of(blockSizeList);
    };
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  /**
   * @see PartitionStore#commitPartition(String).
   */
  @Override
  public void commitPartition(final String partitionId) throws PartitionWriteException {
    final FilePartition partition = partitionIdToFilePartition.get(partitionId);
    if (partition != null) {
      partition.commit();
    } else {
      throw new PartitionWriteException(new Throwable("There isn't any partition with id " + partitionId));
    }
  }

  /**
   * Removes the file that the target partition is stored.
   *
   * @param partitionId of the partition.
   * @return whether the partition exists or not.
   */
  @Override
  public CompletableFuture<Boolean> removePartition(final String partitionId) {
    final FilePartition serializedPartition = partitionIdToFilePartition.remove(partitionId);
    if (serializedPartition == null) {
      return CompletableFuture.completedFuture(false);
    }
    final Supplier<Boolean> supplier = () -> {
      try {
        serializedPartition.deleteFile();
      } catch (final IOException e) {
        throw new PartitionFetchException(e);
      }
      return true;
    };
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  /**
   * @see FileStore#getFileAreas(String, HashRange).
   */
  @Override
  public CompletableFuture<Iterable<FileArea>> getFileAreas(final String partitionId,
                                                            final HashRange hashRange) {
    final FilePartition partition = partitionIdToFilePartition.get(partitionId);
    if (partition == null) {
      throw new PartitionFetchException(new Throwable(String.format("%s does not exists", partitionId)));
    } else {
      return partition.asFileAreas(hashRange);
    }
  }
}
