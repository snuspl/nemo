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
 * It writes and reads synchronously.
 */
@ThreadSafe
final class LocalFileStore extends FileStore {

  private final Map<String, FilePartition> partitionIdToData;
  private final ExecutorService executorService;

  @Inject
  private LocalFileStore(@Parameter(JobConf.FileDirectory.class) final String fileDirectory,
                         @Parameter(JobConf.BlockSize.class) final int blockSizeInKb,
                         @Parameter(JobConf.LocalFileStoreNumThreads.class) final int numThreads,
                         final InjectionFuture<PartitionManagerWorker> partitionManagerWorker) {
    super(blockSizeInKb, fileDirectory, partitionManagerWorker);
    this.partitionIdToData = new ConcurrentHashMap<>();
    new File(fileDirectory).mkdirs();
    executorService = Executors.newFixedThreadPool(numThreads);
  }

  /**
   * Retrieves data in a specific hash range from a partition.
   * @see PartitionStore#retrieveData(String, HashRange).
   */
  @Override
  public CompletableFuture<Optional<Iterable<Element>>> retrieveData(final String partitionId,
                                                                     final HashRange hashRange) {
    // Deserialize the target data in the corresponding file and pass it as a local data.
    final FilePartition partition = partitionIdToData.get(partitionId);
    final Supplier<Optional<Iterable<Element>>> supplier = () -> {
      if (partition == null) {
        return Optional.empty();
      }
      try {
        return Optional.of(partition.retrieveInHashRange(hashRange));
      } catch (final IOException e) {
        throw new PartitionFetchException(e);
      }
    };
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  /**
   * Saves an iterable of data blocks to a partition.
   * @see PartitionStore#putBlocks(String, Iterable).
   */
  @Override
  public CompletableFuture<Optional<List<Long>>> putBlocks(final String partitionId,
                                                           final Iterable<Block> blocks) {
    final Supplier<Optional<List<Long>>> supplier = () -> {
      final Coder coder = getCoderFromWorker(partitionId);
      final List<Long> blockSizeList;
      final LocalFileMetadata metadata = new LocalFileMetadata();

      try (final FilePartition partition =
               new FilePartition(coder, partitionIdToFilePath(partitionId), metadata)) {
        partitionIdToData.putIfAbsent(partitionId, partition);

        // Serialize and write the given blocks.
        blockSizeList = putBlocks(coder, partition, blocks);
      } catch (final IOException e) {
        throw new PartitionWriteException(e);
      }

      return Optional.of(blockSizeList);
    };
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  /**
   * Removes the file that the target partition is stored.
   *
   * @param partitionId of the partition.
   * @return whether the partition exists or not.
   */
  @Override
  public CompletableFuture<Boolean> removePartition(final String partitionId) {
    final FilePartition serializedPartition = partitionIdToData.remove(partitionId);
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

  @Override
  public List<FileArea> getFileAreas(final String partitionId, final HashRange hashRange) {
    try {
      final FilePartition partition = partitionIdToData.get(partitionId);
      if (partition == null) {
        throw new PartitionFetchException(new Exception(String.format("%s does not exists", partitionId)));
      } else {
        return partition.asFileAreas(hashRange);
      }
    } catch (final IOException e) {
      throw new PartitionFetchException(e);
    }
  }
}
