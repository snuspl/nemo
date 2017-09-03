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
import edu.snu.vortex.runtime.executor.PersistentConnectionToMaster;
import edu.snu.vortex.runtime.executor.data.metadata.RemoteFileMetadata;
import edu.snu.vortex.runtime.executor.data.partition.FilePartition;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Stores partitions in a mounted GlusterFS volume.
 * Because the data is stored in remote files and globally accessed by multiple nodes,
 * each access (write, read, or deletion) for a file needs one instance of {@link FilePartition}.
 * It supports concurrent write for a single file, but each writer has to have separate instance.
 * These accesses are judiciously synchronized by the metadata server in master.
 */
@ThreadSafe
final class GlusterFileStore extends FileStore implements RemoteFileStore {

  private final ExecutorService executorService;
  private final PersistentConnectionToMaster persistentConnectionToMaster;
  private final String executorId;

  @Inject
  private GlusterFileStore(@Parameter(JobConf.GlusterVolumeDirectory.class) final String volumeDirectory,
                           @Parameter(JobConf.BlockSize.class) final int blockSizeInKb,
                           @Parameter(JobConf.JobId.class) final String jobId,
                           @Parameter(JobConf.GlusterFileStoreNumThreads.class) final int numThreads,
                           @Parameter(JobConf.ExecutorId.class) final String executorId,
                           final InjectionFuture<PartitionManagerWorker> partitionManagerWorker,
                           final PersistentConnectionToMaster persistentConnectionToMaster) {
    super(blockSizeInKb, volumeDirectory + "/" + jobId, partitionManagerWorker);
    new File(getFileDirectory()).mkdirs();
    this.executorService = Executors.newFixedThreadPool(numThreads);
    this.executorId = executorId;
    this.persistentConnectionToMaster = persistentConnectionToMaster;
  }

  /**
   * Retrieves a deserialized partition of data through remote disks.
   * @see PartitionStore#retrieveData(String, HashRange).
   */
  @Override
  public CompletableFuture<Optional<Iterable<Element>>> retrieveData(final String partitionId,
                                                                     final HashRange hashRange) {
    final Supplier<Optional<Iterable<Element>>> supplier = () -> {
      // Deserialize the target data in the corresponding file and pass it as a local data.
      final Coder coder = getCoderFromWorker(partitionId);
      final String filePath = partitionIdToFilePath(partitionId);
      try {
        if (new File(filePath).isFile()) {
          final RemoteFileMetadata metadata =
              RemoteFileMetadata.openToRead(partitionId, executorId, persistentConnectionToMaster);
          final FilePartition partition = new FilePartition(coder, filePath, metadata);
          return Optional.of(partition.retrieveInHashRange(hashRange));
        } else {
          return Optional.empty();
        }
      } catch (final IOException | InterruptedException | ExecutionException e) {
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
      final String filePath = partitionIdToFilePath(partitionId);

      try {
        final RemoteFileMetadata metadata =
            RemoteFileMetadata.openToWrite(false, false, partitionId, executorId, persistentConnectionToMaster);
        final FilePartition partition = new FilePartition(coder, filePath, metadata);
        // Serialize and write the given blocks.
        final List<Long> blockSizeList = putBlocks(coder, partition, blocks);
        return Optional.of(blockSizeList);
      } catch (final IOException e) {
        throw new PartitionWriteException(e);
      }
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
    final Supplier<Boolean> supplier = () -> {
      final Coder coder = getCoderFromWorker(partitionId);
      final String filePath = partitionIdToFilePath(partitionId);

      try {
        if (new File(filePath).isFile()) {
          final RemoteFileMetadata metadata =
              RemoteFileMetadata.openToRead(partitionId, executorId, persistentConnectionToMaster);
          final FilePartition partition = new FilePartition(coder, filePath, metadata);
          partition.deleteFile();
          return true;
        } else {
          return false;
        }
      } catch (final IOException | InterruptedException | ExecutionException e) {
        throw new PartitionFetchException(e);
      }
    };
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  @Override
  public List<FileArea> getFileAreas(final String partitionId, final HashRange hashRange) {
    final Coder coder = getCoderFromWorker(partitionId);
    final String filePath = partitionIdToFilePath(partitionId);
    try {
      if (new File(filePath).isFile()) {
        final RemoteFileMetadata metadata =
            RemoteFileMetadata.openToRead(partitionId, executorId, persistentConnectionToMaster);
        final FilePartition partition = new FilePartition(coder, filePath, metadata);
        return partition.asFileAreas(hashRange);
      } else {
        throw new PartitionFetchException(new Exception(String.format("%s does not exists", partitionId)));
      }
    } catch (final IOException | InterruptedException | ExecutionException e) {
      throw new PartitionFetchException(e);
    }
  }
}
