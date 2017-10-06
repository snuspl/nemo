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
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.exception.PartitionFetchException;
import edu.snu.vortex.runtime.exception.PartitionWriteException;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMasterMap;
import edu.snu.vortex.runtime.executor.data.metadata.RemoteFileMetadata;
import edu.snu.vortex.runtime.executor.data.partition.FilePartition;
import io.reactivex.schedulers.Schedulers;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * Stores partitions in a mounted GlusterFS volume.
 * Because the data is stored in remote files and globally accessed by multiple nodes,
 * each access (write, read, or deletion) for a file needs one instance of {@link FilePartition}.
 * Concurrent write for a single file is supported, but each writer in different executor
 * has to have separate instance of {@link FilePartition}.
 * These accesses are judiciously synchronized by the metadata server in master.
 * TODO #485: Merge LocalFileStore and GlusterFileStore.
 * TODO #410: Implement metadata caching for the RemoteFileMetadata.
 */
@ThreadSafe
public final class GlusterFileStore extends FileStore implements RemoteFileStore {
  public static final String SIMPLE_NAME = "GlusterFileStore";

  private static final String REMOTE_FILE_STORE = "REMOTE_FILE_STORE";
  private final ExecutorService executorService; // Executor service for non-blocking computation.
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;
  private final MessageEnvironment messageEnvironment;
  private final String executorId;

  @Inject
  private GlusterFileStore(@Parameter(JobConf.GlusterVolumeDirectory.class) final String volumeDirectory,
                           @Parameter(JobConf.JobId.class) final String jobId,
                           @Parameter(JobConf.GlusterFileStoreNumThreads.class) final int numThreads,
                           @Parameter(JobConf.ExecutorId.class) final String executorId,
                           final InjectionFuture<PartitionManagerWorker> partitionManagerWorker,
                           final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                           final MessageEnvironment messageEnvironment) {
    super(volumeDirectory + "/" + jobId, partitionManagerWorker);
    new File(getFileDirectory()).mkdirs();
    this.executorService = Executors.newFixedThreadPool(numThreads);
    this.executorId = executorId;
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.messageEnvironment = messageEnvironment;
  }

  /**
   * Retrieves data in a specific hash range from a partition.
   * Because the retrieval process can be blocked by waiting the creation of the partition,
   * conducts the process on a scheduler designed to serve blocking tasks.
   *
   * @see PartitionStore#getElements(String, HashRange).
   */
  @Override
  public Optional<CompletableFuture<Iterable<Element>>> getElements(final String partitionId,
                                                                    final HashRange hashRange) {
    final String filePath = partitionIdToFilePath(partitionId);
    if (!new File(filePath).isFile()) {
      return Optional.empty();
    } else {
      final Coder coder = getCoderFromWorker(partitionId);
      final RemoteFileMetadata metadata = new RemoteFileMetadata(
          false, partitionId, executorId, persistentConnectionToMasterMap, messageEnvironment);
      final FilePartition partition = new FilePartition(coder, filePath, metadata, executorService);
      final CompletableFuture<Iterable<Element>> resultFuture = new CompletableFuture<>();
      final Runnable runnable = () -> {
        try {
          // Deserialize the target data in the corresponding file.
          resultFuture.complete(partition.getElementsInRange(hashRange).get());
        } catch (final IOException | InterruptedException | ExecutionException cause) {
          throw new PartitionFetchException(cause);
        }
      };
      Schedulers.io().scheduleDirect(runnable);

      return Optional.of(resultFuture);
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
      final String filePath = partitionIdToFilePath(partitionId);
      final RemoteFileMetadata metadata = new RemoteFileMetadata(
          false, partitionId, executorId, persistentConnectionToMasterMap, messageEnvironment);
      final FilePartition partition = new FilePartition(coder, filePath, metadata, executorService);
      // TODO #485: Merge LocalFileStore and GlusterFileStore.
      // (Partition creation will be reported only when the partition is not cached after #485.)
      reportPartitionCreation(partitionId, executorId, REMOTE_FILE_STORE, persistentConnectionToMasterMap);

      try {
        // Serialize and write the given blocks.
        final List<Long> blockSizeList = putBlocks(coder, partition, blocks);
        return Optional.of(blockSizeList);
      } catch (final IOException cause) {
        partition.commit();
        throw new PartitionWriteException(cause);
      }
    };
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  /**
   * @see PartitionStore#commitPartition(String).
   */
  @Override
  public void commitPartition(final String partitionId) throws PartitionWriteException {
    final Coder coder = getCoderFromWorker(partitionId);
    final String filePath = partitionIdToFilePath(partitionId);

    final RemoteFileMetadata metadata = new RemoteFileMetadata(
        false, partitionId, executorId, persistentConnectionToMasterMap, messageEnvironment);
    new FilePartition(coder, filePath, metadata, executorService).commit();
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

      if (new File(filePath).isFile()) {
        final RemoteFileMetadata metadata = new RemoteFileMetadata(
            false, partitionId, executorId, persistentConnectionToMasterMap, messageEnvironment);
        final FilePartition partition = new FilePartition(coder, filePath, metadata, executorService);
        try {
          partition.deleteFile();
          return true;
        } catch (final IOException cause) {
          throw new PartitionFetchException(cause);
        }
      } else {
        return false;
      }
    };
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  /**
   * Gets {@link FileArea}s corresponding to the blocks having data in a specific {@link HashRange} from a partition.
   * Because the retrieval process can be blocked by waiting the creation of the partition,
   * conducts the process on a scheduler designed to serve blocking tasks.
   *
   * @see FileStore#getFileAreas(String, HashRange).
   */
  @Override
  public CompletableFuture<Iterable<FileArea>> getFileAreas(final String partitionId,
                                                            final HashRange hashRange) {
    final String filePath = partitionIdToFilePath(partitionId);
    if (!new File(filePath).isFile()) {
      throw new PartitionFetchException(new Throwable(String.format("%s does not exists", partitionId)));
    } else {
      final Coder coder = getCoderFromWorker(partitionId);
      final RemoteFileMetadata metadata = new RemoteFileMetadata(
          false, partitionId, executorId, persistentConnectionToMasterMap, messageEnvironment);
      final FilePartition partition = new FilePartition(coder, filePath, metadata, executorService);
      final CompletableFuture<Iterable<FileArea>> resultFuture = new CompletableFuture<>();
      final Runnable runnable = () -> {
        try {
          // Deserialize the target data in the corresponding file.
          resultFuture.complete(partition.asFileAreas(hashRange).get());
        } catch (final InterruptedException | ExecutionException cause) {
          throw new PartitionFetchException(cause);
        }
      };
      Schedulers.io().scheduleDirect(runnable);

      return resultFuture;
    }
  }
}
