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
package edu.snu.onyx.runtime.executor.data.stores;

import edu.snu.onyx.common.exception.BlockFetchException;
import edu.snu.onyx.conf.JobConf;
import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.*;
import edu.snu.onyx.runtime.executor.data.metadata.LocalFileMetadata;
import edu.snu.onyx.runtime.executor.data.partition.FileTmpToBe;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.*;
import java.util.List;

/**
 * Stores partitions in local files.
 */
@ThreadSafe
public final class LocalFileStore extends LocalBlockStore implements FileStore {
  private final String fileDirectory;
  private final InjectionFuture<BlockManagerWorker> partitionManagerWorker;

  @Inject
  private LocalFileStore(@Parameter(JobConf.FileDirectory.class) final String fileDirectory,
                         final InjectionFuture<BlockManagerWorker> partitionManagerWorker) {
    super(partitionManagerWorker);
    this.fileDirectory = fileDirectory;
    this.partitionManagerWorker = partitionManagerWorker;
    new File(fileDirectory).mkdirs();
  }

  /**
   * Creates a new partition.
   *
   * @param blockId the ID of the partition to create.
   * @see BlockStore#createBlock(String).
   */
  @Override
  public void createBlock(final String blockId) {
    removeBlock(blockId);

    final Coder coder = getCoderFromWorker(blockId);
    final LocalFileMetadata metadata = new LocalFileMetadata(false);

    final FileTmpToBe partition =
        new FileTmpToBe(coder, DataUtil.partitionIdToFilePath(blockId, fileDirectory), metadata);
    getPartitionMap().put(blockId, partition);
  }

  /**
   * Removes the file that the target partition is stored.
   *
   * @param blockId of the partition.
   * @return whether the partition exists or not.
   */
  @Override
  public Boolean removeBlock(final String blockId) throws BlockFetchException {
    final FileTmpToBe filePartition = (FileTmpToBe) getPartitionMap().remove(blockId);
    if (filePartition == null) {
      return false;
    }
    try {
      filePartition.deleteFile();
    } catch (final IOException e) {
      throw new BlockFetchException(e);
    }
    return true;
  }

  /**
   * @see FileStore#getFileAreas(String, HashRange).
   */
  @Override
  public List<FileArea> getFileAreas(final String partitionId,
                                     final HashRange hashRange) {
    try {
      final FileTmpToBe partition = (FileTmpToBe) getPartitionMap().get(partitionId);
      if (partition == null) {
        throw new IOException(String.format("%s does not exists", partitionId));
      }
      return partition.asFileAreas(hashRange);
    } catch (final IOException retrievalException) {
      throw new BlockFetchException(retrievalException);
    }
  }
}
