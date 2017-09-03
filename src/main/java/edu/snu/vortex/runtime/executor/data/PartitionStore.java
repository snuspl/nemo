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

import edu.snu.vortex.compiler.ir.Element;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for partition placement.
 */
public interface PartitionStore {
  /**
   * Retrieves data in a specific hash range from a partition.
   *
   * @param partitionId of the target partition.
   * @param hashRange   the hash range
   * @return the result data as a new partition (if the target partition exists).
   *         (the future completes exceptionally with {@link edu.snu.vortex.runtime.exception.PartitionFetchException}
   *          for any error occurred while trying to fetch a partition.)
   */
  CompletableFuture<Optional<Iterable<Element>>> retrieveData(String partitionId, // TODO: Iterable<Block> ?
                                                              HashRange hashRange); // TODO: Incremental?

  /**
   * Saves an iterable of data blocks to a partition.
   * If the partition exists already, appends the data to it.
   * Each block can be split into multiple blocks according to it's size.
   * This method supports concurrent write, but these blocks may not be saved consecutively.
   *
   * @param partitionId of the partition.
   * @param blocks      to save as a partition.
   * @return the size of the data per block (only when the data is serialized).
   *         (the future completes with {@link edu.snu.vortex.runtime.exception.PartitionWriteException}
   *          for any error occurred while trying to write a partition.)
   */
  CompletableFuture<Optional<List<Long>>> putBlocks(String partitionId,
                                                    Iterable<Block> blocks); // TODO: Incremental?

  /**
   * Optional<Partition> removePartition(String partitionId) throws PartitionFetchException;
   * Removes a partition of data.
   *
   * @param partitionId of the partition.
   * @return whether the partition exists or not.
   *         (the future completes exceptionally with {@link edu.snu.vortex.runtime.exception.PartitionFetchException}
   *          for any error occurred while trying to remove a partition.)
   */
  CompletableFuture<Boolean> removePartition(String partitionId);
}
