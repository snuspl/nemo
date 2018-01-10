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
package edu.snu.onyx.runtime.executor.data.metadata;

import java.io.IOException;
import java.io.Serializable;

/**
 * This interface represents a metadata for a {@link edu.snu.onyx.runtime.executor.data.block.Block}.
 * The writer and reader determine the status of a file block
 * (such as accessibility, how many bytes are written, etc.) by using this metadata.
 * @param <K> the key type of its partitions.
 */
public interface FileMetadata<K extends Serializable> {

  /**
   * Writes the metadata for a partition.
   *
   * @param key     the key of the partition.
   * @param partitionSize the size of the partition.
   * @param elementsTotal the number of elements in the partition.
   * @throws IOException if fail to append the partition metadata.
   */
  void writePartitionMetadata(final K key,
                              final int partitionSize,
                              final long elementsTotal) throws IOException;

  /**
   * Gets a iterable containing the partition metadata of corresponding block.
   *
   * @return the iterable containing the partition metadata.
   * @throws IOException if fail to get the iterable.
   */
  Iterable<PartitionMetadata<K>> getPartitionMetadataIterable() throws IOException;

  /**
   * Deletes the metadata.
   *
   * @throws IOException if fail to delete.
   */
  void deleteMetadata() throws IOException;

  /**
   * Notifies that all writes are finished for the block corresponding to this metadata.
   */
  void commitBlock();
}
