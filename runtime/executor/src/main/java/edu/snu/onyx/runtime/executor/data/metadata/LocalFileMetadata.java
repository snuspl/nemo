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

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * This class represents a metadata for a local file {@link edu.snu.onyx.runtime.executor.data.block.Block}.
 * It resides in local only, and does not synchronize with master.
 * @param <K> the key type of its partitions.
 */
@ThreadSafe
public final class LocalFileMetadata<K extends Serializable> implements FileMetadata<K> {

  private final List<PartitionMetadata<K>> partitionMetadataList; // The list of partition metadata.
  private volatile long writtenBytesCursor; // Indicates how many bytes are (at least, logically) written in the file.
  private volatile boolean committed;

  public LocalFileMetadata() {
    this.partitionMetadataList = new ArrayList<>();
    this.writtenBytesCursor = 0;
    this.committed = false;
  }

  /**
   * Reserves the region for a partition and get the metadata for the partition.
   * @see FileMetadata#writePartitionMetadata(Serializable, int, long)
   */
  @Override
  public synchronized void writePartitionMetadata(final K key,
                                                  final int partitionSize,
                                                  final long elementsTotal) throws IOException {
    if (committed) {
      throw new IOException("Cannot write a new block to a closed partition.");
    }

    final PartitionMetadata partitionMetadata =
        new PartitionMetadata(partitionMetadataList.size(), key, partitionSize, writtenBytesCursor, elementsTotal);
    partitionMetadataList.add(partitionMetadata);
    writtenBytesCursor += partitionSize;
  }

  /**
   * Gets a iterable containing the partition metadata of corresponding block.
   * @see FileMetadata#getPartitionMetadataIterable()
   * @throws IOException if this block is not committed yet.
   */
  @Override
  public Iterable<PartitionMetadata<K>> getPartitionMetadataIterable() throws IOException {
    if (committed) {
      return Collections.unmodifiableCollection(partitionMetadataList);
    } else {
      throw new IOException("This block is not committed yet.");
    }
  }

  /**
   * @see FileMetadata#deleteMetadata()
   */
  @Override
  public void deleteMetadata() {
    // Do nothing because this metadata is only in the local memory.
  }

  /**
   * Notifies that all writes are finished for the block corresponding to this metadata.
   */
  @Override
  public synchronized void commitBlock() {
    committed = true;
  }
}
