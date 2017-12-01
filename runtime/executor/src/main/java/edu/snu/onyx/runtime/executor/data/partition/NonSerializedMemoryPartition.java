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
package edu.snu.onyx.runtime.executor.data.partition;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.runtime.executor.data.Block;
import edu.snu.onyx.runtime.executor.data.DataUtil;
import edu.snu.onyx.runtime.common.data.HashRange;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This class represents a partition which is stored in local memory and not serialized.
 */
@ThreadSafe
public final class NonSerializedMemoryPartition implements Partition {

  private final List<Block> nonSerializedBlocks;
  private final Coder coder;
  private volatile boolean committed;

  public NonSerializedMemoryPartition(final Coder coder) {
    this.nonSerializedBlocks = new ArrayList<>();
    this.coder = coder;
    this.committed = false;
  }

  /**
   * Stores {@link Block}s to this partition a non-serialized form.
   * Invariant: This should not be invoked after this partition is committed.
   *
   * @param blocksToStore the {@link Block}s to store.
   * @return the size of the data per block.
   * @throws IOException if fail to store.
   */
  @Override
  public synchronized Optional<List<Long>> putBlocks(final Iterable<Block> blocksToStore) throws IOException {
    if (!committed) {
      // If there is any serialized block in the iterable, deserialize it.
      final Iterable<Block> convertedBlocks = DataUtil.convertToNonSerBlocks(coder, blocksToStore);

      convertedBlocks.forEach(nonSerializedBlocks::add);
    } else {
      throw new IOException("Cannot append block to the committed partition");
    }

    return Optional.empty();
  }

  /**
   * Retrieves the {@link Block}s in a specific hash range from this partition.
   * Invariant: This should not be invoked before this partition is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @param serialize whether to get the {@link Block}s in a serialized form or not.
   * @return an iterable of {@link Block}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<Block> getBlocks(final HashRange hashRange,
                                   final boolean serialize) throws IOException {
    if (committed) {
      // Retrieves data in the hash range from the target partition
      final List<Block> blocksInRange = new ArrayList<>();
      nonSerializedBlocks.forEach(block -> {
        if (hashRange.includes(block.getKey())) {
          blocksInRange.add(block);
        }
      });

      if (serialize) {
        // We have to serialize the stored blocks and return.
        return DataUtil.convertToSerBlocks(coder, blocksInRange);
      } else {
        return blocksInRange;
      }
    } else {
      throw new IOException("Cannot retrieve elements before a partition is committed");
    }
  }

  /**
   * Commits this partition to prevent further write.
   */
  @Override
  public synchronized void commit() {
    committed = true;
  }
}
