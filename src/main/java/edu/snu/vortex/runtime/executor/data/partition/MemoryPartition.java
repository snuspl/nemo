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
package edu.snu.vortex.runtime.executor.data.partition;

import edu.snu.vortex.runtime.executor.data.Block;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This class represents a partition which is stored in local memory and not serialized.
 * TODO #463: Support incremental read.
 */
@ThreadSafe
public final class MemoryPartition implements Closeable {

  private final List<Block> blocks;

  public MemoryPartition() {
    blocks = new CopyOnWriteArrayList<>();
  }

  /**
   * Appends all data in the block to this partition.
   *
   * @param blocksToAppend the blocks to append.
   */
  public void appendBlocks(final Iterable<Block> blocksToAppend) {
    blocksToAppend.forEach(blocks::add);
  }

  /**
   * @return the list of the blocks in this partition.
   */
  public List<Block> getBlocks() {
    return blocks;
  }

  /**
   * Close to prevent further write for this partition.
   * If someone "subscribing" the data in this partition, it will be finished.
   */
  @Override
  public void close() {
  }
}
