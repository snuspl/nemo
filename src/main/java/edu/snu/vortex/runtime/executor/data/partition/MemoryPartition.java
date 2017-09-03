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

import java.util.LinkedList;
import java.util.List;

/**
 * This class represents a partition which is stored in local memory and not serialized.
 */
public final class MemoryPartition {

  private final List<Block> blocks;

  public MemoryPartition() {
    blocks = new LinkedList<>();
  }

  /**
   * Appends all data in the block to this partition.
   *
   * @param blocksToAppend the blocks to append.
   */
  public synchronized void appendBlocks(final Iterable<Block> blocksToAppend) {
    blocksToAppend.forEach(blocks::add);
  }

  /**
   * @return the list of the blocks in this partition.
   */
  public List<Block> getBlocks() {
    return blocks;
  }
}
