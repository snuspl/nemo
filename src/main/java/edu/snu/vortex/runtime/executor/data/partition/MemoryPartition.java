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

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.ClosableBlockingIterable;
import edu.snu.vortex.runtime.common.ObservableIterableWrapper;
import edu.snu.vortex.runtime.executor.data.Block;
import edu.snu.vortex.runtime.executor.data.HashRange;
import edu.snu.vortex.runtime.executor.data.partition.observer.CollectBlockObserver;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents a partition which is stored in local memory and not serialized.
 */
@ThreadSafe
public final class MemoryPartition {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryPartition.class.getName());
  private final ClosableBlockingIterable<Block> blocks;
  private final ObservableIterableWrapper<Block> observableBlocks;
  private volatile AtomicBoolean committed;

  public MemoryPartition() {
    blocks = new ClosableBlockingIterable<>();
    observableBlocks = new ObservableIterableWrapper<>(blocks);
    committed = new AtomicBoolean(false);
  }

  /**
   * Appends all data in the block to this partition.
   *
   * @param blocksToAppend the blocks to append.
   * @throws IOException if this partition is committed already.
   */
  public void appendBlocks(final Iterable<Block> blocksToAppend) throws IOException {
    if (!committed.get()) {
      blocksToAppend.forEach(blocks::add);
    } else {
      throw new IOException("Cannot append blocks to the committed partition");
    }
  }

  /**
   * Gets elements having key in a specific {@link HashRange} from a partition.
   * The result will be an {@link Iterable}, and looking up for it's {@link java.util.Iterator} can be blocked.
   *
   * @see edu.snu.vortex.runtime.executor.data.PartitionStore#getElements(String, HashRange).
   * @param hashRange the range of key to get.
   * @return the future of the iterable of the blocks in a specific hash range of this partition.
   */
  public CompletableFuture<Iterable<Element>> getElements(final HashRange hashRange) {
    final CompletableFuture<Iterable<Element>> iterableFuture = new CompletableFuture<>();
    observableBlocks.subscribeOn(Schedulers.io()).subscribe(new CollectBlockObserver(hashRange, iterableFuture));
    return iterableFuture;
  }

  /**
   * Commits this partition to prevent further write.
   * If someone "subscribing" the data in this partition, it will be finished.
   */
  public void commit() {
    blocks.close();
    committed.set(true);
  }
}
