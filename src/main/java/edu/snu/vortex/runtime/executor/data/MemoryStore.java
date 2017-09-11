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
import edu.snu.vortex.runtime.executor.data.partition.MemoryPartition;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Store data in local memory.
 */
@ThreadSafe
final class MemoryStore implements PartitionStore {
  // A map between partition id and data blocks.
  private final ConcurrentHashMap<String, MemoryPartition> partitionMap;

  @Inject
  private MemoryStore() {
    this.partitionMap = new ConcurrentHashMap<>();
  }

  /**
   * @see PartitionStore#retrieveData(String, HashRange).
   */
  @Override
  public CompletableFuture<Optional<Iterable<Element>>> retrieveData(final String partitionId,
                                                                     final HashRange hashRange) {
    final CompletableFuture<Optional<Iterable<Element>>> future = new CompletableFuture<>();
    final MemoryPartition partition = partitionMap.get(partitionId);

    if (partition != null) {
      final Iterable<Block> blocks = partition.getBlocks();
      // Retrieves data in the hash range from the target partition
      final List<Iterable<Element>> retrievedData = new ArrayList<>();
      blocks.forEach(block -> {
        if (hashRange.includes(block.getHashValue())) {
          retrievedData.add(block.getData());
        }
      });

      if (!future.isCompletedExceptionally()) {
        future.complete(Optional.of(concatBlocks(retrievedData)));
      }
    } else {
      future.complete(Optional.empty());
    }
    return future;
  }

  /**
   * @see PartitionStore#putBlocks(String, Iterable, boolean).
   */
  @Override
  public CompletableFuture<Optional<List<Long>>> putBlocks(final String partitionId,
                                                           final Iterable<Block> blocks,
                                                           final boolean commitPerBlock) {
    partitionMap.putIfAbsent(partitionId, new MemoryPartition());
    partitionMap.get(partitionId).appendBlocks(blocks);

    // The partition is not serialized.
    return CompletableFuture.completedFuture(Optional.empty());
  }

  /**
   * @see PartitionStore#commitPartition(String).
   */
  @Override
  public void commitPartition(final String partitionId) {
    final MemoryPartition partition = partitionMap.get(partitionId);
    if (partition != null) {
      partition.commit();
    }
  }

  /**
   * @see PartitionStore#removePartition(String).
   */
  @Override
  public CompletableFuture<Boolean> removePartition(final String partitionId) {
    return CompletableFuture.completedFuture(partitionMap.remove(partitionId) != null);
  }

  /**
   * concatenates an iterable of blocks into a single iterable of elements.
   *
   * @param blocks the iterable of blocks to concatenate.
   * @return the concatenated iterable of all elements.
   */
  private Iterable<Element> concatBlocks(final Iterable<Iterable<Element>> blocks) {
    final List<Element> concatStreamBase = new ArrayList<>();
    Stream<Element> concatStream = concatStreamBase.stream();
    for (final Iterable<Element> block : blocks) {
      concatStream = Stream.concat(concatStream, StreamSupport.stream(block.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }
}
