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
import edu.snu.vortex.runtime.exception.PartitionFetchException;
import edu.snu.vortex.runtime.exception.PartitionWriteException;
import edu.snu.vortex.runtime.executor.data.partition.NonSerializedPartition;
import edu.snu.vortex.runtime.executor.data.partition.Partition;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Store data in local memory.
 */
@ThreadSafe
final class MemoryStore implements PartitionStore {
  // A map between partition id and data.
  private final ConcurrentHashMap<String, Iterable<Element>> partitionIdToData;
  // A map between partition id and data blocked and sorted by the hash value.
  private final ConcurrentHashMap<String, Iterable<Iterable<Element>>> partitionIdToBlockedData;

  @Inject
  private MemoryStore() {
    this.partitionIdToData = new ConcurrentHashMap<>();
    this.partitionIdToBlockedData = new ConcurrentHashMap<>();
  }

  @Override
  public Optional<Partition> getPartition(final String partitionId) {
    final Iterable<Element> partitionData = partitionIdToData.get(partitionId);
    final Iterable<Iterable<Element>> blockedPartitionData = partitionIdToBlockedData.get(partitionId);
    if (partitionData != null) {
      return Optional.of(new NonSerializedPartition(partitionData));
    } else if (blockedPartitionData != null) {
      return Optional.of(new NonSerializedPartition(concatBlocks(blockedPartitionData)));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public Optional<Partition> retrieveDataFromPartition(final String partitionId,
                                                       final int startInclusiveHashVal,
                                                       final int endExclusiveHashVal)
      throws PartitionFetchException {
    final Iterable<Iterable<Element>> blockedPartitionData = partitionIdToBlockedData.get(partitionId);

    if (blockedPartitionData != null) {
      // Retrieves data in the hash range from the target partition
      final List<Iterable<Element>> retrievedData = new ArrayList<>(endExclusiveHashVal - startInclusiveHashVal);
      final Iterator<Iterable<Element>> iterator = blockedPartitionData.iterator();
      IntStream.range(0, endExclusiveHashVal).forEach(hashVal -> {
        if (!iterator.hasNext()) {
          throw new PartitionFetchException(
              new RuntimeException("Illegal hash range. There are only " + hashVal + " blocks in this partition."));
        }
        if (hashVal < startInclusiveHashVal) {
          iterator.next();
        } else {
          retrievedData.add(iterator.next());
        }
      });

      return Optional.of(new NonSerializedPartition(concatBlocks(retrievedData)));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public Optional<Long> putDataAsPartition(final String partitionId,
                                           final Iterable<Element> data) {
    final Iterable<Element> previousData = partitionIdToData.putIfAbsent(partitionId, data);
    if (previousData != null) {
      throw new RuntimeException("Trying to overwrite an existing partition");
    }

    partitionIdToData.put(partitionId, data);

    // The partition is not serialized.
    return Optional.empty();
  }

  @Override
  public Optional<Iterable<Long>> putSortedDataAsPartition(final String partitionId,
                                                           final Iterable<Iterable<Element>> sortedData)
      throws PartitionWriteException {
    final Iterable<Iterable<Element>> previousBlockedData =
        partitionIdToBlockedData.putIfAbsent(partitionId, sortedData);
    if (previousBlockedData != null) {
      throw new RuntimeException("Trying to overwrite an existing partition");
    }

    partitionIdToBlockedData.put(partitionId, sortedData);

    // The partition is not serialized.
    return Optional.empty();
  }

  @Override
  public boolean removePartition(final String partitionId) {
    return (partitionIdToData.remove(partitionId) != null) || (partitionIdToBlockedData.remove(partitionId) != null);
  }

  /**
   * concatenates an iterable of blocks into a single iterable of elements.
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
