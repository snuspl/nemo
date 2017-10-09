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

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.exception.PartitionWriteException;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMasterMap;
import edu.snu.vortex.runtime.executor.data.partition.MemoryPartition;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Store data in local memory.
 */
@ThreadSafe
public final class MemoryStore implements PartitionStore {
  public static final String SIMPLE_NAME = "MemoryStore";
  // A map between partition id and data blocks.
  private final ConcurrentHashMap<String, MemoryPartition> partitionMap;
  private final String executorId;
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  @Inject
  private MemoryStore(@Parameter(JobConf.ExecutorId.class) final String executorId,
                      final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    this.executorId = executorId;
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.partitionMap = new ConcurrentHashMap<>();
  }

  /**
   * @see PartitionStore#getElements(String, HashRange).
   */
  @Override
  public Optional<CompletableFuture<Iterable<Element>>> getElements(final String partitionId,
                                                                    final HashRange hashRange) {
    final MemoryPartition partition = partitionMap.get(partitionId);

    if (partition != null) {
      return Optional.of(partition.getElements(hashRange));
    } else {
      return Optional.empty();
    }
  }

  /**
   * @see PartitionStore#putBlocks(String, Iterable, boolean).
   */
  @Override
  public CompletableFuture<Optional<List<Long>>> putBlocks(final String partitionId,
                                                           final Iterable<Block> blocks,
                                                           final boolean commitPerBlock) {
    final MemoryPartition previousPartition = partitionMap.putIfAbsent(partitionId, new MemoryPartition());
    if (previousPartition == null) {
      // If this partition is newly created, report the creation to the master.
      reportPartitionCreation(partitionId, executorId, executorId, persistentConnectionToMasterMap);
    }

    final CompletableFuture<Optional<List<Long>>> future = new CompletableFuture<>();
    try {
      partitionMap.get(partitionId).appendBlocks(blocks);
      // The partition is not serialized.
      future.complete(Optional.empty());
    } catch (final IOException e) {
      // The partition is committed already.
      future.completeExceptionally(new PartitionWriteException(new Throwable("This partition is already committed.")));
    }

    return future;
  }

  /**
   * @see PartitionStore#commitPartition(String).
   */
  @Override
  public void commitPartition(final String partitionId) {
    final MemoryPartition partition = partitionMap.get(partitionId);
    if (partition != null) {
      partition.commit();
    } else {
      throw new PartitionWriteException(new Throwable("There isn't any partition with id " + partitionId));
    }
  }

  /**
   * @see PartitionStore#removePartition(String).
   */
  @Override
  public CompletableFuture<Boolean> removePartition(final String partitionId) {
    return CompletableFuture.completedFuture(partitionMap.remove(partitionId) != null);
  }
}
