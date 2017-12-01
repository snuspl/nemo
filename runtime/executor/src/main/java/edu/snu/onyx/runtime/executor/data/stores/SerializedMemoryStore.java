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
package edu.snu.onyx.runtime.executor.data.stores;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.common.exception.PartitionFetchException;
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.PartitionManagerWorker;
import edu.snu.onyx.runtime.executor.data.SerializedBlock;
import edu.snu.onyx.runtime.executor.data.partition.SerializedMemoryPartition;
import org.apache.reef.tang.InjectionFuture;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.util.Optional;

/**
 * Serialize and store data in local memory.
 */
@ThreadSafe
public final class SerializedMemoryStore extends LocalPartitionStore {
  private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;

  @Inject
  private SerializedMemoryStore(final InjectionFuture<PartitionManagerWorker> partitionManagerWorker) {
    super(partitionManagerWorker);
    this.partitionManagerWorker = partitionManagerWorker;
  }

  @Override
  public void createPartition(final String partitionId) {
    final Coder coder = getCoderFromWorker(partitionId);
    getPartitionMap().put(partitionId, new SerializedMemoryPartition(coder));
  }

  /**
   * Retrieves {@link SerializedBlock}s in a specific {@link HashRange} from a partition.
   *
   * @param partitionId of the target partition.
   * @param hashRange   the hash range.
   * @return the result data from the target partition (if the target partition exists).
   * @throws PartitionFetchException if fail to get data.
   */
  public Optional<Iterable<SerializedBlock>> getSerializedBlocks(final String partitionId,
                                                                 final HashRange hashRange)
      throws PartitionFetchException {
    final SerializedMemoryPartition partition = (SerializedMemoryPartition) getPartitionMap().get(partitionId);

    if (partition != null) {
      try {
        return Optional.of(partition.getSerializedBlocks(hashRange));
      } catch (final IOException e) {
        throw new PartitionFetchException(e);
      }
    } else {
      return Optional.empty();
    }
  }

  /**
   * @see PartitionStore#removePartition(String).
   */
  @Override
  public Boolean removePartition(final String partitionId) {
    return getPartitionMap().remove(partitionId) != null;
  }
}
