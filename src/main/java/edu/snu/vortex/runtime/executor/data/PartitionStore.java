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
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.exception.PartitionWriteException;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMasterMap;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for partition placement.
 */
public interface PartitionStore {

  /**
   * Report the partition creation to the master.
   *
   * @param partitionId                     the ID of the created partition.
   * @param executorId                      the ID of the sender executor.
   * @param location                        the location of this partition.
   * @param persistentConnectionToMasterMap map having connection to the master.
   */
  default void reportPartitionCreation(final String partitionId,
                                       final String executorId,
                                       final String location,
                                       final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    final ControlMessage.PartitionStateChangedMsg.Builder partitionStateChangedMsgBuilder =
        ControlMessage.PartitionStateChangedMsg.newBuilder()
            .setExecutorId(executorId)
            .setPartitionId(partitionId)
            .setState(ControlMessage.PartitionStateFromExecutor.CREATED)
            .setLocation(location);

    persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID)
        .send(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.PartitionStateChanged)
            .setPartitionStateChangedMsg(partitionStateChangedMsgBuilder.build())
            .build());
  }

  /**
   * Gets elements having key in a specific {@link HashRange} from a partition.
   * The result will be an {@link Iterable}, and looking up for it's {@link java.util.Iterator} can be blocked.
   * If all of the data of the target partition are not committed yet,
   * the iterable will have the committed data only.
   * The requester can just wait the further committed blocks by using it's block-able iterator, or
   * subscribes the further blocks by using {@link org.apache.reef.wake.rx.Observable}.
   * For the further details, check {@link edu.snu.vortex.runtime.common.ClosableBlockingIterable}
   * and {@link edu.snu.vortex.runtime.common.ObservableIterableWrapper}.
   *
   * @param partitionId of the target partition.
   * @param hashRange   the hash range.
   * @return the result data from the target partition (if the target partition exists).
   *         (the future completes exceptionally with {@link edu.snu.vortex.runtime.exception.PartitionFetchException}
   *          for any error occurred while trying to fetch a partition.
   *          This exception will be thrown to the {@link edu.snu.vortex.runtime.master.scheduler.Scheduler}
   *          through {@link edu.snu.vortex.runtime.executor.Executor} and
   *          have to be handled by the scheduler with fault tolerance mechanism.)
   */
  Optional<CompletableFuture<Iterable<Element>>> getElements(String partitionId,
                                                             HashRange hashRange);

  /**
   * Saves an iterable of data blocks to a partition.
   * If the partition exists already, appends the data to it.
   * This method supports concurrent write.
   * If the data is needed to be "incrementally" written (and read),
   * each block can be committed right after being written by using {@param commitPerBlock}.
   *
   * @param partitionId    of the partition.
   * @param blocks         to save to a partition.
   * @param commitPerBlock whether commit every block write or not.
   * @return the size of the data per block (only when the data is serialized).
   *         (the future completes with {@link edu.snu.vortex.runtime.exception.PartitionWriteException}
   *          for any error occurred while trying to write a partition.
   *          This exception will be thrown to the {@link edu.snu.vortex.runtime.master.scheduler.Scheduler}
   *          through {@link edu.snu.vortex.runtime.executor.Executor} and
   *          have to be handled by the scheduler with fault tolerance mechanism.)
   */
  CompletableFuture<Optional<List<Long>>> putBlocks(String partitionId,
                                                    Iterable<Block> blocks,
                                                    boolean commitPerBlock);

  /**
   * Notifies that all writes for a partition is end.
   * Subscribers waiting for the data of the target partition are notified when the partition is committed.
   * Also, further subscription about a committed partition will not blocked but get the data in it and finished.
   *
   * @param partitionId of the partition.
   * @throws PartitionWriteException if fail to commit.
   *         (This exception will be thrown to the {@link edu.snu.vortex.runtime.master.scheduler.Scheduler}
   *          through {@link edu.snu.vortex.runtime.executor.Executor} and
   *          have to be handled by the scheduler with fault tolerance mechanism.)
   */
  void commitPartition(String partitionId) throws PartitionWriteException;

  /**
   * Removes a partition of data.
   *
   * @param partitionId of the partition.
   * @return whether the partition exists or not.
   *         (the future completes exceptionally with {@link edu.snu.vortex.runtime.exception.PartitionFetchException}
   *          for any error occurred while trying to remove a partition.
   *          This exception will be thrown to the {@link edu.snu.vortex.runtime.master.scheduler.Scheduler}
   *          through {@link edu.snu.vortex.runtime.executor.Executor} and
   *          have to be handled by the scheduler with fault tolerance mechanism.))
   */
  CompletableFuture<Boolean> removePartition(String partitionId);
}
