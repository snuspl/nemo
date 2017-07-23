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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.common.Pair;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageContext;
import edu.snu.vortex.runtime.common.state.PartitionState;
import edu.snu.vortex.common.StateMachine;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Master-side partition manager.
 * For now, all its operations are synchronized to guarantee thread safety.
 */
@ThreadSafe
public final class PartitionManagerMaster {
  private static final Logger LOG = Logger.getLogger(PartitionManagerMaster.class.getName());
  private final Map<String, PartitionState> partitionIdToState;
  private final Map<String, String> committedPartitionIdToWorkerId;
  private final Map<String, Set<String>> producerTaskGroupIdToPartitionIds;
  private final Map<String, CompletableFuture<Pair<PartitionState.State, Optional<String>>>>
      partitionIdToLocationFuture;

  @Inject
  public PartitionManagerMaster() {
    this.partitionIdToState = new HashMap<>();
    this.committedPartitionIdToWorkerId = new HashMap<>();
    this.producerTaskGroupIdToPartitionIds = new HashMap<>();
    this.partitionIdToLocationFuture = new HashMap<>();
  }

  public synchronized void initializeState(final String edgeId, final int srcTaskIndex,
                                           final String producerTaskGroupId) {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(edgeId, srcTaskIndex);
    partitionIdToState.put(partitionId, new PartitionState());
    producerTaskGroupIdToPartitionIds.putIfAbsent(producerTaskGroupId, new HashSet<>());
    producerTaskGroupIdToPartitionIds.get(producerTaskGroupId).add(partitionId);
  }

  public synchronized void initializeState(final String edgeId, final int srcTaskIndex, final int partitionIndex,
                                           final String producerTaskGroupId) {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(edgeId, srcTaskIndex, partitionIndex);
    partitionIdToState.put(partitionId, new PartitionState());
    producerTaskGroupIdToPartitionIds.putIfAbsent(producerTaskGroupId, new HashSet<>());
    producerTaskGroupIdToPartitionIds.get(producerTaskGroupId).add(partitionId);
  }

  public synchronized Set<String> removeWorker(final String executorId) {
    final Set<String> taskGroupsToRecompute = new HashSet<>();

    // Set committed partition states to lost
    getCommittedPartitionsByWorker(executorId).forEach(partitionId -> {
      onPartitionStateChanged(partitionId, PartitionState.State.LOST, executorId);
      final Optional<String> producerTaskGroupForPartition = getProducerTaskGroupId(partitionId);

      // producerTaskGroupForPartition should always be non-empty.
      taskGroupsToRecompute.add(producerTaskGroupForPartition.get());
    });

    // Update worker-related global variables
    committedPartitionIdToWorkerId.entrySet().removeIf(e -> e.getValue().equals(executorId));

    return taskGroupsToRecompute;
  }

  public synchronized Optional<String> getPartitionLocation(final String partitionId) {
    final String executorId = committedPartitionIdToWorkerId.get(partitionId);
    return Optional.ofNullable(executorId);
  }

  /**
   * Return a {@link CompletableFuture} of partition state, which is not yet resolved in {@code SCHEDULED} state.
   * @param partitionId id of the specified partition
   * @return {@link CompletableFuture} of {@link PartitionState.State} and {@link Optional} of partition location.
   */
  public synchronized CompletableFuture<Pair<PartitionState.State, Optional<String>>>
      getPartitionStateFuture(final String partitionId) {
    final PartitionState.State state =
        (PartitionState.State) getPartitionState(partitionId).getStateMachine().getCurrentState();
    switch (state) {
      case SCHEDULED:
        return partitionIdToLocationFuture.computeIfAbsent(partitionId, pId -> new CompletableFuture<>());
      case COMMITTED:
        return CompletableFuture.completedFuture(Pair.of(state, getPartitionLocation(partitionId)));
      case READY:
      case LOST_BEFORE_COMMIT:
      case LOST:
      case REMOVED:
        return CompletableFuture.completedFuture(Pair.of(state, Optional.empty()));
      default:
        throw new UnsupportedOperationException(state.toString());
    }
  }

  public synchronized Optional<String> getProducerTaskGroupId(final String partitionId) {
    for (Map.Entry<String, Set<String>> entry : producerTaskGroupIdToPartitionIds.entrySet()) {
      if (entry.getValue().contains(partitionId)) {
        return Optional.of(entry.getKey());
      }
    }
    return Optional.empty();
  }

  /**
   * To be called when a potential producer task group is scheduled.
   * To be precise, it is called when the task group is enqueued to
   * {@link edu.snu.vortex.runtime.master.scheduler.PendingTaskGroupPriorityQueue}
   * @param scheduledTaskGroupId the ID of the scheduled task group.
   */
  public synchronized void onProducerTaskGroupScheduled(final String scheduledTaskGroupId) {
    if (producerTaskGroupIdToPartitionIds.containsKey(scheduledTaskGroupId)) {
      producerTaskGroupIdToPartitionIds.get(scheduledTaskGroupId).forEach(partitionId ->
          onPartitionStateChanged(partitionId, PartitionState.State.SCHEDULED, null));
    } // else this task group does not produce any partition
  }

  /**
   * To be called when a potential producer task group fails.
   * Only the TaskGroups that have not yet completed (i.e. partitions not yet committed) will call this method.
   * @param failedTaskGroupId the ID of the task group that failed.
   */
  public synchronized void onProducerTaskGroupFailed(final String failedTaskGroupId) {
    if (producerTaskGroupIdToPartitionIds.containsKey(failedTaskGroupId)) {
      producerTaskGroupIdToPartitionIds.get(failedTaskGroupId).forEach(partitionId ->
          onPartitionStateChanged(partitionId, PartitionState.State.LOST_BEFORE_COMMIT, null));
    } // else this task group does not produce any partition
  }

  public synchronized Set<String> getCommittedPartitionsByWorker(final String executorId) {
    final Set<String> partitionIds = new HashSet<>();
    committedPartitionIdToWorkerId.forEach((partitionId, workerId) -> {
      if (workerId.equals(executorId)) {
        partitionIds.add(partitionId);
      }
    });
    return partitionIds;
  }

  public synchronized PartitionState getPartitionState(final String partitionId) {
    return partitionIdToState.get(partitionId);
  }

  public synchronized void onPartitionStateChanged(final String partitionId,
                                                   final PartitionState.State newState,
                                                   final String committedWorkerId) {
    final StateMachine sm = partitionIdToState.get(partitionId).getStateMachine();
    final Enum oldState = sm.getCurrentState();
    LOG.log(Level.FINE, "Partition State Transition: id {0} from {1} to {2}",
        new Object[]{partitionId, oldState, newState});

    sm.setState(newState);

    switch (newState) {
      case SCHEDULED:
        break;
      case LOST_BEFORE_COMMIT:
        completeLocationFuture(partitionId, newState, Optional.empty());
        break;
      case COMMITTED:
        committedPartitionIdToWorkerId.put(partitionId, committedWorkerId);
        completeLocationFuture(partitionId, newState, Optional.of(committedWorkerId));
        break;
      case REMOVED:
        committedPartitionIdToWorkerId.remove(partitionId);
        completeLocationFuture(partitionId, newState, Optional.empty());
        break;
      case LOST:
        LOG.log(Level.INFO, "Partition {0} lost in {1}", new Object[]{partitionId, committedWorkerId});
        committedPartitionIdToWorkerId.remove(partitionId);
        completeLocationFuture(partitionId, newState, Optional.empty());
        break;
      default:
        throw new UnsupportedOperationException(newState.toString());
    }
  }

  private synchronized void completeLocationFuture(final String partitionId,
                                                   final PartitionState.State state,
                                                   final Optional<String> result) {
    partitionIdToLocationFuture.entrySet().removeIf(e -> {
      if (e.getKey().equals(partitionId)) {
        e.getValue().complete(Pair.of(state, result));
        return true;
      }
      return false;
    });
  }

  public synchronized void onRequestPartitionLocation(final ControlMessage.Message message,
                                                      final MessageContext messageContext) {
    final ControlMessage.RequestPartitionLocationMsg requestPartitionLocationMsg =
        message.getRequestPartitionLocationMsg();
    final CompletableFuture<Pair<PartitionState.State, Optional<String>>> pairFuture
        = getPartitionStateFuture(requestPartitionLocationMsg.getPartitionId());
    pairFuture.thenAccept(pair -> {
      final ControlMessage.PartitionLocationInfoMsg.Builder infoMsgBuilder =
          ControlMessage.PartitionLocationInfoMsg.newBuilder()
              .setRequestId(message.getId())
              .setPartitionId(requestPartitionLocationMsg.getPartitionId())
              .setState(RuntimeMaster.convertPartitionState(pair.left()));
      if (pair.right().isPresent()) {
        infoMsgBuilder.setOwnerExecutorId(pair.right().get());
      }
      messageContext.reply(
          ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
              .setType(ControlMessage.MessageType.PartitionLocationInfo)
              .setPartitionLocationInfoMsg(infoMsgBuilder.build())
              .build());
    });
  }
}
