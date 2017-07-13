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
package edu.snu.vortex.runtime.master.scheduler;

import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStage;
import edu.snu.vortex.runtime.common.plan.physical.ScheduledTaskGroup;
import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Keep tracks of all pending task groups.
 * This class provides two-level queue scheduling by prioritizing TaskGroups of parent stages to be scheduled first.
 * Stages that are mutually independent alternate turns in scheduling each of their TaskGroups.
 * This PQ assumes that stages/task groups of higher priorities are never enqueued without first removing
 * those of lower priorities (which is how Scheduler behaves) for simplicity.
 */
@ThreadSafe
@DriverSide
public final class PendingTaskGroupPriorityQueue {
  private PhysicalPlan physicalPlan;

  /**
   * Pending TaskGroups awaiting to be scheduled for each stage.
   */
  private final Map<String, Deque<ScheduledTaskGroup>> stageIdToPendingTaskGroups;

  /**
   * Stages with TaskGroups that have not yet been scheduled.
   */
  private final BlockingDeque<String> pendingStages;

  @Inject
  public PendingTaskGroupPriorityQueue() {
    stageIdToPendingTaskGroups = new HashMap<>();
    pendingStages = new LinkedBlockingDeque<>();
  }

  /**
   * Enqueues a TaskGroup to this PQ.
   * @param scheduledTaskGroup
   */
  public synchronized void enqueue(final ScheduledTaskGroup scheduledTaskGroup) {
    final String stageId = scheduledTaskGroup.getTaskGroup().getStageId();

    if (stageIdToPendingTaskGroups.containsKey(stageId)) {
      stageIdToPendingTaskGroups.get(stageId).addLast(scheduledTaskGroup);
    } else {
      final Deque<ScheduledTaskGroup> pendingTaskGroupsForStage = new ArrayDeque<>();
      pendingTaskGroupsForStage.add(scheduledTaskGroup);

      stageIdToPendingTaskGroups.put(stageId, pendingTaskGroupsForStage);
      updatePendingStages(stageId);
    }
  }

  /**
   * Dequeues the next TaskGroup to be scheduled according to job dependency priority.
   * @return the next TaskGroup to be scheduled
   * @throws InterruptedException
   */
  public synchronized ScheduledTaskGroup dequeueNextTaskGroup() throws InterruptedException {
    final String stageId = pendingStages.takeFirst();

    final Deque<ScheduledTaskGroup> pendingTaskGroupsForStage = stageIdToPendingTaskGroups.get(stageId);
    final ScheduledTaskGroup taskGroupToSchedule = pendingTaskGroupsForStage.poll();

    if (pendingTaskGroupsForStage.isEmpty()) {
      stageIdToPendingTaskGroups.remove(stageId);
      stageIdToPendingTaskGroups.keySet().forEach(scheduledStageId -> updatePendingStages(scheduledStageId));
    } else {
      pendingStages.addLast(stageId);
    }

    return taskGroupToSchedule;
  }

  /**
   * Removes a stage and its descendant stages from this PQ.
   * @param stageId for the stage to begin the removal recursively.
   */
  public synchronized void removeStageAndDescendantsFromQueue(final String stageId) {
    removeStageAndChildren(stageId);
  }

  /**
   * Recursively removes a stage and its children stages from this PQ.
   * @param stageId for the stage to begin the removal recursively.
   */
  private void removeStageAndChildren(final String stageId) {
    pendingStages.remove(stageId);
    stageIdToPendingTaskGroups.remove(stageId);

    physicalPlan.getStageDAG().getChildren(stageId).forEach(
        physicalStage -> removeStageAndChildren(physicalStage.getId()));
  }

  /**
   * Updates the two-level PQ when all TaskGroups for a stage are scheduled.
   * This enables those depending on this stage to begin being scheduled.
   * @param scheduledStageId for the stage that has completed scheduling.
   */
  private void updatePendingStages(final String scheduledStageId) {
    boolean readyToScheduleImmediately = true;
    for (final PhysicalStage parentStage : physicalPlan.getStageDAG().getParents(scheduledStageId)) {
      if (pendingStages.contains(parentStage.getId())) {
        readyToScheduleImmediately = false;
        break;
      }
    }

    if (readyToScheduleImmediately) {
      if (!pendingStages.contains(scheduledStageId)) {
        pendingStages.addLast(scheduledStageId);
      }
    }
  }

  public synchronized void onJobScheduled(final PhysicalPlan physicalPlanForJob) {
    this.physicalPlan = physicalPlanForJob;
  }
}
