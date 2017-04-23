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

import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStage;
import edu.snu.vortex.runtime.common.state.JobState;
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.common.state.TaskState;
import edu.snu.vortex.utils.StateMachine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Manages the states related to a job.
 * This class can be used to track a job's execution status to task level in the future.
 */
public final class ExecutionStateManager {
  private static final Logger LOG = Logger.getLogger(ExecutionStateManager.class.getName());

  private String jobId;
  private JobState jobState;
  private final Map<String, StageState> idToStageStates;
  private final Map<String, TaskGroupState> idToTaskGroupStates;
  private final Map<String, TaskState> idToTaskStates;

  private PhysicalPlan physicalPlan;
  private Set<String> currentStageTaskGroupIds;
  private Set<String> currentJobStageIds;

  public ExecutionStateManager() {
    this.idToStageStates = new HashMap<>();
    this.idToTaskGroupStates = new HashMap<>();
    this.idToTaskStates = new HashMap<>();
    this.currentStageTaskGroupIds = new HashSet<>();
    this.currentJobStageIds = new HashSet<>();
  }

  public void manageNewJob(final PhysicalPlan physicalPlan) {
    this.physicalPlan = physicalPlan;
    idToStageStates.clear();
    idToTaskGroupStates.clear();
    idToTaskStates.clear();
    currentJobStageIds.clear();

    this.jobId = physicalPlan.getId();
    this.jobState = new JobState();
    onJobStateChanged(JobState.State.EXECUTING);

    physicalPlan.getStageDAG().topologicalDo(physicalStage -> {
      idToStageStates.put(physicalStage.getId(), new StageState());
      physicalStage.getTaskGroupList().forEach(taskGroup -> {
        idToTaskGroupStates.put(taskGroup.getTaskGroupId(), new TaskGroupState());
        taskGroup.getTaskDAG().getVertices().forEach(task -> idToTaskStates.put(task.getTaskId(), new TaskState()));
      });
    });
  }

  public void onJobStateChanged(final JobState.State newState) {
    if (newState == JobState.State.EXECUTING) {
      LOG.log(Level.FINE, "Executing Job ID {0}...", jobId);
      jobState.getStateMachine().setState(newState);
      physicalPlan.getStageDAG().getVertices().forEach(physicalStage -> currentJobStageIds.add(physicalStage.getId()));
    } else if (newState == JobState.State.COMPLETE) {
      LOG.log(Level.FINE, "Job ID {0} complete!", jobId);
      jobState.getStateMachine().setState(newState);
      dumpPreviousJobExecutionStateToFile();
    } else if (newState == JobState.State.FAILED) {
      LOG.log(Level.FINE, "Job ID {0} failed.", jobId);
      jobState.getStateMachine().setState(newState);
      dumpPreviousJobExecutionStateToFile();
    } else {
      throw new RuntimeException("Illegal Job State Transition");
    }
  }

  // Stage states only change in master(scheduler).
  public boolean onStageStateChanged(final String stageId, final StageState.State newState) {
    final StateMachine stageStateMachine = idToStageStates.get(stageId).getStateMachine();
    LOG.log(Level.FINE, "Stage State Transition: id {0} from {1} to {2}",
        new Object[]{stageId, stageStateMachine.getCurrentState(), newState});
    stageStateMachine.setState(newState);
    if (newState == StageState.State.EXECUTING) {
      currentStageTaskGroupIds.clear();
      for (final PhysicalStage stage : physicalPlan.getStageDAG().getVertices()) {
        if (stage.getId().equals(stageId)) {
          currentStageTaskGroupIds.addAll(
              stage.getTaskGroupList()
                  .stream()
                  .map(taskGroup -> taskGroup.getTaskGroupId())
                  .collect(Collectors.toSet()));
          break;
        }
      }
    } else if (newState == StageState.State.COMPLETE) {
      currentJobStageIds.remove(stageId);
    }
    return currentJobStageIds.isEmpty();
  }

  // Task Group states can change in master(scheduler) and executor (but the events are received by scheduler).
  public boolean onTaskGroupStateChanged(final String taskGroupId, final TaskGroupState.State newState) {
    final StateMachine taskGroupStateChanged = idToTaskGroupStates.get(taskGroupId).getStateMachine();
    LOG.log(Level.FINE, "Task Group State Transition: id {0} from {1} to {2}",
        new Object[]{taskGroupId, taskGroupStateChanged.getCurrentState(), newState});
    taskGroupStateChanged.setState(newState);
    if (newState == TaskGroupState.State.COMPLETE) {
      currentStageTaskGroupIds.remove(taskGroupId);
    }
    return currentStageTaskGroupIds.isEmpty();
  }

  // Tentative
  public void getCurrentJobExecutionState() {

  }

  // Tentative
  private void dumpPreviousJobExecutionStateToFile() {

  }
}
