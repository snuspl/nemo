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
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.common.state.JobState;
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.common.state.TaskState;
import edu.snu.vortex.runtime.exception.IllegalStateTransitionException;
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
 * The methods of this class are synchronized.
 */
public final class ExecutionStateManager {
  private static final Logger LOG = Logger.getLogger(ExecutionStateManager.class.getName());

  private String jobId;

  /**
   * The data structures below track the execution states of this job.
   */
  private JobState jobState;
  private final Map<String, StageState> idToStageStates;
  private final Map<String, TaskGroupState> idToTaskGroupStates;
  private final Map<String, TaskState> idToTaskStates;

  /**
   * Represent the job to manage.
   */
  private PhysicalPlan physicalPlan;

  /**
   * Used to track current stage completion status.
   * All task group ids are added to the set when the a stage begins executing.
   * Each task group id is removed upon completion,
   * therefore indicating the stage's completion when this set becomes empty.
   */
  private Set<String> currentStageTaskGroupIds;

  /**
   * Used to track job completion status.
   * All stage ids are added to the set when the this job begins executing.
   * Each stage id is removed upon completion,
   * therefore indicating the job's completion when this set becomes empty.
   */
  private Set<String> currentJobStageIds;

  public ExecutionStateManager() {
    this.idToStageStates = new HashMap<>();
    this.idToTaskGroupStates = new HashMap<>();
    this.idToTaskStates = new HashMap<>();
    this.currentStageTaskGroupIds = new HashSet<>();
    this.currentJobStageIds = new HashSet<>();
  }

  /**
   * Receives a new job to manage.
   * @param physicalPlanForNewJob to manage.
   */
  public synchronized void manageNewJob(final PhysicalPlan physicalPlanForNewJob) {
    this.physicalPlan = physicalPlanForNewJob;
    idToStageStates.clear();
    idToTaskGroupStates.clear();
    idToTaskStates.clear();
    currentJobStageIds.clear();

    this.jobId = physicalPlanForNewJob.getId();
    this.jobState = new JobState();
    onJobStateChanged(JobState.State.EXECUTING);

    // Initialize the states for the job down to task-level.
    physicalPlanForNewJob.getStageDAG().topologicalDo(physicalStage -> {
      currentJobStageIds.add(physicalStage.getId());
      idToStageStates.put(physicalStage.getId(), new StageState());
      physicalStage.getTaskGroupList().forEach(taskGroup -> {
        idToTaskGroupStates.put(taskGroup.getTaskGroupId(), new TaskGroupState());
        taskGroup.getTaskDAG().getVertices().forEach(
            task -> idToTaskStates.put(task.getTaskId(), new TaskState()));
      });
    });
  }

  /**
   * Updates the state of the job.
   * @param newState of the job.
   */
  public synchronized void onJobStateChanged(final JobState.State newState) {
    if (newState == JobState.State.EXECUTING) {
      LOG.log(Level.FINE, "Executing Job ID {0}...", jobId);
      jobState.getStateMachine().setState(newState);
    } else if (newState == JobState.State.COMPLETE) {
      LOG.log(Level.FINE, "Job ID {0} complete!", jobId);
      jobState.getStateMachine().setState(newState);
      dumpPreviousJobExecutionStateToFile();
    } else if (newState == JobState.State.FAILED) {
      LOG.log(Level.FINE, "Job ID {0} failed.", jobId);
      jobState.getStateMachine().setState(newState);
      dumpPreviousJobExecutionStateToFile();
    } else {
      throw new IllegalStateTransitionException(new Exception("Illegal Job State Transition"));
    }
  }

  /**
   * Updates the state of a stage.
   * Stage state changes only occur in master.
   * @param stageId of the stage.
   * @param newState of the stage.
   * @return true if this state change results in the entire job completion, false otherwise.
   */
  public synchronized boolean onStageStateChanged(final String stageId, final StageState.State newState) {
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

  /**
   * Updates the state of a task group.
   * Task group state changes can occur both in master and executor.
   * State changes that occur in master are initiated in {@link edu.snu.vortex.runtime.master.scheduler.Scheduler}.
   * State changes that occur in executors are sent to master as a control message,
   * and the call to this method is initiated in {@link edu.snu.vortex.runtime.master.scheduler.Scheduler}
   * when the message/event is received.
   * A task group completion implies completion of all its tasks.
   * @param taskGroupId of the task group.
   * @param newState of the task group.
   * @return true if this state change results in the current stage completion, false otherwise.
   */
  public synchronized boolean onTaskGroupStateChanged(final String taskGroupId, final TaskGroupState.State newState) {
    final StateMachine taskGroupStateChanged = idToTaskGroupStates.get(taskGroupId).getStateMachine();
    LOG.log(Level.FINE, "Task Group State Transition: id {0} from {1} to {2}",
        new Object[]{taskGroupId, taskGroupStateChanged.getCurrentState(), newState});
    taskGroupStateChanged.setState(newState);
    if (newState == TaskGroupState.State.COMPLETE) {
      currentStageTaskGroupIds.remove(taskGroupId);
      for (final PhysicalStage physicalStage : physicalPlan.getStageDAG().getVertices()) {
        for (final TaskGroup taskGroup : physicalStage.getTaskGroupList()) {
          if (taskGroup.getTaskGroupId().equals(taskGroupId)) {
            taskGroup.getTaskDAG().getVertices().forEach(task -> {
              idToTaskStates.get(task.getTaskId()).getStateMachine().setState(TaskState.State.PENDING_IN_EXECUTOR);
              idToTaskStates.get(task.getTaskId()).getStateMachine().setState(TaskState.State.EXECUTING);
              idToTaskStates.get(task.getTaskId()).getStateMachine().setState(TaskState.State.COMPLETE);
            });
            break;
          }
        }
      }
    }
    return currentStageTaskGroupIds.isEmpty();
  }

  public String getJobId() {
    return jobId;
  }

  public JobState getJobState() {
    return jobState;
  }

  public Map<String, StageState> getIdToStageStates() {
    return idToStageStates;
  }

  public Map<String, TaskGroupState> getIdToTaskGroupStates() {
    return idToTaskGroupStates;
  }

  public Map<String, TaskState> getIdToTaskStates() {
    return idToTaskStates;
  }

  // Tentative
  public void printCurrentJobExecutionState() {
    final StringBuffer sb = new StringBuffer("Job ID ");
    sb.append(this.jobId).append(":").append(jobState.getStateMachine().getCurrentState());
    sb.append("\n{Stages:\n{");
    physicalPlan.getStageDAG().topologicalDo(physicalStage -> {
      final StageState stageState = idToStageStates.get(physicalStage.getId());
      sb.append(physicalStage.getId()).append(":").append(stageState).append("\nTaskGroups:\n{");
      physicalStage.getTaskGroupList().forEach(taskGroup -> {
        sb.append(taskGroup.getTaskGroupId()).append(":").append(idToTaskGroupStates.get(taskGroup.getTaskGroupId()))
            .append(", Tasks:{\n");
        taskGroup.getTaskDAG().topologicalDo(
            task -> sb.append(task.getTaskId()).append(":").append(idToTaskStates.get(task.getTaskId())).append(","));
        sb.append("},\n");
      });
      sb.append("}}\n");
    });
    LOG.log(Level.INFO, sb.toString());
  }

  // Tentative
  private void dumpPreviousJobExecutionStateToFile() {

  }
}
