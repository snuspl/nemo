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

import edu.snu.vortex.runtime.common.*;
import edu.snu.vortex.runtime.common.execplan.ExecutionPlan;
import edu.snu.vortex.runtime.common.execplan.RtStage;

import java.util.*;
import java.util.logging.Logger;

/**
 * ExecutionStateManager.
 */
public class ExecutionStateManager {
  private static final Logger LOG = Logger.getLogger(ExecutionStateManager.class.getName());
  private final Map<String, Set<String>> rsIdToTaskIdMap;
  private final Map<String, ExecutionState.TaskState> taskIdToTaskStateMap;

  private ExecutionPlan executionPlan;

  public void submitExecutionPlan(final ExecutionPlan execPlan) {
    this.executionPlan = execPlan;

    // call APIs of RtStage, RtOperator, RtStageLink, etc.
    // to create tasks and specify channels
  }

  public ExecutionStateManager() {
    this.rsIdToTaskIdMap = new HashMap<>();
    this.taskIdToTaskStateMap = new HashMap<>();
  }

  public void initializeRSAndTaskStates(final RtStage runtimeStage) {
//    Set<String> taskIds = new HashSet<>();
//    final List<TaskLabel> taskLabelList = runtimeStage.getTaskLabelList();
//    for (final TaskLabel taskLabel: taskLabelList) {
//      for (final Task task : taskLabel.getTaskList()) {
//        final String taskId = task.getTaskId();
//        taskIdToTaskStateMap.put(taskId, State.TaskState.SCHEDULED);
//        taskIds.add(taskId);
//      }
//    }
//    rsIdToTaskIdMap.put(runtimeStage.getRsId(), taskIds);
  }

  public void onTaskStateChanged(final String taskId, final ExecutionState.TaskState newState) {
    updateTaskState(taskId, newState);

//    if (newState == State.TaskState.COMPLETE) {
//
//    }
  }
//
//  private void updateStageState(final String rsId, final State.StageState newState) {
//
//  }

  private void updateTaskState(final String taskId, final ExecutionState.TaskState newState) {
    taskIdToTaskStateMap.replace(taskId, newState);
  }
}
