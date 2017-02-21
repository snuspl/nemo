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

import edu.snu.vortex.runtime.common.comm.RuntimeDefinitions;
import edu.snu.vortex.runtime.common.execplan.ExecutionPlan;
import edu.snu.vortex.runtime.common.execplan.RtAttributes;
import edu.snu.vortex.runtime.common.execplan.RtStage;
import edu.snu.vortex.runtime.common.operator.RtDoOp;
import edu.snu.vortex.runtime.common.operator.RtSinkOp;
import edu.snu.vortex.runtime.common.operator.RtSourceOp;

import java.util.*;
import java.util.logging.Logger;

/**
 * ExecutionStateManager.
 */
public class ExecutionStateManager {
  private static final Logger LOG = Logger.getLogger(ExecutionStateManager.class.getName());
  private final Map<String, Set<String>> stageToTaskGroupMap;
  private final Map<String, RuntimeDefinitions.TaskState> taskGroupIdToTaskStateMap;

  private ExecutionPlan executionPlan;

  public ExecutionStateManager() {
    this.stageToTaskGroupMap = new HashMap<>();
    this.taskGroupIdToTaskStateMap = new HashMap<>();
  }

  public void submitExecutionPlan(final ExecutionPlan execPlan) {
    this.executionPlan = execPlan;

    // call APIs of RtStage, RtOperator, RtStageLink, etc.
    // to create tasks and specify channels
    Set<RtStage> rtStages = execPlan.getNextRtStagesToExecute();
    while (!rtStages.isEmpty()) {
      rtStages.forEach(this::convertRtStageToPhysicalPlan);
      rtStages = execPlan.getNextRtStagesToExecute();
    }
  }

  private void convertRtStageToPhysicalPlan(final RtStage rtStage) {
    final Map<RtAttributes.RtStageAttribute, Object> attributes = rtStage.getRtStageAttr();
    final int stageParallelism = (attributes == null || attributes.isEmpty())
        ? 1 : (int) attributes.get(RtAttributes.RtStageAttribute.PARALLELISM);

    rtStage.getRtOps().forEach((id, op) -> {
      for (int i = 0; i < stageParallelism; i++) {
        if (op instanceof RtDoOp) {

        } else if (op instanceof RtSourceOp) {

        } else if (op instanceof RtSinkOp) {

        } else {

        }
      }
    });
  }

  public void initializeRSAndTaskStates(final RtStage runtimeStage) {
//    Set<String> taskIds = new HashSet<>();
//    final List<TaskLabel> taskLabelList = runtimeStage.getTaskLabelList();
//    for (final TaskLabel taskLabel: taskLabelList) {
//      for (final Task task : taskLabel.getTaskList()) {
//        final String taskId = task.getTaskId();
//        taskGroupIdToTaskStateMap.put(taskId, State.TaskState.SCHEDULED);
//        taskIds.add(taskId);
//      }
//    }
//    stageToTaskGroupMap.put(runtimeStage.getRsId(), taskIds);
  }

  public void onTaskStateChanged(final String taskGroupId, final RuntimeDefinitions.TaskState newState) {
    updateTaskState(taskGroupId, newState);

//    if (newState == State.TaskState.COMPLETE) {
//
//    }
  }

  private void updateTaskState(final String taskGroupId, final RuntimeDefinitions.TaskState newState) {
    taskGroupIdToTaskStateMap.replace(taskGroupId, newState);
  }
//
//  private void updateStageState(final String rsId, final State.StageState newState) {
//
//  }

}
