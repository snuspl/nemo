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

import edu.snu.vortex.runtime.common.IdGenerator;
import edu.snu.vortex.runtime.common.channel.ChannelBundle;
import edu.snu.vortex.runtime.common.comm.RuntimeDefinitions;
import edu.snu.vortex.runtime.common.execplan.*;
import edu.snu.vortex.runtime.common.operator.RtDoOp;
import edu.snu.vortex.runtime.common.operator.RtSinkOp;
import edu.snu.vortex.runtime.common.operator.RtSourceOp;
import edu.snu.vortex.runtime.common.task.DoTask;
import edu.snu.vortex.runtime.common.task.TaskGroup;
import edu.snu.vortex.runtime.exception.UnsupportedCommPatternException;

import java.util.*;
import java.util.logging.Logger;

/**
 * ExecutionStateManager.
 */
public class ExecutionStateManager {
  private static final Logger LOG = Logger.getLogger(ExecutionStateManager.class.getName());

  private Scheduler scheduler;

  private final Map<String, Set<String>> stageToTaskGroupMap;
  private final Map<String, RuntimeDefinitions.TaskState> taskGroupIdToTaskStateMap;

  private ExecutionPlan executionPlan;

  public ExecutionStateManager() {
    this.stageToTaskGroupMap = new HashMap<>();
    this.taskGroupIdToTaskStateMap = new HashMap<>();
  }

  public void initialize(final Scheduler scheduler) {
    this.scheduler = scheduler;
  }

  public void submitExecutionPlan(final ExecutionPlan execPlan) {
    this.executionPlan = execPlan;

    // call APIs of RtStage, RtOperator, RtStageLink, etc.
    // to create tasks and specify channels
    Set<RtStage> rtStages = execPlan.getNextRtStagesToExecute();
    while (!rtStages.isEmpty()) {
      rtStages.forEach(this::convertRtStageToPhysicalPlan);
      rtStages = execPlan.getNextRtStagesToExecute();

      rtStages.forEach(scheduler::launchNextStage);
    }
  }

  private void convertRtStageToPhysicalPlan(final RtStage rtStage) {
    final Map<RtAttributes.RtStageAttribute, Object> attributes = rtStage.getRtStageAttr();
    final int stageParallelism = (attributes == null || attributes.isEmpty())
        ? 1 : (int) attributes.get(RtAttributes.RtStageAttribute.PARALLELISM);

//    final RtStageLink stageLink = rtStage.getInputLinks();

    final List<TaskGroup> taskGroups = new ArrayList<>(stageParallelism);
    final List<RtOperator> operators = rtStage.getRtOperatorList();
    final int taskGroupSize = operators.size();

    for (int i = 0; i < stageParallelism; i++) {
      final TaskGroup taskGroup = new TaskGroup(IdGenerator.generateTaskGroupId(), taskGroupSize);
      operators.forEach((op) -> {
        if (op instanceof RtDoOp) {
          op.getOutputLinks();

          final DoTask task = new DoTask(null, (RtDoOp) op, null);
          op.addTask(task);
          taskGroup.addTask(task);
        } else if (op instanceof RtSourceOp) {
          final DoTask task = new DoTask(null, (RtDoOp) op, null);
          taskGroup.addTask(task);
        } else if (op instanceof RtSinkOp) {
          final DoTask task = new DoTask(null, (RtDoOp) op, null);
          taskGroup.addTask(task);
        } else {
          final DoTask task = new DoTask(null, (RtDoOp) op, null);
          taskGroup.addTask(task);
        }
      });
      rtStage.addTaskGroup(taskGroup);
    }

  }

  private void convertRtOpLinkToPhysicalChannel(final Map<String, RtOpLink> rtOpLinkMap) {
    rtOpLinkMap.forEach((id, link) -> {
      final Map<RtAttributes.RtOpLinkAttribute, Object> linkAttributes = link.getRtOpLinkAttr();
      final RtAttributes.CommPattern commPattern
          = (RtAttributes.CommPattern) linkAttributes.get(RtAttributes.RtOpLinkAttribute.COMM_PATTERN);

      final ChannelBundle channelBundle = new ChannelBundle();
      switch (commPattern) {
      case BROADCAST:
        break;
      case ONE_TO_ONE:
//        link.getSrcRtOp().getTaskList().forEach(t -> t.set);
        break;
      case SCATTER_GATHER:
        break;
      default:
        throw new UnsupportedCommPatternException("This communication pattern is unsupported");
      }
    });
  }

  public void onTaskGroupStateChanged(final String taskGroupId, final RuntimeDefinitions.TaskState newState) {
    updateTaskGroupState(taskGroupId, newState);

    String stageId = "";
    boolean stageComplete = true;
    if (newState == RuntimeDefinitions.TaskState.COMPLETE) {
      for (final Map.Entry<String, Set<String>> stage : stageToTaskGroupMap.entrySet()) {
        if (stage.getValue().contains(taskGroupId)) {
          stageId = stage.getKey();
          for (final String otherTaskGroupId : stage.getValue()) {
            if (taskGroupIdToTaskStateMap.get(otherTaskGroupId) != RuntimeDefinitions.TaskState.COMPLETE) {
              stageComplete = false;
              break;
            }
          }
          break;
        }
      }
      if (stageComplete) {
        // TODO #000 : what to do when a stage completes?
        stageToTaskGroupMap.remove(stageId);
      }
    }
  }

  private void updateTaskGroupState(final String taskGroupId, final RuntimeDefinitions.TaskState newState) {
    taskGroupIdToTaskStateMap.replace(taskGroupId, newState);
  }
}
