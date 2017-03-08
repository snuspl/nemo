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
import edu.snu.vortex.runtime.common.RuntimeStates;
import edu.snu.vortex.runtime.common.channel.*;
import edu.snu.vortex.runtime.common.execplan.*;
import edu.snu.vortex.runtime.common.operator.RtDoOp;
import edu.snu.vortex.runtime.common.operator.RtGroupByKeyOp;
import edu.snu.vortex.runtime.common.operator.RtSinkOp;
import edu.snu.vortex.runtime.common.operator.RtSourceOp;
import edu.snu.vortex.runtime.common.task.*;
import edu.snu.vortex.runtime.exception.UnsupportedCommPatternException;
import edu.snu.vortex.runtime.exception.UnsupportedRtOperatorException;

import java.util.*;
import java.util.logging.Logger;

/**
 * ExecutionStateManager.
 */
public class ExecutionStateManager {
  private static final Logger LOG = Logger.getLogger(ExecutionStateManager.class.getName());

  private Scheduler scheduler;

  private ExecutionPlan executionPlan;
  private final Map<String, RtStage> rtStageMap;
  private final Map<String, RuntimeStates.TaskGroupState> taskGroupIdToTaskStateMap;

  public ExecutionStateManager() {
    this.rtStageMap = new HashMap<>();
    this.taskGroupIdToTaskStateMap = new HashMap<>();
  }

  public void initialize(final Scheduler scheduler) {
    this.scheduler = scheduler;
  }

  public void submitExecutionPlan(final ExecutionPlan execPlan) {
    this.executionPlan = execPlan;
    Set<RtStage> rtStages = execPlan.getNextRtStagesToExecute();
    while (!rtStages.isEmpty()) {
      rtStages.forEach(this::convertRtStageToPhysicalPlan);
      rtStages = execPlan.getNextRtStagesToExecute();
      rtStages.forEach(scheduler::launchNextStage);
    }
  }

  private void convertRtStageToPhysicalPlan(final RtStage rtStage) {
    final Map<RuntimeAttributes.StageAttribute, Object> attributes = rtStage.getRtStageAttr();
    final int stageParallelism = (attributes == null || attributes.isEmpty())
        ? 1 : (int) attributes.get(RuntimeAttributes.StageAttribute.PARALLELISM);
    final RuntimeAttributes.ResourceType resourceType =
        (RuntimeAttributes.ResourceType) attributes.get(RuntimeAttributes.StageAttribute.RESOURCE_TYPE);

    final List<RtOperator> operators = rtStage.getRtOperatorList();
    final int taskGroupSize = operators.size();

    for (int i = 0; i < stageParallelism; i++) {
      final TaskGroup taskGroup = new TaskGroup(IdGenerator.generateTaskGroupId(), taskGroupSize, resourceType);
      for (RtOperator op : operators) {
        final Map<String, RtOpLink> inputRtOpLinks = op.getInputLinks();
        final Map<String, RtOpLink> outputRtOpLinks = op.getOutputLinks();
        final Task task;
        final String taskId = IdGenerator.generateTaskId();
        if (op instanceof RtDoOp) {
          task = new DoTask(taskId, new HashMap<>(inputRtOpLinks.size()),
              (RtDoOp) op, new HashMap<>(outputRtOpLinks.size()));
        } else if (op instanceof RtSourceOp) {
          final List<RtSourceOp.Reader> readers = ((RtSourceOp) op).getReaders();
          task = new SourceTask(taskId, readers.get(i), new HashMap<>(outputRtOpLinks.size()));
        } else if (op instanceof RtSinkOp) {
          final List<RtSinkOp.Writer> writers = ((RtSinkOp) op).getWriters();
          task = new SinkTask(taskId, writers.get(i), new HashMap<>(inputRtOpLinks.size()));
        } else if (op instanceof RtGroupByKeyOp) {
          task = new MergeTask(taskId, new HashMap<>(inputRtOpLinks.size()), new HashMap<>(outputRtOpLinks.size()));
        } else {
          throw new UnsupportedRtOperatorException("this operator is not yet supported");
        }
        convertRtOpLinkToPhysicalChannel(inputRtOpLinks, outputRtOpLinks, i, stageParallelism, task);
        op.addTask(task);
        taskGroup.addTask(task);
      }
      taskGroupIdToTaskStateMap.put(taskGroup.getTaskGroupId(), RuntimeStates.TaskGroupState.READY);
      rtStage.addTaskGroup(taskGroup);
    }
    rtStageMap.put(rtStage.getId(), rtStage);
  }

  private void convertRtOpLinkToPhysicalChannel(final Map<String, RtOpLink> inputRtOpLinks,
                                                final Map<String, RtOpLink> outputRtOpLinks,
                                                final int parallelismIdx,
                                                final int stageParallelism,
                                                final Task taskToAdd) {
    inputRtOpLinks.forEach((id, link) -> {
      final Map<RuntimeAttributes.OperatorLinkAttribute, Object> linkAttributes = link.getRtOpLinkAttr();
      final RuntimeAttributes.CommPattern commPattern
          = (RuntimeAttributes.CommPattern)
          linkAttributes.get(RuntimeAttributes.OperatorLinkAttribute.COMMUNICATION_PATTERN);

      final ChannelBundle<ChannelReader> channelBundle = new ChannelBundle();
      final List<Task> srcTaskList = link.getSrcRtOp().getTaskList();
      switch (commPattern) {
      case BROADCAST:
      case SCATTER_GATHER:
        srcTaskList.forEach(task -> {
          final ChannelWriter channelWriter = task.getOutputChannels().get(id).findChannelByIndex(parallelismIdx);
          channelBundle.addChannel(createChannelReader(channelWriter.getType(), channelWriter.getId()));
          channelWriter.setDstTaskId(taskToAdd.getTaskId());
        });
        break;
      case ONE_TO_ONE:
        final Task srcTask = srcTaskList.get(parallelismIdx);
        final ChannelWriter channelWriter = srcTask.getOutputChannels().get(id).findChannelByIndex(parallelismIdx);
        channelBundle.addChannel(createChannelReader(channelWriter.getType(), channelWriter.getId()));
        channelWriter.setDstTaskId(taskToAdd.getTaskId());
        break;
      default:
        throw new UnsupportedCommPatternException("This communication pattern is unsupported");
      }
      taskToAdd.getInputChannels().put(id, channelBundle);
    });

    outputRtOpLinks.forEach((id, link) -> {
      final Map<RuntimeAttributes.OperatorLinkAttribute, Object> linkAttributes = link.getRtOpLinkAttr();
      final RuntimeAttributes.CommPattern commPattern
          = (RuntimeAttributes.CommPattern)
          linkAttributes.get(RuntimeAttributes.OperatorLinkAttribute.COMMUNICATION_PATTERN);
      final RuntimeAttributes.ChannelType channelType
          = (RuntimeAttributes.ChannelType)
          linkAttributes.get(RuntimeAttributes.OperatorLinkAttribute.CHANNEL_TYPE);

      final ChannelBundle<ChannelWriter> channelBundle = new ChannelBundle();
      switch (commPattern) {
      case BROADCAST:
      case SCATTER_GATHER:
        for (int i = 0; i < stageParallelism; i++) {
          final ChannelWriter channelToAdd = createChannelWriter(convertRtAttributeToChannelType(channelType),
              taskToAdd.getTaskId());
          channelBundle.addChannel(channelToAdd);
        }
        break;
      case ONE_TO_ONE:
        final ChannelWriter channelToAdd = createChannelWriter(convertRtAttributeToChannelType(channelType),
            taskToAdd.getTaskId());
        channelBundle.addChannel(channelToAdd);
        break;
      default:
        throw new UnsupportedCommPatternException("This communication pattern is unsupported");
      }
      taskToAdd.getOutputChannels().put(id, channelBundle);
    });
  }

  private ChannelType convertRtAttributeToChannelType(final RuntimeAttributes.ChannelType rtAttribute) {
    switch (rtAttribute) {
    case LOCAL:
      return ChannelType.LOCAL;
    case MEMORY:
      return ChannelType.MEMORY;
    case FILE:
      return ChannelType.FILE;
    case DISTR_STORAGE:
      return ChannelType.DISTRIBUTED_STORAGE;
    default:
      throw new UnsupportedCommPatternException("This channel type is unsupported");
    }
  }

  private ChannelReader createChannelReader(final ChannelType channelType,
                                      final String srcTaskId) {
    return createChannelReader(channelType, srcTaskId, "");
  }

  private ChannelReader createChannelReader(final ChannelType channelType,
                                final String srcTaskId, final String dstTaskId) {
    // TODO #000: create channels when the implementation is pushed.
    final ChannelReader channelReader;
    switch (channelType) {
    case LOCAL:
      channelReader = new LocalChannel(IdGenerator.generateChannelId(), srcTaskId, dstTaskId);
      break;
    case MEMORY:
      channelReader = new MemoryChannelReader(IdGenerator.generateChannelId(), srcTaskId, dstTaskId);
      break;
    case FILE:
      channelReader = new FileChannelReader(IdGenerator.generateChannelId(), srcTaskId, dstTaskId);
      break;
    case DISTRIBUTED_STORAGE:
      channelReader = new DistStorageChannelReader(IdGenerator.generateChannelId(), srcTaskId, dstTaskId);
      break;
    default:
      throw new UnsupportedCommPatternException("This channel type is unsupported");
    }
    return channelReader;
  }

  private ChannelWriter createChannelWriter(final ChannelType channelType,
                                            final String srcTaskId) {
    return createChannelWriter(channelType, srcTaskId, "");
  }

  private ChannelWriter createChannelWriter(final ChannelType channelType,
                                            final String srcTaskId, final String dstTaskId) {
    // TODO #000: create channels when the implementation is pushed.
    final ChannelWriter channelWriter;
    switch (channelType) {
    case LOCAL:
      channelWriter = new LocalChannel(IdGenerator.generateChannelId(), srcTaskId, dstTaskId);
      break;
    case MEMORY:
      channelWriter = new MemoryChannelWriter(IdGenerator.generateChannelId(), srcTaskId, dstTaskId);
      break;
    case FILE:
      channelWriter = new FileChannelWriter(IdGenerator.generateChannelId(), srcTaskId, dstTaskId);
      break;
    case DISTRIBUTED_STORAGE:
      channelWriter = new DistStorageChannelWriter(IdGenerator.generateChannelId(), srcTaskId, dstTaskId);
      break;
    default:
      throw new UnsupportedCommPatternException("This channel type is unsupported");
    }

    return channelWriter;
  }


  public void onTaskGroupStateChanged(final String taskGroupId, final RuntimeStates.TaskGroupState newState) {
    updateTaskGroupState(taskGroupId, newState);

    String stageId = "";
    boolean stageComplete = true;
    RtStage taskGroupStage = null;
    if (newState == RuntimeStates.TaskGroupState.COMPLETE) {
      scheduler.onTaskGroupExecutionComplete(taskGroupId);
      taskGroupIdToTaskStateMap.replace(taskGroupId, RuntimeStates.TaskGroupState.COMPLETE);
      for (final Map.Entry<String, RtStage> stage : rtStageMap.entrySet()) {
        taskGroupStage = stage.getValue();
        final List<TaskGroup> taskGroups = taskGroupStage.getTaskGroups();
        if (taskGroups.contains(taskGroupId)) {
          taskGroups.removeIf(taskGroup -> taskGroup.getTaskGroupId().equals(taskGroupId));
          if (!taskGroups.isEmpty()) {
            stageComplete = false;
            break;
          }
        }
      }
      if (stageComplete) {
        // TODO #000 : what to do when a stage completes?
        rtStageMap.remove(stageId);
        executionPlan.removeCompleteStage(taskGroupStage);
      }
    }
  }

  private void updateTaskGroupState(final String taskGroupId, final RuntimeStates.TaskGroupState newState) {
    taskGroupIdToTaskStateMap.replace(taskGroupId, newState);
  }
}
