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

import com.google.protobuf.ByteString;
import edu.snu.vortex.runtime.common.comm.RuntimeDefinitions;
import edu.snu.vortex.runtime.common.execplan.RtStage;
import edu.snu.vortex.runtime.common.task.TaskGroup;
import edu.snu.vortex.runtime.exception.EmptyExecutionPlanException;
import org.apache.commons.lang.SerializationUtils;

import java.util.List;
import java.util.Map;

/**
 * Scheduler.
 */
public class Scheduler {

  private SchedulingPolicy schedulingPolicy;
  private MasterCommunicator masterCommunicator;
  private Map<RtAttributes.ResourceType, String> resources;

  public Scheduler(final SchedulingPolicy schedulingPolicy) {
    this.schedulingPolicy = schedulingPolicy;
  }

  public void initialize(final MasterCommunicator masterCommunicator) {
    this.masterCommunicator = masterCommunicator;
  }

  public void launchNextStage(final RtStage rtStage) throws EmptyExecutionPlanException {
    // TODO #000: identify where resource type is tagged - stage, taskgroup or operator
    final RtAttributes.ResourceType resourceType =
        (RtAttributes.ResourceType) rtStage.getRtStageAttr().get(RtAttributes.RtStageAttribute.RESOURCE_TYPE);

    final List<TaskGroup> taskGroups = rtStage.getTaskGroups();

    for (final Map.Entry<RtAttributes.ResourceType, String> entry : resources.entrySet()) {
      if (taskGroups.isEmpty()) {
        break;
      }
      if (resourceType == entry.getKey()) {
        final TaskGroup taskGroup = taskGroups.get(0);
        taskGroups.remove(taskGroup);

        final RuntimeDefinitions.ScheduleTaskGroupMsg.Builder msgBuilder
            = RuntimeDefinitions.ScheduleTaskGroupMsg.newBuilder();
        final ByteString taskGroupBytes = ByteString.copyFrom(SerializationUtils.serialize(taskGroup));
        msgBuilder.setTaskGroup(taskGroupBytes);
        final RuntimeDefinitions.RtControllableMsg.Builder builder
            = RuntimeDefinitions.RtControllableMsg.newBuilder();
        builder.setType(RuntimeDefinitions.MessageType.ScheduleTaskGroup);
        builder.setScheduleTaskGroupMsg(msgBuilder.build());
        masterCommunicator.sendRtControllable(entry.getValue(), builder.build());
      }
    }
  }

  public void onResourceUpdated(final Map<RtAttributes.ResourceType, String> updatedMap) {
    this.resources = updatedMap;
  }
}
