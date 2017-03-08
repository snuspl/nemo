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
import edu.snu.vortex.runtime.common.execplan.RuntimeAttributes;
import edu.snu.vortex.runtime.common.task.TaskGroup;
import edu.snu.vortex.runtime.exception.EmptyExecutionPlanException;
import org.apache.commons.lang.SerializationUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Scheduler.
 */
public class Scheduler {
  private final ExecutorService schedulerThread;
  private final BlockingDeque<TaskGroup> taskGroupsToSchedule;

  private SchedulingPolicy schedulingPolicy;
  private MasterCommunicator masterCommunicator;

  public Scheduler(final SchedulingPolicy schedulingPolicy) {
    this.schedulerThread = Executors.newSingleThreadExecutor();
    this.schedulingPolicy = schedulingPolicy;
    this.taskGroupsToSchedule = new LinkedBlockingDeque<>();
  }

  public void initialize(final MasterCommunicator masterCommunicator) {
    this.masterCommunicator = masterCommunicator;
    this.schedulerThread.execute(new TaskGroupScheduleHandler());
  }

  public void launchNextStage(final RtStage rtStage) {
    final List<TaskGroup> taskGroups = rtStage.getTaskGroups();
    taskGroupsToSchedule.addAll(taskGroups);
  }

  private class TaskGroupScheduleHandler implements Runnable {
    @Override
    public void run() {
      while (!schedulerThread.isShutdown()) {
        try {
          final TaskGroup taskGroup = taskGroupsToSchedule.take();
          final Optional<String> executorId = schedulingPolicy.attemptSchedule(taskGroup);
          if (executorId.isPresent()) {
            taskGroupsToSchedule.offer(taskGroup);
          } else {
            final RuntimeDefinitions.ScheduleTaskGroupMsg.Builder msgBuilder
                = RuntimeDefinitions.ScheduleTaskGroupMsg.newBuilder();
            final ByteString taskGroupBytes = ByteString.copyFrom(SerializationUtils.serialize(taskGroup));
            msgBuilder.setTaskGroup(taskGroupBytes);
            final RuntimeDefinitions.RtControllableMsg.Builder builder
                = RuntimeDefinitions.RtControllableMsg.newBuilder();
            builder.setType(RuntimeDefinitions.MessageType.ScheduleTaskGroup);
            builder.setScheduleTaskGroupMsg(msgBuilder.build());
            masterCommunicator.sendRtControllable(executorId.get(), builder.build());
            schedulingPolicy.onTaskGroupLaunched(executorId.get(), taskGroup);
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  public void onResourceAdded(final RuntimeAttributes.ResourceType resourceType, final String resourceId) {
    schedulingPolicy.resourceAdded(resourceType, resourceId);
  }

  public void onResourceDeleted(final RuntimeAttributes.ResourceType resourceType, final String resourceId) {
    schedulingPolicy.resourceDeleted(resourceType, resourceId);
  }

  public void onTaskGroupExecutionComplete(final String taskGroupId) {
    schedulingPolicy.onTaskGroupExecutionComplete(taskGroupId);
  }

  public void terminate() {
    schedulerThread.shutdown();
    taskGroupsToSchedule.clear();
  }
}
