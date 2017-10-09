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
package edu.snu.vortex.runtime.master.resource;

import com.google.protobuf.ByteString;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.common.plan.physical.ScheduledTaskGroup;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.driver.context.ActiveContext;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Contains information/state regarding an executor.
 * Such information may include:
 *    a) The executor's resource type.
 *    b) The executor's capacity (ex. number of cores).
 *    c) Task groups scheduled/launched for the executor.
 *    d) (Please add other information as we implement more features).
 */
public final class ExecutorRepresenter {

  private final String executorId;
  private final ResourceSpecification resourceSpecification;
  private final Set<String> runningTaskGroups;
  private final Set<String> completeTaskGroups;
  private final Set<String> failedTaskGroups;
  private final MessageEnvironment messageEnvironment;
  private final MessageSender<ControlMessage.Message> messageSenderToExecutor;
  private final ActiveContext activeContext;

  public ExecutorRepresenter(final String executorId,
                             final ResourceSpecification resourceSpecification,
                             final MessageEnvironment messageEnvironment,
                             final MessageSender<ControlMessage.Message> messageSenderToMaster,
                             final ActiveContext activeContext) {
    this.executorId = executorId;
    this.resourceSpecification = resourceSpecification;
    this.messageEnvironment = messageEnvironment;
    this.messageSenderToExecutor = messageSenderToMaster;
    this.runningTaskGroups = new HashSet<>();
    this.completeTaskGroups = new HashSet<>();
    this.failedTaskGroups = new HashSet<>();
    this.activeContext = activeContext;
  }

  public void onExecutorFailed() {
    runningTaskGroups.forEach(taskGroupId -> failedTaskGroups.add(taskGroupId));
    runningTaskGroups.clear();
  }

  public void onTaskGroupScheduled(final ScheduledTaskGroup scheduledTaskGroup) {
    runningTaskGroups.add(scheduledTaskGroup.getTaskGroup().getTaskGroupId());
    failedTaskGroups.remove(scheduledTaskGroup.getTaskGroup().getTaskGroupId());

    sendControlMessage(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.ScheduleTaskGroup)
            .setScheduleTaskGroupMsg(
                ControlMessage.ScheduleTaskGroupMsg.newBuilder()
                    .setTaskGroup(ByteString.copyFrom(SerializationUtils.serialize(scheduledTaskGroup)))
                    .build())
            .build());
  }

  /**
   * Send a message to the executor.
   *
   * @param message the message to send.
   */
  public void sendControlMessage(final ControlMessage.Message message) {
    messageSenderToExecutor.send(message);
  }

  /**
   * Send a message to a specific listener in the executor.
   *
   * @param message    the message ot send.
   * @param listenerId the listener to send the message.
   */
  public void sendControlMessage(final ControlMessage.Message message,
                                 final String listenerId) {
    try {
      final MessageSender<ControlMessage.Message> messageSender =
          messageEnvironment.<ControlMessage.Message>asyncConnect(executorId, listenerId).get();
      messageSender.send(message);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void onTaskGroupExecutionComplete(final String taskGroupId) {
    runningTaskGroups.remove(taskGroupId);
    completeTaskGroups.add(taskGroupId);
  }

  public void onTaskGroupExecutionFailed(final String taskGroupId) {
    runningTaskGroups.remove(taskGroupId);
    failedTaskGroups.add(taskGroupId);
  }

  public int getExecutorCapacity() {
    return resourceSpecification.getCapacity();
  }

  public Set<String> getRunningTaskGroups() {
    return runningTaskGroups;
  }

  public Set<String> getCompleteTaskGroups() {
    return completeTaskGroups;
  }

  public String getExecutorId() {
    return executorId;
  }

  public String getContainerType() {
    return resourceSpecification.getContainerType();
  }

  public void shutDown() {
    activeContext.close();
  }
}

