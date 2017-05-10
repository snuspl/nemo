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
package edu.snu.vortex.runtime.executor;

import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageContext;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.exception.IllegalMessageException;
import edu.snu.vortex.runtime.executor.datatransfer.DataTransferFactory;
import edu.snu.vortex.runtime.master.BlockManagerMaster;
import org.apache.commons.lang3.SerializationUtils;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Executor.
 */
public final class Executor {

  private final String executorId;
  private final int numCores;
  private final MessageEnvironment<ControlMessage.Message> myMessageEnvironment;
  private final Map<String, MessageSender<ControlMessage.Message>> nodeIdToMessageSenderMap;
  private final ExecutorService executorService;
  private final DataTransferFactory dataTransferFactory;

  private PhysicalPlan physicalPlan;
  private TaskGroupStateManager taskGroupStateManager;

  public Executor(final String executorId,
                  final int numCores,
                  final MessageEnvironment myMessageEnvironment,
                  final Map<String, MessageSender<ControlMessage.Message>> nodeIdToMessageSenderMap,
                  final BlockManagerMaster blockManagerMaster) {
    this.executorId = executorId;
    this.numCores = numCores;
    this.myMessageEnvironment = myMessageEnvironment;
    myMessageEnvironment.setupListener(MessageEnvironment.EXECUTOR_MESSAGE_RECEIVER, new ExecutorMessageReceiver());
    this.nodeIdToMessageSenderMap = nodeIdToMessageSenderMap;
    this.executorService = Executors.newFixedThreadPool(numCores);

    // TODO #: Check
    this.dataTransferFactory = new DataTransferFactory(executorId, blockManagerMaster);
  }

  private synchronized void onTaskGroupReceived(final TaskGroup taskGroup) {
    executorService.execute(() -> launchTaskGroup(taskGroup));
  }

  private void launchTaskGroup(final TaskGroup taskGroup) {
    taskGroupStateManager = new TaskGroupStateManager(taskGroup);
    new TaskGroupExecutor(taskGroup,
        taskGroupStateManager,
        physicalPlan.getStageDAG().getIncomingEdgesOf(taskGroup.getStageId()),
        physicalPlan.getStageDAG().getOutgoingEdgesOf(taskGroup.getStageId()),
        dataTransferFactory).execute();
  }

  /**
   * MessageListener for Executor.
   */
  private final class ExecutorMessageReceiver implements MessageListener<ControlMessage.Message> {

    @Override
    public void onSendMessage(final ControlMessage.Message message) {
      switch (message.getType()) {

      case ScheduleTaskGroup:
        final ControlMessage.ScheduleTaskGroupMsg scheduleTaskGroupMsg = message.getScheduleTaskGroupMsg();
        final TaskGroup taskGroup = SerializationUtils.deserialize(scheduleTaskGroupMsg.getTaskGroup().toByteArray());
        onTaskGroupReceived(taskGroup);
        break;
      case BlockLocationInfo:
        break;
      case RequestBlock:
        break;
      case TransferBlock:
        break;
      default:
        throw new IllegalMessageException(
            new Exception("This message should not be received by an executor :" + message.getType()));
      }
      physicalPlan = message.getPhysicalPlan();
    }

    @Override
    public void onRequestMessage(final ControlMessage.Message message, final MessageContext messageContext) {

    }
  }
}
