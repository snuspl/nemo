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
import edu.snu.vortex.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.vortex.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.exception.IllegalMessageException;
import edu.snu.vortex.runtime.executor.datatransfer.DataTransferFactory;
import edu.snu.vortex.runtime.master.BlockManagerMaster;
import org.apache.commons.lang3.SerializationUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executor.
 */
public final class Executor {
  private static final Logger LOG = Logger.getLogger(Executor.class.getName());

  private final String executorId;
  private final int capacity;
  private final MessageEnvironment myMessageEnvironment;
  private final Map<String, MessageSender<ControlMessage.Message>> nodeIdToMsgSenderMap;
  private final ExecutorService executorService;
  private final DataTransferFactory dataTransferFactory;

  private PhysicalPlan physicalPlan;
  private TaskGroupStateManager taskGroupStateManager;

  public Executor(final String executorId,
                  final int capacity,
                  final LocalMessageDispatcher localMessageDispatcher,
                  final BlockManagerMaster blockManagerMaster) {
    this.executorId = executorId;
    this.capacity = capacity;
    this.myMessageEnvironment = new LocalMessageEnvironment(executorId, localMessageDispatcher);
    this.nodeIdToMsgSenderMap = new HashMap<>();
    myMessageEnvironment.setupListener(MessageEnvironment.EXECUTOR_MESSAGE_RECEIVER, new ExecutorMessageReceiver());
    connectToOtherNodes(myMessageEnvironment);
    this.executorService = Executors.newFixedThreadPool(capacity);

    // TODO #: Check
    this.dataTransferFactory = new DataTransferFactory(executorId, blockManagerMaster);
  }

  private void connectToOtherNodes(final MessageEnvironment myMessageEnvironment) {
    // Connect to Master for now.
    try {
      nodeIdToMsgSenderMap.put(MessageEnvironment.MASTER_COMMUNICATION_ID,
          myMessageEnvironment.<ControlMessage.Message>asyncConnect(
              MessageEnvironment.MASTER_COMMUNICATION_ID, MessageEnvironment.MASTER_MESSAGE_RECEIVER).get());
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  private synchronized void onTaskGroupReceived(final TaskGroup taskGroup) {
    LOG.log(Level.INFO, "Executor [{0}] received TaskGroup [{1}] to execute.",
        new Object[]{executorId, taskGroup.getTaskGroupId()});
    executorService.execute(() -> launchTaskGroup(taskGroup));
  }

  private void launchTaskGroup(final TaskGroup taskGroup) {
    taskGroupStateManager = new TaskGroupStateManager(taskGroup, executorId, nodeIdToMsgSenderMap);
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
      case BroadcastPhysicalPlan:
        final ControlMessage.BroadcastPhysicalPlanMsg broadcastPhysicalPlanMsg = message.getBroadcastPhysicalPlanMsg();
        physicalPlan = SerializationUtils.deserialize(broadcastPhysicalPlanMsg.getPhysicalPlan().toByteArray());
        break;
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
    }

    @Override
    public void onRequestMessage(final ControlMessage.Message message, final MessageContext messageContext) {

    }
  }
}
