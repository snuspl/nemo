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

import edu.snu.vortex.runtime.common.comm.Communicator;
import edu.snu.vortex.runtime.common.comm.RuntimeDefinitions;
import edu.snu.vortex.runtime.common.config.RtConfig;
import edu.snu.vortex.runtime.common.task.TaskGroup;
import edu.snu.vortex.runtime.exception.UnsupportedRtControllable;
import org.apache.commons.lang.SerializationUtils;

import java.util.logging.Logger;


/**
 * ExecutorCommunicator.
 */
public class ExecutorCommunicator extends Communicator {
  private static final Logger LOG = Logger.getLogger(ExecutorCommunicator.class.getName());

  private Executor executor;
  private DataTransferManager transferManager;
  private final String executorId;

  public ExecutorCommunicator(final String executorId) {
    super(executorId);
    this.executorId = executorId;
  }

  public void initialize(final Executor executor,
                         final DataTransferManager transferManager) {
    this.executor = executor;
    this.transferManager = transferManager;

    // Send Executor ready message to master
    final RuntimeDefinitions.ExecutorReadyMsg.Builder msgBuilder
        = RuntimeDefinitions.ExecutorReadyMsg.newBuilder();
    msgBuilder.setExecutorId(executorId);
    final RuntimeDefinitions.RtControllableMsg.Builder builder
        = RuntimeDefinitions.RtControllableMsg.newBuilder();
    builder.setType(RuntimeDefinitions.MessageType.ExecutorReady);
    builder.setExecutorReadyMsg(msgBuilder.build());
    sendRtControllable(RtConfig.MASTER_NAME, builder.build());
  }

  @Override
  public void processRtControllable(final RuntimeDefinitions.RtControllableMsg rtControllable) {
    switch (rtControllable.getType()) {
    case ScheduleTaskGroup:
      final TaskGroup toSchedule =
          (TaskGroup) SerializationUtils.deserialize(
              rtControllable.getScheduleTaskGroupMsg().getTaskGroup().toByteArray());
      executor.submitTaskGroupForExecution(toSchedule);
      break;
    case TransferReady:
      final RuntimeDefinitions.TransferReadyMsg transferReadyMsg = rtControllable.getTransferReadyMsg();
      transferManager.triggerTransferReadyNotifyCallback(transferReadyMsg.getChannelId(),
          transferReadyMsg.getSendExecutorId());
      break;

    default:
      throw new UnsupportedRtControllable("This RtControllable is not supported by executors");
    }
  }
}
