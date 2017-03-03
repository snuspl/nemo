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

import edu.snu.vortex.runtime.common.comm.Communicator;
import edu.snu.vortex.runtime.common.comm.RuntimeDefinitions;
import edu.snu.vortex.runtime.common.config.RtConfig;
import edu.snu.vortex.runtime.exception.UnsupportedRtControllable;
import edu.snu.vortex.runtime.master.transfer.DataTransferManagerMaster;

import java.util.logging.Logger;

/**
 * ExecutorCommunicator.
 */
public class MasterCommunicator extends Communicator {
  private static final Logger LOG = Logger.getLogger(MasterCommunicator.class.getName());

  private ResourceManager resourceManager;
  private ExecutionStateManager executionStateManager;
  private DataTransferManagerMaster transferMgrMaster;

  public MasterCommunicator() {
    super(RtConfig.MASTER_NAME);
  }

  public void initialize(final ResourceManager resourceManager,
                         final ExecutionStateManager executionStateManager,
                         final DataTransferManagerMaster transferMgrMaster) {
    this.resourceManager = resourceManager;
    this.executionStateManager = executionStateManager;
    this.transferMgrMaster = transferMgrMaster;
  }

  @Override
  public void processRtControllable(final RuntimeDefinitions.RtControllableMsg rtControllable) {
    switch (rtControllable.getType()) {
    case ExecutorReady:
      final String executorId = rtControllable.getExecutorReadyMsg().getExecutorId();
      final Communicator newCommunicator = resourceManager.getResourceById(executorId).getExecutorCommunicator();
      resourceManager.onResourceAllocated(executorId);
      registerNewRemoteCommunicator(executorId, newCommunicator);
      getRoutingTable().forEach(((id, communicator) ->
          communicator.registerNewRemoteCommunicator(executorId, newCommunicator)));
      break;
    case TaskStateChanged:
      final RuntimeDefinitions.TaskStateChangedMsg taskStateChangedMsg = rtControllable.getTaskStateChangedMsg();
      final String taskGroupId = taskStateChangedMsg.getTaskGroupId();
      final RuntimeDefinitions.TaskState newState = taskStateChangedMsg.getState();
      executionStateManager.onTaskGroupStateChanged(taskGroupId, newState);
      break;
    case ChannelBind:
      if (rtControllable.getChannelBindMsg().getChannelType() == RuntimeDefinitions.ChannelType.READER) {
        transferMgrMaster.bindChannelReaderToExecutor(
            rtControllable.getChannelBindMsg().getChannelId(),
            rtControllable.getChannelBindMsg().getExecutorId());
      } else {
        transferMgrMaster.bindChannelWriterToExecutor(
            rtControllable.getChannelBindMsg().getChannelId(),
            rtControllable.getChannelBindMsg().getExecutorId());
      }
      break;
    case ChannelUnbind:
      if (rtControllable.getChannelBindMsg().getChannelType() == RuntimeDefinitions.ChannelType.READER) {
        transferMgrMaster.unbindChannelReader(rtControllable.getChannelUnbindMsg().getChannelId());
      } else {
        transferMgrMaster.unbindChannelWriter(rtControllable.getChannelUnbindMsg().getChannelId());
      }
      break;
    case TransferReady:
      transferMgrMaster.notifyTransferReadyToReceiver(rtControllable.getTransferReadyMsg().getChannelId());
      break;

    default:
      throw new UnsupportedRtControllable("This RtControllable is not supported by executors");
    }
  }
}
