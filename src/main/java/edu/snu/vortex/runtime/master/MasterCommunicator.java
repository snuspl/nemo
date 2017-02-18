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
import edu.snu.vortex.runtime.common.comm.RtControllable;
import edu.snu.vortex.runtime.common.comm.RuntimeDefinitions;
import edu.snu.vortex.runtime.common.config.RtConfig;
import edu.snu.vortex.runtime.exception.UnsupportedRtControllable;

import java.util.logging.Logger;

/**
 * ExecutorCommunicator.
 */
public class MasterCommunicator extends Communicator{
  private static final Logger LOG = Logger.getLogger(MasterCommunicator.class.getName());

  private ResourceManager resourceManager;

  public MasterCommunicator() {
    super(RtConfig.MASTER_NAME);
  }

  public void initialize(final ResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public void processRtControllable(final RtControllable rtControllable) {
    final RuntimeDefinitions.RtControllableMsg message = rtControllable.getMessage();
    switch (message.getType()) {
    case ExecutorReady:
      final String executorId = message.getExecutorReadyMsg().getExecutorId();
      final Communicator newCommunicator = resourceManager.getResourceById(executorId).getExecutorCommunicator();
      resourceManager.onResourceAllocated(executorId);
      registerNewRemoteCommunicator(executorId, newCommunicator);
      routingTable.forEach(((id, communicator) ->
          communicator.registerNewRemoteCommunicator(executorId, newCommunicator)));
      break;
    default:
      throw new UnsupportedRtControllable("This RtControllable is not supported by executors");
    }
  }
}
