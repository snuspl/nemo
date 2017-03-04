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
import edu.snu.vortex.runtime.common.config.ExecutorConfig;
import edu.snu.vortex.runtime.common.config.RtConfig;
import edu.snu.vortex.runtime.master.MasterCommunicator;
import edu.snu.vortex.runtime.master.RtMaster;

import java.util.Map;
import java.util.logging.Logger;

/**
 * ExecutorContainer.
 */
public class ExecutorContainer {
  private static final Logger LOG = Logger.getLogger(ExecutorContainer.class.getName());

  private final String executorId;

  private final RtMaster master;
  private final Executor executor;
  private final ExecutorConfig executorConfig;
  private final ExecutorCommunicator executorCommunicator;
  private final DataTransferManager dataTransferManager;

  public ExecutorContainer(final RtMaster master,
                           final String executorId,
                           final RtConfig.RtExecMode executionMode,
                           final ExecutorConfig executorConfig) {
    this.master = master;
    this.executorId = executorId;
    this.executor = new Executor(executionMode, executorConfig);
    this.executorCommunicator = new ExecutorCommunicator(executor, executorId);
    this.dataTransferManager =
        new DataTransferManager(executorId, executorId, RtConfig.MASTER_NAME, executorCommunicator);
    this.executorConfig = executorConfig;
    initialize();
  }

  public ExecutorConfig getExecutorConfig() {
    return executorConfig;
  }

  public void initialize(final Map<String, Communicator> routingTable) {
    executorCommunicator.initialize(dataTransferManager, routingTable);
    executor.initialize(executorCommunicator);
    sendExecutorReadyMsg();
  }

  public String getExecutorId() {
    return executorId;
  }

  public ExecutorCommunicator getExecutorCommunicator() {
    return executorCommunicator;
  }

  private void sendExecutorReadyMsg() {
    final RuntimeDefinitions.ExecutorReadyMsg.Builder msgBuilder
        = RuntimeDefinitions.ExecutorReadyMsg.newBuilder();
    msgBuilder.setExecutorId(executorId);
    final RuntimeDefinitions.RtControllableMsg.Builder builder
        = RuntimeDefinitions.RtControllableMsg.newBuilder();
    builder.setType(RuntimeDefinitions.MessageType.ExecutorReady);
    builder.setExecutorReadyMsg(msgBuilder.build());
    executorCommunicator.sendRtControllable(RtConfig.MASTER_NAME, builder.build());
  }

  public void terminate() {
    cleanup();
    executor.terminate();
    executorCommunicator.terminate();
  }

  private void cleanup() {
  }
}
