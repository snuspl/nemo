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

import edu.snu.vortex.runtime.common.comm.RuntimeDefinitions;
import edu.snu.vortex.runtime.common.config.ExecutorConfig;
import edu.snu.vortex.runtime.common.config.RtConfig;
import edu.snu.vortex.runtime.master.RtMaster;

import java.io.Serializable;
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
  private final DataTransferManager transferManager;

  public ExecutorContainer(final RtMaster master,
                           final String executorId,
                           final RtConfig.RtExecMode executionMode,
                           final ExecutorConfig executorConfig) {
    this.master = master;
    this.executorId = executorId;
    this.executorCommunicator = new ExecutorCommunicator(executorId);
    this.transferManager = new DataTransferManager(executorId, RtConfig.MASTER_NAME, executorCommunicator);
    this.executor = new Executor(executionMode, executorConfig);
    this.executorConfig = executorConfig;
    initialize();
  }

  public ExecutorConfig getExecutorConfig() {
    return executorConfig;
  }

  public void initialize() {
    executorCommunicator.initialize(executor);
    executor.initialize(executorCommunicator);
    sendExecutorReadyMsg();
  }

  public ExecutorCommunicator getExecutorCommunicator() {
    return executorCommunicator;
  }

  public DataTransferManager getDataTransferManager() {
    return transferManager;
  }

  private void sendExecutorReadyMsg() {
    final RuntimeDefinitions.ExecutorReadyMsg.Builder msgBuilder
        = RuntimeDefinitions.ExecutorReadyMsg.newBuilder();
    msgBuilder.setExecutorId(executorId);
    final RuntimeDefinitions.RtControllableMsg.Builder builder
        = RuntimeDefinitions.RtControllableMsg.newBuilder();
    builder.setType(RuntimeDefinitions.MessageType.ExecutorReady);
    builder.setExecutorReadyMsg(msgBuilder.build());
    executorCommunicator.sendRtControllable(RtConfig.MASTER_NAME, builder.build(), new Serializable() { });
  }

  public void terminate() {
    cleanup();
    executor.terminate();
    executorCommunicator.terminate();
  }

  private void cleanup() {
  }
}
