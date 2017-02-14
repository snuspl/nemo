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

import edu.snu.vortex.runtime.common.TaskGroup;
import edu.snu.vortex.runtime.common.comm.RuntimeMessages;
import edu.snu.vortex.runtime.common.config.ExecutorConfig;
import edu.snu.vortex.runtime.common.config.RtConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Executor.
 */
public class Executor {
  private final RtConfig rtConfig;

  private final ExecutorService schedulerThread;
  private final ExecutorService executeThreads;
  private final ExecutorService resubmitThread;

  private final Communicator communicator;

  public Executor(final RtConfig.RtExecMode executionMode,
                  final ExecutorConfig executorConfig) {
    this.rtConfig = new RtConfig(executionMode);
    this.schedulerThread = Executors.newSingleThreadExecutor();
    this.executeThreads = Executors.newFixedThreadPool(executorConfig.getNumExecutionThreads());
    this.resubmitThread = Executors.newSingleThreadExecutor();
    this.communicator = new Communicator();
    initialize();
  }

  public final void initialize() {
    communicator.initialize();
  }

  public void submitTaskGroupForExecution(final TaskGroup taskGroupToExecute) {

  }

  public void executeStream(final TaskGroup taskGroup) {

  }

  public void executeBatch(final TaskGroup taskGroup) {
  }

  private void reportTaskStateChange(final String taskId,
                                     final RuntimeMessages.TaskStateChangedMsg.TaskState newState) {
    final RuntimeMessages.TaskStateChangedMsg.Builder msgBuilder
        = RuntimeMessages.TaskStateChangedMsg.newBuilder();
    msgBuilder.setTaskId(taskId);
    msgBuilder.setState(newState);
    final RuntimeMessages.RtControllableMsg.Builder builder
        = RuntimeMessages.RtControllableMsg.newBuilder();
    builder.setType(RuntimeMessages.Type.TaskStateChanged);
    builder.setTaskStateChangedMsg(msgBuilder.build());
    communicator.sendRtControllable("master", builder.build());
  }
}
