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

import edu.snu.vortex.runtime.common.RuntimeStates;
import edu.snu.vortex.runtime.common.task.TaskGroup;
import edu.snu.vortex.runtime.common.comm.RuntimeDefinitions;
import edu.snu.vortex.runtime.common.config.ExecutorConfig;
import edu.snu.vortex.runtime.common.config.RtConfig;
import edu.snu.vortex.runtime.exception.UnsupportedRtConfigException;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

/**
 * Executor.
 */
public class Executor {
  private static final Logger LOG = Logger.getLogger(Executor.class.getName());
  private final RtConfig rtConfig;

  private final ExecutorService executeThreads;
  private final ExecutorService resubmitThread;
  private final BlockingDeque<TaskGroup> incomingTaskGroups;

  private ExecutorCommunicator executorCommunicator;

  public Executor(final RtConfig.RtExecMode executionMode,
                  final ExecutorConfig executorConfig) {
    this.rtConfig = new RtConfig(executionMode);
    this.executeThreads = Executors.newFixedThreadPool(executorConfig.getNumExecutionThreads());
    this.resubmitThread = Executors.newSingleThreadExecutor();
    this.incomingTaskGroups = new LinkedBlockingDeque<>();
  }

  public void initialize(final ExecutorCommunicator executorCommunicator) {
    this.executorCommunicator = executorCommunicator;
    executeThreads.execute(new RtControllableMsgHandler());
  }

  public void submitTaskGroupForExecution(final TaskGroup taskGroup) {
    incomingTaskGroups.offer(taskGroup);
  }

  private void executeStream(final TaskGroup taskGroup) {
    taskGroup.getTaskList().forEach(t -> t.compute());
    reportTaskStateChange(taskGroup.getTaskGroupId(), RuntimeStates.TaskGroupState.RUNNING);
  }

  private void executeBatch(final TaskGroup taskGroup) {
    taskGroup.getTaskList().forEach(t -> t.compute());
    reportTaskStateChange(taskGroup.getTaskGroupId(), RuntimeStates.TaskGroupState.RUNNING);
  }

  private void reportTaskStateChange(final String taskGroupId,
                                     final RuntimeStates.TaskGroupState newState) {
    final RuntimeDefinitions.TaskGroupStateChangedMsg.Builder msgBuilder
        = RuntimeDefinitions.TaskGroupStateChangedMsg.newBuilder();
    msgBuilder.setTaskGroupId(taskGroupId);
    msgBuilder.setState(newState);
    final RuntimeDefinitions.RtControllableMsg.Builder builder
        = RuntimeDefinitions.RtControllableMsg.newBuilder();
    builder.setType(RuntimeDefinitions.MessageType.TaskGroupStateChanged);
    builder.setTaskStateChangedMsg(msgBuilder.build());
    executorCommunicator.sendRtControllable("master", builder.build());
  }

  /**
   * RtControllableMsgHandler.
   */
  private class RtControllableMsgHandler implements Runnable {

    @Override
    public void run() {
      while (!executeThreads.isShutdown()) {
        try {
          final TaskGroup taskGroup = incomingTaskGroups.take();
          switch (rtConfig.getRtExecMode()) {
          case STREAM:
            executeStream(taskGroup);
            break;
          case BATCH:
            executeBatch(taskGroup);
            break;
          default:
            throw new UnsupportedRtConfigException("RtExecMode should be STREAM or BATCH");
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void terminate() {
    executeThreads.shutdown();
    resubmitThread.shutdown();
    incomingTaskGroups.clear();
  }
}
