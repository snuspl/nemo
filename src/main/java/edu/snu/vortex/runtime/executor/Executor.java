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

import edu.snu.vortex.runtime.common.ExecutionState;
import edu.snu.vortex.runtime.common.TaskGroup;
import edu.snu.vortex.runtime.common.TaskLabel;
import edu.snu.vortex.runtime.common.comm.RtControllable;
import edu.snu.vortex.runtime.common.comm.TaskStateChangedMsg;
import edu.snu.vortex.runtime.common.config.ExecutorConfig;
import edu.snu.vortex.runtime.common.config.RtConfig;

import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class Executor {
  private final String executorId;
  private final RtConfig rtConfig;

  private final ExecutorService schedulerThread;
  private final ExecutorService executeThreads;
  private final ExecutorService resubmitThread;

  private final BlockingDeque<RtControllable> incomingRtControllables;
  private final BlockingDeque<RtControllable> outgoingRtControllables;

  public Executor(final String executorId,
                  final RtConfig.RtExecMode executionMode,
                  final ExecutorConfig executorConfig) {
    this.executorId = executorId;
    this.rtConfig = new RtConfig(executionMode);
    this.schedulerThread = Executors.newSingleThreadExecutor();
    this.executeThreads = Executors.newFixedThreadPool(executorConfig.getNumExecutionThreads());
    this.resubmitThread = Executors.newSingleThreadExecutor();
    this.incomingRtControllables = new LinkedBlockingDeque<>();
    this.outgoingRtControllables = new LinkedBlockingDeque<>();
    initialize();
  }

  public void initialize() {
    schedulerThread.execute(new RtExchangeableHandler());
  }

  public void submitTaskGroupForExecution(final TaskGroup taskGroupToExecute) {

  }

  public void executeStream(final List<TaskLabel> taskLabelList) {

  }

  public void executeBatch(final List<TaskLabel> taskLabelList) {
  }

  private void reportTaskStateChange(final String taskId,
                                     final ExecutionState.TaskState newState) {
    // Create RtControllable
    final RtControllable toSend = new RtControllable("this", "master",
        RtControllable.Type.TaskStateChanged, new TaskStateChangedMsg(taskId, newState));
    // Send RtExechangeable to master
    outgoingRtControllables.offer(toSend);
  }

  private void onRtExchangeableReceived(final RtControllable rtControllable) {
    incomingRtControllables.offer(rtControllable);
  }

  private void sendRtExchangeable(final RtControllable rtControllable) {
    outgoingRtControllables.offer(rtControllable);
  }

  private class RtExchangeableHandler implements Runnable {
    @Override
    public void run() {
      final RtControllable rtControllable;
      try {
        rtControllable = incomingRtControllables.take();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  // sendExecutorHeartBeat handled by REEF
  // sendRtExchangeables handled by REEF
  // receiveRtExchangeables handled by REEF
}
