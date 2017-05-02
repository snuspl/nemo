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

import edu.snu.vortex.runtime.common.message.MessageContext;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.executor.datatransfer.DataTransferFactory;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class Executor {

  private final String executorId;
  private final int numCores;
  private final ExecutorService executorService;
  private final DataTransferFactory dataTransferFactory;

  private PhysicalPlan physicalPlan;

  public Executor(final String executorId, final int numCores, final MessageEnvironment messageEnvironment) {
    this.executorId = executorId;
    this.numCores = numCores;
    this.executorService = Executors.newFixedThreadPool(numCores);
    this.dataTransferFactory = new DataTransferFactory();
  }

  private synchronized void onTaskGroupReceived(final TaskGroup taskGroup) {
    executorService.execute(() -> launchTaskGroup(taskGroup));
  }

  private void launchTaskGroup(final TaskGroup taskGroup) {
    final TaskGroupStateManager taskGroupStateManager = new TaskGroupStateManager(messageSenderToDriver, taskGroup);
    new TaskGroupExecutor(taskGroup,
        taskGroupStateManager,
        physicalPlan.getStageDAG().getIncomingEdgesOf(taskGroup.getStageId()),
        physicalPlan.getStageDAG().getOutgoingEdgesOf(taskGroup.getStageId()),
        dataTransferFactory).execute();
  }


  private final class Hello implements MessageListener<Serializable> {

    @Override
    public void onSendMessage(Serializable message) {
      // TODO #: call onTaskGroupReceived()
      // TODO #: Set physical plan
    }

    @Override
    public void onRequestMessage(Serializable message, MessageContext messageContext) {

    }
  }
}
