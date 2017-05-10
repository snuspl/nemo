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

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageContext;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.vortex.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.vortex.runtime.exception.UnsupportedMessageException;
import edu.snu.vortex.runtime.master.resourcemanager.LocalResourceManager;
import edu.snu.vortex.runtime.master.resourcemanager.ResourceManager;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan;
import edu.snu.vortex.runtime.common.plan.logical.Stage;
import edu.snu.vortex.runtime.common.plan.logical.StageEdge;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalDAGGenerator;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.master.scheduler.BatchScheduler;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;
import edu.snu.vortex.utils.dag.DAG;

import java.util.logging.Logger;

/**
 * Runtime Master is the central controller of Runtime.
 * Compiler submits an {@link ExecutionPlan} to Runtime Master to execute a job.
 * Runtime Master handles:
 *    a) Physical conversion of a job's DAG into a physical plan.
 *    b) Scheduling the job with {@link BatchScheduler}.
 *    c) (Please list others done by Runtime Master as features are added).
 */
public final class RuntimeMaster {
  private static final Logger LOG = Logger.getLogger(RuntimeMaster.class.getName());

  // TODO #: Configuration
  private static final int DEFAULT_NUM_EXECUTOR = 4;
  private static final int DEFAULT_EXECUTOR_CAPACITY = 4;

  private final Scheduler scheduler;
  private final ResourceManager resourceManager;
  private final LocalMessageDispatcher localMessageDispatcher;
  private final MessageEnvironment masterMessageEnvironment;
  private final BlockManagerMaster blockManagerMaster;
  private ExecutionStateManager executionStateManager;

  public RuntimeMaster(final RuntimeAttribute schedulerType) {
    switch (schedulerType) {
    case Batch:
      this.scheduler = new BatchScheduler(RuntimeAttribute.RoundRobin, 2000);
      break;
    default:
      throw new RuntimeException("Unknown scheduler type");
    }
    this.localMessageDispatcher = new LocalMessageDispatcher();
    this.masterMessageEnvironment =
        new LocalMessageEnvironment(MessageEnvironment.MASTER_COMMUNICATION_ID, localMessageDispatcher);
    masterMessageEnvironment.setupListener(MessageEnvironment.MASTER_MESSAGE_RECEIVER, new MasterMessageReceiver());
    this.blockManagerMaster = new BlockManagerMaster();
    this.resourceManager = new LocalResourceManager(scheduler, masterMessageEnvironment, blockManagerMaster);
  }

  /**
   * Submits the {@link ExecutionPlan} to Runtime.
   * @param executionPlan to execute.
   * @param dagDirectory the directory to which JSON representation of the plan is saved
   */
  public void execute(final ExecutionPlan executionPlan, final String dagDirectory) {
    final PhysicalPlan physicalPlan = generatePhysicalPlan(executionPlan, dagDirectory);
    try {
      new SimpleRuntime().executePhysicalPlan(physicalPlan);
      // to be replaced by:
      // executionStateManager = scheduler.scheduleJob(physicalPlan);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generates the {@link PhysicalPlan} to be executed.
   * @param executionPlan that should be converted to a physical plan
   * @param dagDirectory the directory to which JSON representation of the plan is saved
   * @return {@link PhysicalPlan} to execute.
   */
  private PhysicalPlan generatePhysicalPlan(final ExecutionPlan executionPlan, final String dagDirectory) {
    final DAG<Stage, StageEdge> logicalDAG = executionPlan.getRuntimeStageDAG();
    logicalDAG.storeJSON(dagDirectory, "plan-logical", "logical execution plan");

    final PhysicalPlan physicalPlan = new PhysicalPlan(executionPlan.getId(),
        logicalDAG.convert(new PhysicalDAGGenerator()));
    physicalPlan.getStageDAG().storeJSON(dagDirectory, "plan-physical", "physical execution plan");
    return physicalPlan;
  }

  /**
   * Handler for messages received by Master.
   */
  private final class MasterMessageReceiver implements MessageListener<ControlMessage.Message> {

    @Override
    public void onSendMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
      case TaskGroupStateChanged:
//        scheduler.onTaskGroupStateChanged(message.getTaskStateChangedMsg().getTaskGroupId());
        break;
      default:
        throw new UnsupportedMessageException(
            new Exception("This message type is not supported: " + message.getType()));
      }
      // scheduler.onTaskGroupStateChanged();
    }

    @Override
    public void onRequestMessage(final ControlMessage.Message message, final MessageContext messageContext) {
    }
  }
}
