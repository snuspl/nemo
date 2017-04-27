/*
 * Copyright (C) 2016 Seoul National University
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

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.Transform;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.comm.ExecutorMessage;
import edu.snu.vortex.runtime.common.plan.logical.LogicalDAGGenerator;
import edu.snu.vortex.runtime.common.plan.logical.Stage;
import edu.snu.vortex.runtime.common.plan.logical.StageEdge;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.master.scheduler.BatchScheduler;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * Tests {@link BatchScheduler}
 */
public final class BatchSchedulerTest {
  private final ExecutionStateManager executionStateManager = new ExecutionStateManager();
  private final BatchScheduler scheduler = new BatchScheduler(executionStateManager, RuntimeAttribute.Batch, 2000);
  private DAGBuilder<IRVertex, IREdge> irDAGBuilder;

  @Before
  public void setUp() {
    irDAGBuilder = new DAGBuilder<>();
  }

  /**
   * This method builds a physical DAG starting from an IR DAG to test whether the it successfully gets scheduled.
   */
  @Test
  public void testSimplePhysicalPlanScheduling() throws InterruptedException {
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v1.setAttr(Attribute.Key.Placement, Attribute.Compute);
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v1.setAttr(Attribute.Key.Placement, Attribute.Storage);
    irDAGBuilder.addVertex(v2);

    final IREdge e = new IREdge(IREdge.Type.ScatterGather, v1, v2);
    e.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
    irDAGBuilder.connectVertices(e);

    final DAG<IRVertex, IREdge> irDAG = irDAGBuilder.build();
    final DAG<Stage, StageEdge> logicalDAG = irDAG.convert(new LogicalDAGGenerator());
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = logicalDAG.convert(new PhysicalDAGGenerator());

    final ExecutorRepresenter a1 = new ExecutorRepresenter("a1", RuntimeAttribute.Compute, 1);
    final ExecutorRepresenter a2 = new ExecutorRepresenter("a2", RuntimeAttribute.Compute, 1);
    final ExecutorRepresenter a3 = new ExecutorRepresenter("a3", RuntimeAttribute.Compute, 1);
    final ExecutorRepresenter b1 = new ExecutorRepresenter("b1", RuntimeAttribute.Storage, 1);
    final ExecutorRepresenter b2 = new ExecutorRepresenter("b2", RuntimeAttribute.Storage, 1);


    scheduler.onExecutorAdded(a1);
//    scheduler.onExecutorAdded(a2);
//    scheduler.onExecutorAdded(a3);
    scheduler.onExecutorAdded(b1);
    Thread.sleep(2000);
//    scheduler.onExecutorAdded(b2);

    scheduler.scheduleJob(new PhysicalPlan("TestPlan", physicalDAG));

    for (final PhysicalStage physicalStage : physicalDAG.getVertices()) {
      physicalStage.getTaskGroupList().forEach(taskGroup -> {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
        final ExecutorMessage.TaskGroupStateChangedMsg.Builder taskGroupStateChangedMsg =
            ExecutorMessage.TaskGroupStateChangedMsg.newBuilder();
        taskGroupStateChangedMsg.setTaskGroupId(taskGroup.getTaskGroupId());
        taskGroupStateChangedMsg.setState(ExecutorMessage.TaskGroupStateFromExecutor.COMPLETE);
        scheduler.onTaskGroupStateChanged(a1.getExecutorId(), taskGroupStateChangedMsg.build());
      });
    }
    executionStateManager.printCurrentJobExecutionState();

    scheduler.terminate();
  }
}
