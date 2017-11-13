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
package edu.snu.onyx.runtime.master.scheduler;

import edu.snu.onyx.common.PubSubEventHandlerWrapper;
import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.compiler.CompilerTestUtil;
import edu.snu.onyx.compiler.frontend.beam.transform.DoTransform;
import edu.snu.onyx.compiler.ir.IREdge;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.compiler.ir.OperatorVertex;
import edu.snu.onyx.compiler.ir.Transform;
import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.compiler.ir.executionproperty.vertex.ExecutorPlacementProperty;
import edu.snu.onyx.compiler.ir.executionproperty.vertex.ParallelismProperty;
import edu.snu.onyx.compiler.optimizer.Optimizer;
import edu.snu.onyx.compiler.optimizer.TestPolicy;
import edu.snu.onyx.compiler.optimizer.examples.EmptyComponents;
import edu.snu.onyx.runtime.RuntimeTestUtil;
import edu.snu.onyx.runtime.common.comm.ControlMessage;
import edu.snu.onyx.runtime.common.message.MessageSender;
import edu.snu.onyx.runtime.common.metric.MetricMessageHandler;
import edu.snu.onyx.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.onyx.runtime.common.plan.physical.PhysicalPlanGenerator;
import edu.snu.onyx.runtime.common.plan.physical.PhysicalStage;
import edu.snu.onyx.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.onyx.runtime.common.state.StageState;
import edu.snu.onyx.runtime.executor.datatransfer.communication.ScatterGather;
import edu.snu.onyx.runtime.master.JobStateManager;
import edu.snu.onyx.runtime.master.PartitionManagerMaster;
import edu.snu.onyx.runtime.master.eventhandler.UpdatePhysicalPlanEventHandler;
import edu.snu.onyx.runtime.master.resource.ContainerManager;
import edu.snu.onyx.runtime.master.resource.ExecutorRepresenter;
import edu.snu.onyx.runtime.master.resource.ResourceSpecification;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests fault tolerance.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ContainerManager.class, PartitionManagerMaster.class,
    PubSubEventHandlerWrapper.class, UpdatePhysicalPlanEventHandler.class, MetricMessageHandler.class})
public final class FaultToleranceTest {
  private static final Logger LOG = LoggerFactory.getLogger(FaultToleranceTest.class.getName());
  private Scheduler scheduler;
  private SchedulingPolicy schedulingPolicy;
  private SchedulerRunner schedulerRunner;

  private ContainerManager containerManager;
  private final Map<String, ExecutorRepresenter> executorRepresenterMap = new HashMap<>();

  private MetricMessageHandler metricMessageHandler;
  private PendingTaskGroupQueue pendingTaskGroupQueue;
  private PubSubEventHandlerWrapper pubSubEventHandler;
  private UpdatePhysicalPlanEventHandler updatePhysicalPlanEventHandler;
  private PartitionManagerMaster partitionManagerMaster = mock(PartitionManagerMaster.class);
  private final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);
  private PhysicalPlanGenerator physicalPlanGenerator;

  private static final int TEST_TIMEOUT_MS = 500;

  // This schedule index will make sure that task group events are not ignored
  private static final int MAGIC_SCHEDULE_ATTEMPT_INDEX = Integer.MAX_VALUE;

  @Before
  public void setUp() throws Exception {
    RuntimeTestUtil.initialize();

    pendingTaskGroupQueue = new SingleJobTaskGroupQueue();
    schedulingPolicy = new RoundRobinSchedulingPolicy(containerManager, TEST_TIMEOUT_MS);
    schedulerRunner = mock(SchedulerRunner.class);
    scheduler =
        new BatchSingleJobScheduler(schedulingPolicy, schedulerRunner, pendingTaskGroupQueue,
            partitionManagerMaster, pubSubEventHandler, updatePhysicalPlanEventHandler);

    containerManager = mock(ContainerManager.class);
    when(containerManager.getExecutorRepresenterMap()).thenReturn(executorRepresenterMap);

    metricMessageHandler = mock(MetricMessageHandler.class);
    pubSubEventHandler = mock(PubSubEventHandlerWrapper.class);
    updatePhysicalPlanEventHandler = mock(UpdatePhysicalPlanEventHandler.class);

    final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();

    final ResourceSpecification computeSpec = new ResourceSpecification(ExecutorPlacementProperty.COMPUTE, 2, 0);
    final ExecutorRepresenter a3 = new ExecutorRepresenter("a3", computeSpec, mockMsgSender, activeContext);
    final ExecutorRepresenter a2 = new ExecutorRepresenter("a2", computeSpec, mockMsgSender, activeContext);
    final ExecutorRepresenter a1 = new ExecutorRepresenter("a1", computeSpec, mockMsgSender, activeContext);

    executorRepresenterMap.put(a1.getExecutorId(), a1);
    executorRepresenterMap.put(a2.getExecutorId(), a2);
    executorRepresenterMap.put(a3.getExecutorId(), a3);

    // Add compute nodes
    scheduler.onExecutorAdded(a1.getExecutorId());
    scheduler.onExecutorAdded(a2.getExecutorId());
    scheduler.onExecutorAdded(a3.getExecutorId());

    physicalPlanGenerator = Tang.Factory.getTang().newInjector().getInstance(PhysicalPlanGenerator.class);
  }

  /**
   *
   */
  @Test(timeout=10000)
  public void testContainerRemoval() throws Exception {
    final DAG<IRVertex, IREdge> mrDAG = Optimizer.optimize(CompilerTestUtil.compileMRDAG(), new TestPolicy(), "");
    System.err.println(mrDAG);
    scheduleAndCheckJobTermination(mrDAG);
  }

  private void scheduleAndCheckJobTermination(final DAG<IRVertex, IREdge> irDAG) throws InjectionException {
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    final PhysicalPlan plan = new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap());
    final JobStateManager jobStateManager = new JobStateManager(plan, partitionManagerMaster, metricMessageHandler, 1);
    scheduler.scheduleJob(plan, jobStateManager);

    RuntimeTestUtil.cleanup();
  }

  private int getNumScheduleGroups(final DAG<IRVertex, IREdge> irDAG) {
    final Set<Integer> scheduleGroupSet = new HashSet<>();
    irDAG.getVertices().forEach(irVertex ->
        scheduleGroupSet.add((Integer) irVertex.getProperty(ExecutionProperty.Key.ScheduleGroupIndex)));
    return scheduleGroupSet.size();
  }
}
