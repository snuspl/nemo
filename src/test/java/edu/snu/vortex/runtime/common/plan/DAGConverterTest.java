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
package edu.snu.vortex.runtime.common.plan;

import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.Transform;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan;
import edu.snu.vortex.runtime.common.plan.logical.LogicalDAGGenerator;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeStage;
import edu.snu.vortex.runtime.common.plan.logical.StageEdge;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.master.RuntimeMaster;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;
import edu.snu.vortex.utils.dag.Edge;
import org.apache.beam.sdk.io.BoundedSource;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link LogicalDAGGenerator} and {@link PhysicalDAGGenerator}
 */
public final class DAGConverterTest<I, O> {
  private DAGBuilder<IRVertex, IREdge<I, O>> irDAGBuilder;
  private final RuntimeMaster runtimeMaster = new RuntimeMaster();

  @Before
  public void setUp() {
    irDAGBuilder = new DAGBuilder<>();
  }

  @Test
  public void testSimplePlan() {
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setAttr(Attribute.IntegerKey.Parallelism, 3);
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setAttr(Attribute.IntegerKey.Parallelism, 2);
    irDAGBuilder.addVertex(v2);

    final IREdge e = new IREdge(IREdge.Type.ScatterGather, v1, v2);
    e.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
    irDAGBuilder.connectVertices(e);

    final DAG<IRVertex, IREdge<I, O>> irDAG = irDAGBuilder.build();
    final DAG<RuntimeStage, StageEdge> logicalDAG = irDAG.convert(new LogicalDAGGenerator<>());
    final DAG<PhysicalStage, StageBoundaryEdgeInfo> physicalDAG = logicalDAG.convert(new PhysicalDAGGenerator());

    // Test Logical DAG
    final List<RuntimeStage> sortedLogicalDAG = logicalDAG.getTopologicalSort();
    final RuntimeStage runtimeStage1 = sortedLogicalDAG.get(0);
    final RuntimeStage runtimeStage2 = sortedLogicalDAG.get(1);

    assertEquals(logicalDAG.getVertices().size(), 2);
    assertEquals(logicalDAG.getIncomingEdges(runtimeStage1).size(), 0);
    assertEquals(logicalDAG.getIncomingEdges(runtimeStage2).size(), 1);
    assertEquals(logicalDAG.getOutgoingEdges(runtimeStage1).size(), 1);
    assertEquals(logicalDAG.getOutgoingEdges(runtimeStage2).size(), 0);

    // Test Physical DAG
    final List<PhysicalStage> sortedPhysicalDAG = physicalDAG.getTopologicalSort();
    final PhysicalStage physicalStage1 = sortedPhysicalDAG.get(0);
    final PhysicalStage physicalStage2 = sortedPhysicalDAG.get(1);
    assertEquals(physicalDAG.getVertices().size(), 2);
    assertEquals(physicalDAG.getIncomingEdges(physicalStage1).size(), 0);
    assertEquals(physicalDAG.getIncomingEdges(physicalStage2).size(), 1);
    assertEquals(physicalDAG.getOutgoingEdges(physicalStage1).size(), 1);
    assertEquals(physicalDAG.getOutgoingEdges(physicalStage2).size(), 0);

    final List<TaskGroup> taskGroupList1 = physicalStage1.getTaskGroupList();
    final List<TaskGroup> taskGroupList2 = physicalStage2.getTaskGroupList();
    assertEquals(taskGroupList1.size(), 3);
    assertEquals(taskGroupList2.size(), 2);
  }

  @Test
  public void testComplexPlan() {
    // Tests a plan of 4 stages.
    final BoundedSource s = mock(BoundedSource.class);
    final IRVertex v1 = new BoundedSourceVertex<>(s);
    v1.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v1.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final Transform t = mock(Transform.class);
    final IRVertex v2 = new OperatorVertex(t);
    v2.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v2.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final IRVertex v3 = new OperatorVertex(t);
    v3.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v3.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final IRVertex v4 = new OperatorVertex(t);
    v4.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v4.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final IRVertex v5 = new OperatorVertex(t);
    v5.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v5.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final IRVertex v6 = new OperatorVertex(t);
    v6.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v6.setAttr(Attribute.Key.Placement, Attribute.Reserved);

    final IRVertex v7 = new OperatorVertex(t);
    v7.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v7.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final IRVertex v8 = new OperatorVertex(t);
    v8.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v8.setAttr(Attribute.Key.Placement, Attribute.Compute);

    irDAGBuilder.addVertex(v1);
    irDAGBuilder.addVertex(v2);
    irDAGBuilder.addVertex(v3);
    irDAGBuilder.addVertex(v4);
    irDAGBuilder.addVertex(v5);
    irDAGBuilder.addVertex(v6);
//    irDAGBuilder.addVertex(v7);
    irDAGBuilder.addVertex(v8);

    final IREdge e1 = new IREdge(IREdge.Type.OneToOne, v1, v2);
    e1.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Local);
    e1.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
    e1.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final IREdge e2 = new IREdge(IREdge.Type.OneToOne, v1, v3);
    e2.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Local);
    e2.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
    e2.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final IREdge e3 = new IREdge(IREdge.Type.ScatterGather, v2, v4);
    e3.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e3.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Push);
    e3.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);

    final IREdge e4 = new IREdge(IREdge.Type.ScatterGather, v3, v5);
    e4.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e4.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Push);
    e4.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);

    final IREdge e5 = new IREdge(IREdge.Type.OneToOne, v4, v6);
    e5.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.File);
    e5.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
    e5.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final IREdge e6 = new IREdge(IREdge.Type.OneToOne, v4, v8);
    e6.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.File);
    e6.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
    e6.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final IREdge e7 = new IREdge(IREdge.Type.OneToOne, v7, v5);
    e7.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e7.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Push);
    e7.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final IREdge e8 = new IREdge(IREdge.Type.OneToOne, v5, v8);
    e8.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Local);
    e8.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
    e8.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    // Stage 1 = {v1, v2, v3}
    irDAGBuilder.connectVertices(e1);
    irDAGBuilder.connectVertices(e2);

    // Stage 2 = {v4}
    irDAGBuilder.connectVertices(e3);

    // Stage 3 = {v7}
    // Commented out since SimpleRuntime does not yet support multi-input.
    // TODO #13: Implement Join Node
//    physicalDAGBuilder.createNewStage();
//    physicalDAGBuilder.addVertex(v7);

    // Stage 4 = {v5, v8}
    irDAGBuilder.connectVertices(e4);
    irDAGBuilder.connectVertices(e6);

    // Commented out since SimpleRuntime does not yet support multi-input.
    // TODO #13: Implement Join Node
//    irDAGBuilder.connectVertices(e7);
//    irDAGBuilder.connectVertices(e8);

    // Stage 5 = {v6}
    irDAGBuilder.connectVertices(e5);

    final DAG<IRVertex, IREdge<I, O>> irDAG = irDAGBuilder.build();
    final DAG<RuntimeStage, StageEdge> logicalDAG = irDAG.convert(new LogicalDAGGenerator<>());
    final DAG<PhysicalStage, StageBoundaryEdgeInfo> physicalDAG = logicalDAG.convert(new PhysicalDAGGenerator());

    // Test Logical DAG
    final List<RuntimeStage> sortedLogicalDAG = logicalDAG.getTopologicalSort();
    final RuntimeStage runtimeStage1 = sortedLogicalDAG.get(0);
    final RuntimeStage runtimeStage2 = sortedLogicalDAG.get(1);
    final RuntimeStage runtimeStage3 = sortedLogicalDAG.get(2);
    final RuntimeStage runtimeStage4 = sortedLogicalDAG.get(3);

    assertEquals(logicalDAG.getVertices().size(), 4);
//    assertEquals(logicalDAG.getIncomingEdges(runtimeStage1).size(), 0);
//    assertEquals(logicalDAG.getIncomingEdges(runtimeStage2).size(), 1);
//    assertEquals(logicalDAG.getIncomingEdges(runtimeStage3).size(), 1);
//    assertEquals(logicalDAG.getIncomingEdges(runtimeStage4).size(), 1);
//    assertEquals(logicalDAG.getIncomingEdges(runtimeStage5).size(), 1);
//    assertEquals(logicalDAG.getOutgoingEdges(runtimeStage1).size(), 2);
//    assertEquals(logicalDAG.getOutgoingEdges(runtimeStage2).size(), 0);
//    assertEquals(logicalDAG.getOutgoingEdges(runtimeStage3).size(), 1);
//    assertEquals(logicalDAG.getOutgoingEdges(runtimeStage4).size(), 0);
//    assertEquals(logicalDAG.getOutgoingEdges(runtimeStage5).size(), 0);

    // Test Physical DAG
//    final List<PhysicalStage> sortedPhysicalDAG = physicalDAG.getTopologicalSort();
//    final PhysicalStage physicalStage1 = sortedPhysicalDAG.get(0);
//    final PhysicalStage physicalStage2 = sortedPhysicalDAG.get(1);
//    assertEquals(physicalDAG.getVertices().size(), 2);
//    assertEquals(physicalDAG.getIncomingEdges(physicalStage1).size(), 0);
//    assertEquals(physicalDAG.getIncomingEdges(physicalStage2).size(), 1);
//    assertEquals(physicalDAG.getOutgoingEdges(physicalStage1).size(), 1);
//    assertEquals(physicalDAG.getOutgoingEdges(physicalStage2).size(), 0);
//
//    final List<TaskGroup> taskGroupList1 = physicalStage1.getTaskGroupList();
//    final List<TaskGroup> taskGroupList2 = physicalStage2.getTaskGroupList();
//    assertEquals(taskGroupList1.size(), 3);
//    assertEquals(taskGroupList2.size(), 2);
  }
}
