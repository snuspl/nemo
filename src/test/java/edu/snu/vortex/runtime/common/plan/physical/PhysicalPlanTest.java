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
package edu.snu.vortex.runtime.common.plan.physical;

import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlanBuilder;
import edu.snu.vortex.runtime.master.RuntimeMaster;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link ExecutionPlanBuilder}
 */
public final class PhysicalPlanTest {
  private ExecutionPlanBuilder builder;
  private final RuntimeMaster runtimeMaster = new RuntimeMaster();

  @Before
  public void setUp() {
    builder = new ExecutionPlanBuilder();
  }

  @Test
  public void testSimplePlan() {
    // Tests a simple plan of 2 operators.
    builder.createNewStage();

    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setAttr(Attribute.IntegerKey.Parallelism, 3);
    builder.addVertex(v1);

    builder.createNewStage();
    final IRVertex v2 = new OperatorVertex(t);
    v2.setAttr(Attribute.IntegerKey.Parallelism, 2);
    builder.addVertex(v2);

    final DAGBuilder tempDAGBuilder = new DAGBuilder();
    tempDAGBuilder.addVertex(v1);
    tempDAGBuilder.addVertex(v2);
    final IREdge e = tempDAGBuilder.connectVertices(v1, v2, IREdge.Type.ScatterGather);
    e.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
    builder.connectVertices(e);

    final ExecutionPlan plan = builder.build();

    // Submit the created execution plan to Runtime Master.
    runtimeMaster.execute(plan);
  }

  @Test
  public void testComplexPlan() {
    // Tests a plan of 5 stages.
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v1.setAttr(Attribute.Key.Placement, Attribute.Compute);

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

    final DAGBuilder tempDAGBuilder = new DAGBuilder();
    tempDAGBuilder.addVertex(v1);
    tempDAGBuilder.addVertex(v2);
    tempDAGBuilder.addVertex(v3);
    tempDAGBuilder.addVertex(v4);
    tempDAGBuilder.addVertex(v5);
    tempDAGBuilder.addVertex(v6);
    tempDAGBuilder.addVertex(v7);
    tempDAGBuilder.addVertex(v8);

    final IREdge e1 = tempDAGBuilder.connectVertices(v1, v2, IREdge.Type.OneToOne);
    e1.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Local);
    e1.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
    e1.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final IREdge e2 = tempDAGBuilder.connectVertices(v1, v3, IREdge.Type.OneToOne);
    e2.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Local);
    e2.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
    e2.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final IREdge e3 = tempDAGBuilder.connectVertices(v2, v4, IREdge.Type.ScatterGather);
    e3.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e3.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Push);
    e3.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);

    final IREdge e4 = tempDAGBuilder.connectVertices(v3, v5, IREdge.Type.ScatterGather);
    e4.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e4.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Push);
    e4.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);

    final IREdge e5 = tempDAGBuilder.connectVertices(v4, v6, IREdge.Type.OneToOne);
    e5.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.File);
    e5.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
    e5.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final IREdge e6 = tempDAGBuilder.connectVertices(v4, v8, IREdge.Type.OneToOne);
    e6.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.File);
    e6.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
    e6.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final IREdge e7 = tempDAGBuilder.connectVertices(v7, v5, IREdge.Type.OneToOne);
    e7.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e7.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Push);
    e7.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final IREdge e8 = tempDAGBuilder.connectVertices(v5, v8, IREdge.Type.OneToOne);
    e8.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Local);
    e8.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
    e8.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    // Stage 1 = {v1, v2, v3}
    builder.createNewStage();
    builder.addVertex(v1);
    builder.addVertex(v2);
    builder.addVertex(v3);
    builder.connectVertices(e1);
    builder.connectVertices(e2);

    // Stage 2 = {v4}
    builder.createNewStage();
    builder.addVertex(v4);
    builder.connectVertices(e3);

    // Stage 3 = {v7}
    // Commented out since SimpleRuntime does not yet support multi-input.
    // TODO #13: Implement Join Node
//    builder.createNewStage();
//    builder.addVertex(v7);

    // Stage 4 = {v5, v8}
    builder.createNewStage();
    builder.addVertex(v5);
    builder.addVertex(v8);
    builder.connectVertices(e4);
    builder.connectVertices(e6);

    // Commented out since SimpleRuntime does not yet support multi-input.
    // TODO #13: Implement Join Node
//    builder.connectVertices(e7);
//    builder.connectVertices(e8);

    // Stage 5 = {v6}
    builder.createNewStage();
    builder.addVertex(v6);
    builder.connectVertices(e5);

    final ExecutionPlan plan = builder.build();

    // Submit the created execution plan to Runtime Master.
    runtimeMaster.execute(plan);
  }
}
