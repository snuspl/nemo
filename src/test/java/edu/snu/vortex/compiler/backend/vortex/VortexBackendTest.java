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
package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.TestUtil;
import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.compiler.frontend.beam.transform.DoTransform;
import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.common.dag.DAGBuilder;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import org.junit.Before;
import org.junit.Test;

import static edu.snu.vortex.common.dag.DAG.EMPTY_DAG_DIRECTORY;
import static org.junit.Assert.assertEquals;

/**
 * Test Vortex Backend.
 */
public final class VortexBackendTest<I, O> {
  private final IRVertex source = new BoundedSourceVertex<>(new TestUtil.EmptyBoundedSource("Source"));
  private final IRVertex map1 = new OperatorVertex(new TestUtil.EmptyTransform("MapElements"));
  private final IRVertex groupByKey = new OperatorVertex(new TestUtil.EmptyTransform("GroupByKey"));
  private final IRVertex combine = new OperatorVertex(new TestUtil.EmptyTransform("Combine"));
  private final IRVertex map2 = new OperatorVertex(new DoTransform(null, null));

  private final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
  private DAG<IRVertex, IREdge> dag;

  @Before
  public void setUp() throws Exception {
    this.dag = builder.addVertex(source).addVertex(map1).addVertex(groupByKey).addVertex(combine).addVertex(map2)
        .connectVertices(new IREdge(IREdge.Type.OneToOne, source, map1, Coder.DUMMY_CODER))
        .connectVertices(new IREdge(IREdge.Type.ScatterGather, map1, groupByKey, Coder.DUMMY_CODER))
        .connectVertices(new IREdge(IREdge.Type.OneToOne, groupByKey, combine, Coder.DUMMY_CODER))
        .connectVertices(new IREdge(IREdge.Type.OneToOne, combine, map2, Coder.DUMMY_CODER))
        .build();

    this.dag = new Optimizer().optimize(dag, Optimizer.PolicyType.Pado, EMPTY_DAG_DIRECTORY);
  }

  /**
   * This method uses an IR DAG and tests whether VortexBackend successfully generates an Execution Plan.
   * @throws Exception during the Execution Plan generation.
   */
  @Test
  public void testExecutionPlanGeneration() throws Exception {
    final Backend<PhysicalPlan> backend = new VortexBackend();
    final PhysicalPlan executionPlan = backend.compile(dag);

    assertEquals(2, executionPlan.getStageDAG().getVertices().size());
    assertEquals(1, executionPlan.getStageDAG().getTopologicalSort().get(0).getTaskGroupList().size());
    assertEquals(1, executionPlan.getStageDAG().getTopologicalSort().get(1).getTaskGroupList().size());
  }
}
