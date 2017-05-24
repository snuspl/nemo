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
package edu.snu.vortex.compiler.optimizer.passes.optimization;

import edu.snu.vortex.client.JobLauncher;
import edu.snu.vortex.compiler.TestUtil;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.LoopVertex;
import edu.snu.vortex.compiler.optimizer.passes.LoopGroupingPass;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link LoopOptimizations.LoopFusionPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class LoopFusionPassTest {
  private DAG<IRVertex, IREdge> originalALSDAG;
  private DAG<IRVertex, IREdge> groupedDAG;
  private DAG<IRVertex, IREdge> dagToBeFused;
  private DAG<IRVertex, IREdge> dagNotToBeFused;

  @Before
  public void setUp() throws Exception {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    final DAGBuilder<IRVertex, IREdge> builder2 = new DAGBuilder<>();

    originalALSDAG = TestUtil.compileALSDAG();
    groupedDAG = new LoopGroupingPass().process(originalALSDAG);

    final Optional<LoopVertex> anotherLoop = groupedDAG.getTopologicalSort().stream()
        .filter(irVertex -> irVertex instanceof LoopVertex).map(irVertex -> (LoopVertex) irVertex).findFirst();
    groupedDAG.topologicalDo(v -> {
      builder.addVertex(v, groupedDAG);
      groupedDAG.getIncomingEdgesOf(v).forEach(builder::connectVertices);

      builder2.addVertex(v, groupedDAG);
      groupedDAG.getIncomingEdgesOf(v).forEach(builder2::connectVertices);
    });
    assertTrue(anotherLoop.isPresent());

    // We're going to put this additional loop to the DAG, to test out the LoopFusion.
    anotherLoop.ifPresent(loopVertex -> {
      final LoopVertex newLoopVertex = loopVertex.getClone();
      builder.addVertex(newLoopVertex);
      newLoopVertex.getIterativeIncomingEdges().values().forEach(irEdges -> irEdges.forEach(edge -> {
        final IREdge newIREdge = new IREdge(edge.getType(), loopVertex, newLoopVertex, edge.getCoder());
        builder.connectVertices(newIREdge);
      }));
      newLoopVertex.getNonIterativeIncomingEdges().values().forEach(irEdges -> irEdges.forEach(edge -> {
        final IREdge newIREdge = new IREdge(edge.getType(), edge.getSrc(), newLoopVertex, edge.getCoder());
        builder.connectVertices(newIREdge);
      }));
    });
    dagToBeFused = builder.build();

    // additional Loop with different condition.
    anotherLoop.ifPresent(loopVertex -> {
      final LoopVertex newLoopVertex = loopVertex.getClone();
      newLoopVertex.setTerminationCondition((i) -> i == 100);
      builder2.addVertex(newLoopVertex);
      newLoopVertex.getIterativeIncomingEdges().values().forEach(irEdges -> irEdges.forEach(edge -> {
        final IREdge newIREdge = new IREdge(edge.getType(), loopVertex, newLoopVertex, edge.getCoder());
        builder2.connectVertices(newIREdge);
      }));
      newLoopVertex.getNonIterativeIncomingEdges().values().forEach(irEdges -> irEdges.forEach(edge -> {
        final IREdge newIREdge = new IREdge(edge.getType(), edge.getSrc(), newLoopVertex, edge.getCoder());
        builder2.connectVertices(newIREdge);
      }));
    });
    dagNotToBeFused = builder2.build();
  }

  @Test
  public void testLoopFusionPass() throws Exception {
    final long numberOfGroupedVertices = groupedDAG.getVertices().size();
    final DAG<IRVertex, IREdge> processedDAG = new LoopOptimizations().getLoopFusionPass().process(dagToBeFused);
    assertEquals(numberOfGroupedVertices, processedDAG.getVertices().size());

    // no loop
    final long numberOfOriginalVertices = originalALSDAG.getVertices().size();
    final DAG<IRVertex, IREdge> processedNoLoopDAG =
        new LoopOptimizations().getLoopFusionPass().process(originalALSDAG);
    assertEquals(numberOfOriginalVertices, processedNoLoopDAG.getVertices().size());

    // one loop
    final DAG<IRVertex, IREdge> processedOneLoopDAG = new LoopOptimizations().getLoopFusionPass().process(groupedDAG);
    assertEquals(numberOfGroupedVertices, processedOneLoopDAG.getVertices().size());

    // not to be fused loops
    final long numberOfNotToBeFusedVertices = dagNotToBeFused.getVertices().size();
    final DAG<IRVertex, IREdge> processedNotToBeFusedDAG =
        new LoopOptimizations().getLoopFusionPass().process(dagNotToBeFused);
    assertEquals(numberOfNotToBeFusedVertices, processedNotToBeFusedDAG.getVertices().size());
  }
}
