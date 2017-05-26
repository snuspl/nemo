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
  private DAG<IRVertex, IREdge> dagToBePartiallyFused;
  private DAG<IRVertex, IREdge> dagToBePartiallyFused2;

  @Before
  public void setUp() throws Exception {
    final DAGBuilder<IRVertex, IREdge> dagToBeFusedBuilder = new DAGBuilder<>();
    final DAGBuilder<IRVertex, IREdge> dagNotToBeFusedBuilder = new DAGBuilder<>();

    originalALSDAG = TestUtil.compileALSDAG();
    groupedDAG = new LoopGroupingPass().process(originalALSDAG);

    groupedDAG.topologicalDo(v -> {
      dagToBeFusedBuilder.addVertex(v, groupedDAG);
      groupedDAG.getIncomingEdgesOf(v).forEach(dagToBeFusedBuilder::connectVertices);

      dagNotToBeFusedBuilder.addVertex(v, groupedDAG);
      groupedDAG.getIncomingEdgesOf(v).forEach(dagNotToBeFusedBuilder::connectVertices);
    });
    final Optional<LoopVertex> loopInDAG = groupedDAG.getTopologicalSort().stream()
        .filter(irVertex -> irVertex instanceof LoopVertex).map(irVertex -> (LoopVertex) irVertex).findFirst();
    assertTrue(loopInDAG.isPresent());

    // We're going to put this additional loop to the DAG, to test out the LoopFusion.
    final LoopVertex newLoop = loopInDAG.get().getClone();
    addLoopVertexToBuilder(dagToBeFusedBuilder, loopInDAG.get(), newLoop);
    dagToBeFused = dagToBeFusedBuilder.build();

    // additional Loop with different condition.
    final LoopVertex newLoopWithDiffCondition = loopInDAG.get().getClone();
    newLoopWithDiffCondition.setTerminationCondition((i) -> i < 100);
    addLoopVertexToBuilder(dagNotToBeFusedBuilder, loopInDAG.get(), newLoopWithDiffCondition);
    dagNotToBeFused = dagNotToBeFusedBuilder.build();

    // partially fused: two and one.
    addLoopVertexToBuilder(dagToBeFusedBuilder, newLoop, newLoopWithDiffCondition);
    dagToBePartiallyFused = dagToBeFusedBuilder.build();

    // partially fused2: two and two.
    final LoopVertex newLoopWithDiffDiffCondition = newLoopWithDiffCondition.getClone();
    newLoopWithDiffDiffCondition.setTerminationCondition((i) -> i < 100);
    addLoopVertexToBuilder(dagToBeFusedBuilder, newLoopWithDiffCondition, newLoopWithDiffDiffCondition);
    dagToBePartiallyFused2 = dagToBeFusedBuilder.build();
  }

  private static void addLoopVertexToBuilder(final DAGBuilder<IRVertex, IREdge> builder,
                                             final LoopVertex loopVertexToBeFollowed,
                                             final LoopVertex loopVertexToFollow) {
    builder.addVertex(loopVertexToFollow);
    loopVertexToFollow.getIterativeIncomingEdges().values().forEach(irEdges -> irEdges.forEach(irEdge -> {
      final IREdge newIREdge =
          new IREdge(irEdge.getType(), loopVertexToBeFollowed, loopVertexToFollow, irEdge.getCoder());
      builder.connectVertices(newIREdge);
    }));
    loopVertexToFollow.getNonIterativeIncomingEdges().values().forEach(irEdges -> irEdges.forEach(irEdge -> {
      final IREdge newIREdge = new IREdge(irEdge.getType(), irEdge.getSrc(), loopVertexToFollow, irEdge.getCoder());
      builder.connectVertices(newIREdge);
    }));
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

    // to be partially fused loops: two and one
    final DAG<IRVertex, IREdge> processedToBePartiallyFusedDAG =
        new LoopOptimizations().getLoopFusionPass().process(dagToBePartiallyFused);
    assertEquals(numberOfNotToBeFusedVertices, processedToBePartiallyFusedDAG.getVertices().size());

    // to be partially fused loops: two and two
    final DAG<IRVertex, IREdge> processedToBePartiallyFusedDAG2 =
        new LoopOptimizations().getLoopFusionPass().process(dagToBePartiallyFused2);
    assertEquals(numberOfNotToBeFusedVertices, processedToBePartiallyFusedDAG2.getVertices().size());
  }
}
