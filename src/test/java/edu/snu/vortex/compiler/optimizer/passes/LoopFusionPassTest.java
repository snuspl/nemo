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
package edu.snu.vortex.compiler.optimizer.passes;

import edu.snu.vortex.client.JobLauncher;
import edu.snu.vortex.compiler.TestUtil;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.LoopVertex;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link LoopFusionPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class LoopFusionPassTest {
  private DAG<IRVertex, IREdge> dagToBeFused;

  @Before
  public void setUp() throws Exception {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    final DAG<IRVertex, IREdge> groupedDAG = new LoopGroupingPass().process(TestUtil.compileALSDAG());
    final Optional<LoopVertex> anotherLoop = groupedDAG.getTopologicalSort().stream()
        .filter(irVertex -> irVertex instanceof LoopVertex).map(irVertex -> (LoopVertex) irVertex).findFirst();
    groupedDAG.topologicalDo(v -> {
      builder.addVertex(v, groupedDAG);
      groupedDAG.getIncomingEdgesOf(v).forEach(builder::connectVertices);
    });
    anotherLoop.ifPresent(loopVertex -> {
      final LoopVertex newLoopVertex = loopVertex.getClone();
      builder.addVertex(newLoopVertex);
      groupedDAG.getIncomingEdgesOf(loopVertex).forEach(edge -> {
        final IREdge newIREdge = new IREdge(edge.getType(), edge.getSrc(), newLoopVertex, edge.getCoder());
        builder.connectVertices(newIREdge);
      });
      groupedDAG.getOutgoingEdgesOf(loopVertex).forEach(edge -> {
        final IREdge newIREdge = new IREdge(edge.getType(), newLoopVertex, edge.getDst(), edge.getCoder());
        builder.connectVertices(newIREdge);
      });
    });
    dagToBeFused = builder.build();
  }

  @Test
  public void testLoopFusionPass() throws Exception {
    final DAG<IRVertex, IREdge> processedDAG = new LoopFusionPass().process(dagToBeFused);

    assertEquals(9, processedDAG.getTopologicalSort().size());
  }
}
