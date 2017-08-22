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

import edu.snu.vortex.compiler.CompilerTestUtil;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.common.dag.DAG;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link PadoVertexPass} and {@link PadoEdgePass}.
 */
public class PadoPassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testPadoPass() throws Exception {
    final DAG<IRVertex, IREdge> processedDAG = new PadoEdgePass().process(new PadoVertexPass().process(compiledDAG));

    final IRVertex vertex1 = processedDAG.getTopologicalSort().get(0);
    assertEquals(Attribute.Transient, vertex1.getAttr(Attribute.Key.Placement));

    final IRVertex vertex5 = processedDAG.getTopologicalSort().get(1);
    assertEquals(Attribute.Transient, vertex5.getAttr(Attribute.Key.Placement));
    processedDAG.getIncomingEdgesOf(vertex5).forEach(irEdge -> {
      assertEquals(Attribute.Memory, irEdge.getAttr(Attribute.Key.ChannelDataPlacement));
      assertEquals(Attribute.Pull, irEdge.getAttr(Attribute.Key.ChannelTransferPolicy));
    });

    final IRVertex vertex6 = processedDAG.getTopologicalSort().get(2);
    assertEquals(Attribute.Reserved, vertex6.getAttr(Attribute.Key.Placement));
    processedDAG.getIncomingEdgesOf(vertex6).forEach(irEdge -> {
      assertEquals(Attribute.LocalFile, irEdge.getAttr(Attribute.Key.ChannelDataPlacement));
      assertEquals(Attribute.Push, irEdge.getAttr(Attribute.Key.ChannelTransferPolicy));
    });

    final IRVertex vertex4 = processedDAG.getTopologicalSort().get(6);
    assertEquals(Attribute.Reserved, vertex4.getAttr(Attribute.Key.Placement));
    processedDAG.getIncomingEdgesOf(vertex4).forEach(irEdge -> {
      assertEquals(Attribute.Memory, irEdge.getAttr(Attribute.Key.ChannelDataPlacement));
      assertEquals(Attribute.Pull, irEdge.getAttr(Attribute.Key.ChannelTransferPolicy));
    });

    final IRVertex vertex12 = processedDAG.getTopologicalSort().get(10);
    assertEquals(Attribute.Reserved, vertex12.getAttr(Attribute.Key.Placement));
    processedDAG.getIncomingEdgesOf(vertex12).forEach(irEdge -> {
      assertEquals(Attribute.LocalFile, irEdge.getAttr(Attribute.Key.ChannelDataPlacement));
      assertEquals(Attribute.Pull, irEdge.getAttr(Attribute.Key.ChannelTransferPolicy));
    });

    final IRVertex vertex13 = processedDAG.getTopologicalSort().get(11);
    assertEquals(Attribute.Reserved, vertex13.getAttr(Attribute.Key.Placement));
    processedDAG.getIncomingEdgesOf(vertex13).forEach(irEdge -> {
      assertEquals(Attribute.Memory, irEdge.getAttr(Attribute.Key.ChannelDataPlacement));
      assertEquals(Attribute.Pull, irEdge.getAttr(Attribute.Key.ChannelTransferPolicy));
    });
  }
}
