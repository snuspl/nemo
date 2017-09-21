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
package edu.snu.vortex.compiler.optimizer.pass;

import edu.snu.vortex.client.JobLauncher;
import edu.snu.vortex.compiler.CompilerTestUtil;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.attribute.ExecutionFactor;
import edu.snu.vortex.compiler.ir.attribute.edge.DataFlowModel;
import edu.snu.vortex.compiler.ir.attribute.edge.DataStore;
import edu.snu.vortex.compiler.ir.attribute.vertex.ExecutorPlacement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link PadoVertexPass} and {@link PadoEdgePass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class PadoPassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testPadoPass() throws Exception {
    final DAG<IRVertex, IREdge> processedDAG = new PadoEdgePass().apply(new PadoVertexPass().apply(compiledDAG));

    final IRVertex vertex1 = processedDAG.getTopologicalSort().get(0);
    assertEquals(ExecutorPlacement.TRANSIENT, vertex1.getStringAttr(ExecutionFactor.Type.ExecutorPlacement));

    final IRVertex vertex5 = processedDAG.getTopologicalSort().get(1);
    assertEquals(ExecutorPlacement.TRANSIENT, vertex5.getStringAttr(ExecutionFactor.Type.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex5).forEach(irEdge -> {
      assertEquals(DataStore.MEMORY, irEdge.getStringAttr(ExecutionFactor.Type.DataStore));
      assertEquals(DataFlowModel.PULL, irEdge.getStringAttr(ExecutionFactor.Type.DataFlowModel));
    });

    final IRVertex vertex6 = processedDAG.getTopologicalSort().get(2);
    assertEquals(ExecutorPlacement.RESERVED, vertex6.getStringAttr(ExecutionFactor.Type.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex6).forEach(irEdge -> {
      assertEquals(DataStore.LOCAL_FILE, irEdge.getStringAttr(ExecutionFactor.Type.DataStore));
      assertEquals(DataFlowModel.PUSH, irEdge.getStringAttr(ExecutionFactor.Type.DataFlowModel));
    });

    final IRVertex vertex4 = processedDAG.getTopologicalSort().get(6);
    assertEquals(ExecutorPlacement.RESERVED, vertex4.getStringAttr(ExecutionFactor.Type.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex4).forEach(irEdge -> {
      assertEquals(DataStore.MEMORY, irEdge.getStringAttr(ExecutionFactor.Type.DataStore));
      assertEquals(DataFlowModel.PULL, irEdge.getStringAttr(ExecutionFactor.Type.DataFlowModel));
    });

    final IRVertex vertex12 = processedDAG.getTopologicalSort().get(10);
    assertEquals(ExecutorPlacement.RESERVED, vertex12.getStringAttr(ExecutionFactor.Type.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex12).forEach(irEdge -> {
      assertEquals(DataStore.LOCAL_FILE, irEdge.getStringAttr(ExecutionFactor.Type.DataStore));
      assertEquals(DataFlowModel.PULL, irEdge.getStringAttr(ExecutionFactor.Type.DataFlowModel));
    });

    final IRVertex vertex13 = processedDAG.getTopologicalSort().get(11);
    assertEquals(ExecutorPlacement.RESERVED, vertex13.getStringAttr(ExecutionFactor.Type.ExecutorPlacement));
    processedDAG.getIncomingEdgesOf(vertex13).forEach(irEdge -> {
      assertEquals(DataStore.MEMORY, irEdge.getStringAttr(ExecutionFactor.Type.DataStore));
      assertEquals(DataFlowModel.PULL, irEdge.getStringAttr(ExecutionFactor.Type.DataFlowModel));
    });
  }
}
