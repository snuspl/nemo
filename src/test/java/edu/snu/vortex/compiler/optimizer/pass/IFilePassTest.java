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
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.CompilerTestUtil;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;
import edu.snu.vortex.compiler.ir.execution_property.edge.WriteOptimizationProperty;
import edu.snu.vortex.runtime.executor.data.GlusterFileStore;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.ScatterGather;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertTrue;

/**
 * Test {@link IFilePass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class IFilePassTest {
  private DAG<IRVertex, IREdge> compiledDAG;

  @Before
  public void setUp() throws Exception {
    compiledDAG = CompilerTestUtil.compileALSDAG();
  }

  @Test
  public void testIFileWrite() throws Exception {
    final DAG<IRVertex, IREdge> disaggProcessedDAG = new DisaggregationPass().apply(compiledDAG);
    final DAG<IRVertex, IREdge> processedDAG = new IFilePass().apply(disaggProcessedDAG);

    processedDAG.getVertices().forEach(v -> processedDAG.getIncomingEdgesOf(v).stream()
        .filter(e -> e.getClassProperty(ExecutionProperty.Key.DataCommunicationPattern)
            .equals(ScatterGather.class))
        .filter(e -> e.getClassProperty(ExecutionProperty.Key.DataStore).equals(GlusterFileStore.class))
        .forEach(e -> assertTrue(e.get(ExecutionProperty.Key.WriteOptimization) != null
            && e.get(ExecutionProperty.Key.WriteOptimization).equals(WriteOptimizationProperty.IFILE_WRITE))));

    processedDAG.getVertices().forEach(v -> processedDAG.getIncomingEdgesOf(v).stream()
        .filter(e -> !e.getClassProperty(ExecutionProperty.Key.DataCommunicationPattern)
            .equals(ScatterGather.class))
        .filter(e -> e.getClassProperty(ExecutionProperty.Key.DataStore).equals(GlusterFileStore.class))
        .forEach(e -> assertTrue(e.get(ExecutionProperty.Key.WriteOptimization) == null)));
  }
}
