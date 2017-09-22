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

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.execution_property.edge.DataFlowModel;
import edu.snu.vortex.compiler.ir.execution_property.edge.DataStore;
import edu.snu.vortex.compiler.ir.execution_property.vertex.ExecutorPlacement;
import edu.snu.vortex.runtime.executor.data.GlusterFileStore;
import edu.snu.vortex.runtime.executor.data.MemoryStore;

import java.util.List;

/**
 * Disaggregated Resources pass for tagging vertices.
 */
public final class DisaggregationPass implements StaticOptimizationPass {
  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(vertex -> {
      vertex.setProperty(ExecutorPlacement.of(ExecutorPlacement.COMPUTE));
    });

    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (!inEdges.isEmpty()) {
        inEdges.forEach(edge -> {
          if (edge.getType().equals(IREdge.Type.OneToOne)) {
            edge.setProperty(DataStore.of(MemoryStore.class));
            edge.setProperty(DataFlowModel.of(DataFlowModel.Value.Pull));
          } else {
            edge.setProperty(DataStore.of(GlusterFileStore.class));
            edge.setProperty(DataFlowModel.of(DataFlowModel.Value.Pull));
          }
        });
      }
    });
    return dag;
  }
}
