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

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;
import edu.snu.vortex.compiler.ir.execution_property.edge.DataStore;
import edu.snu.vortex.compiler.ir.execution_property.edge.WriteOptimization;

import java.util.List;

/**
 * Pass which enables I-File style write optimization.
 * It sets IFileWrite execution property on ScatterGather edges with RemoteFile partition store.
 */
public final class IFilePass implements StaticOptimizationPass {
  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge -> {
        if (edge.getType().equals(IREdge.Type.ScatterGather)
            && edge.getStringProperty(ExecutionProperty.Key.DataStore).equals(DataStore.REMOTE_FILE)) {
          edge.setProperty(WriteOptimization.of(WriteOptimization.IFILE_WRITE));
        }
      });
    });
    return dag;
  }
}
