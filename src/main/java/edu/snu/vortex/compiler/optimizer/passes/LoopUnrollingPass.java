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

import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;

import java.util.HashSet;
import java.util.Set;

/**
 * Pass for unrolling the loops grouped by the {@link edu.snu.vortex.compiler.ir.LoopVertex}.
 */
public final class LoopUnrollingPass implements Pass {
  private Set<LoopVertex> loopVertices = new HashSet<>();

  public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    unrollRecursively(builder, dag);

    this.loopVertices.forEach(loopVertex -> {
      loopVertex.getIncomingEdges().values().forEach(irEdges -> {
        irEdges.forEach(builder::connectVertices);
      });
      loopVertex.getOutgoingEdges().values().forEach(irEdges -> {
        irEdges.forEach(builder::connectVertices);
      });
    });

    return builder.build();
  }

  private void unrollRecursively(final DAGBuilder<IRVertex, IREdge> builder, final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(irVertex -> {
      if (irVertex instanceof SourceVertex) {
        builder.addVertex(irVertex);
      } else if (irVertex instanceof OperatorVertex) {
        builder.addVertex(irVertex);
        dag.getIncomingEdgesOf(irVertex).forEach(builder::connectVertices);
      } else if (irVertex instanceof LoopVertex) {
        final LoopVertex loopVertex = (LoopVertex) irVertex;
        final DAG<IRVertex, IREdge> loopDAG = loopVertex.getDAG();
        unrollRecursively(builder, loopDAG);
        this.loopVertices.add(loopVertex);
      }
    });
  }
}
