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
 * Pass for unrolling the loops grouped by the {@link LoopGroupingPass}.
 * It first unrolls the root LoopVertex, which is in the form of linked list, into a straight line in the DAG.
 * Then, it decomposes each of the LoopVertices with the DAG information that each of them contain.
 */
public final class LoopUnrollingPass implements Pass {
  private Set<LoopVertex> loopVertices = new HashSet<>();

  public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // We decompose LoopVertices of the DAG that had been unrolled by the loopUnrolling method.
    decomposeLoopVertices(builder, loopUnrolling(dag));

    // We make sure of the remaining incoming/outgoing edges of the DAG itself, in the LoopVertices, are connected.
    this.loopVertices.forEach(loopVertex -> {
      loopVertex.getDagIncomingEdges().values().forEach(irEdges -> {
        irEdges.forEach(builder::connectVertices);
      });
      loopVertex.getDagOutgoingEdges().values().forEach(irEdges -> {
        irEdges.forEach(builder::connectVertices);
      });
    });

    return builder.build();
  }

  /**
   * This method unrolls the root LoopVertex, in the form of linked list, into a line of Loop Vertices in the DAG.
   * @param dag DAG to process.
   * @return Processed DAG.
   * @throws Exception exceptions through the way.
   */
  private DAG<IRVertex, IREdge> loopUnrolling(final DAG<IRVertex, IREdge> dag) throws Exception {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    final Set<LoopVertex> followingLoopVertices = new HashSet<>();

    dag.topologicalDo(irVertex -> {
      if (irVertex instanceof SourceVertex) {
        builder.addVertex(irVertex);
      } else if (irVertex instanceof OperatorVertex) {
        builder.addVertex(irVertex);
        dag.getIncomingEdgesOf(irVertex).forEach(builder::connectVertices);
      } else if (irVertex instanceof LoopVertex) {
        LoopVertex loopVertex = (LoopVertex) irVertex;
        builder.addVertex(loopVertex);
        dag.getIncomingEdgesOf(irVertex).forEach(builder::connectVertices);

        while (loopVertex.hasNext()) {
          loopVertex = loopVertex.getNextLoopVertex();
          followingLoopVertices.add(loopVertex);
          builder.addVertex(loopVertex);
          loopVertex.getVertexIncomingEdges().forEach(builder::connectVertices);
        }
      } else {
        throw new UnsupportedOperationException("Unknown vertex type: " + irVertex);
      }

      followingLoopVertices.forEach(vertex -> vertex.getVertexOutgoingEdges().forEach(builder::connectVertices));
    });

    return builder.build();
  }

  /**
   * It decomposes each of the LoopVertices with the DAG info that each of they contain, in a recursive manner.
   * @param builder Builder to add the vertices to.
   * @param dag DAG to add the vetices from.
   */
  private void decomposeLoopVertices(final DAGBuilder<IRVertex, IREdge> builder, final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(irVertex -> {
      if (irVertex instanceof SourceVertex) {
        builder.addVertex(irVertex);
      } else if (irVertex instanceof OperatorVertex) {
        builder.addVertex(irVertex);
        dag.getIncomingEdgesOf(irVertex).forEach(builder::connectVertices);
      } else if (irVertex instanceof LoopVertex) {
        final LoopVertex loopVertex = (LoopVertex) irVertex;
        final DAG<IRVertex, IREdge> loopDAG = loopVertex.getDAG();
        decomposeLoopVertices(builder, loopDAG);
        this.loopVertices.add(loopVertex);
      } else {
        throw new UnsupportedOperationException("Unknown vertex type: " + irVertex);
      }
    });
  }
}
