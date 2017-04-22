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

/**
 * Pass for grouping each loops together using the LoopVertex.
 */
public final class LoopGroupingPass implements Pass {
  public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    LoopVertex currentLoopVertex = null;

    for (IRVertex irVertex : dag.getTopologicalSort()) {
      if (irVertex instanceof SourceVertex) { // Source vertex: no incoming edges
        builder.addVertex(irVertex);
      } else if (irVertex instanceof OperatorVertex) { // Operator vertex
        final OperatorVertex operatorVertex = (OperatorVertex) irVertex;
        // If this is Composite.
        if (operatorVertex.isComposite()) {
          final LoopVertex assignedLoopVertex = operatorVertex.getAssignedLoopVertex();
          final LoopVertex finalCurrentLoopVertex = currentLoopVertex;

          builder.addVertex(assignedLoopVertex);
          assignedLoopVertex.getBuilder().addVertex(operatorVertex);

          dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
            if (assignedLoopVertex.getBuilder().contains(irEdge.getSrc())) { // inside the composite loop
              assignedLoopVertex.getBuilder().connectVertices(irEdge);
            } else { // connecting with outside the composite loop
              assignedLoopVertex.addIncomingEdge(irEdge);
              final IREdge edgeToLoop =
                  finalCurrentLoopVertex != null && finalCurrentLoopVertex.getBuilder().contains(irEdge.getSrc()) ?
                      new IREdge(irEdge.getType(), finalCurrentLoopVertex, assignedLoopVertex) : // loop to loop
                      irEdge.getSrc() instanceof OperatorVertex && ((OperatorVertex) irEdge.getSrc()).isComposite() ?
                          new IREdge(irEdge.getType(),
                              ((OperatorVertex) irEdge.getSrc()).getAssignedLoopVertex(), assignedLoopVertex) :
                          new IREdge(irEdge.getType(), irEdge.getSrc(), assignedLoopVertex); // otherwise
              builder.connectVertices(edgeToLoop);
            }
          });

          if (!assignedLoopVertex.equals(currentLoopVertex)) { // it is a new loop.
            // update current loop vertex.
            currentLoopVertex = assignedLoopVertex;
          }
        } else { // Otherwise: it is not composite.
          final LoopVertex finalCurrentLoopVertex = currentLoopVertex;
          builder.addVertex(irVertex);
          dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
            if (finalCurrentLoopVertex != null && finalCurrentLoopVertex.getBuilder().contains(irEdge.getSrc())) {
              // connecting with the loop.
              finalCurrentLoopVertex.addOutgoingEdge(irEdge);
              final IREdge edgeFromLoop = new IREdge(irEdge.getType(), finalCurrentLoopVertex, irEdge.getDst());
              builder.connectVertices(edgeFromLoop);
            } else { // connecting outside the composite loop.
              builder.connectVertices(irEdge);
            }
          });
        }
      } else {
        throw new UnsupportedOperationException("Loop Vertex generated from Visitor: weird case");
      }
    }

    return builder.build();
  }
}
