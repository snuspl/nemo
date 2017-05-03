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

import java.util.*;

/**
 * Pass for grouping each loops together using the LoopVertex.
 * It first groups loops together, making each iteration into a LoopOperator.
 * Then, it rolls repetitive operators into one root LoopOperator, which is linked with other LoopOperators,
 * each of which represents a single iteration of the loop, in the form of a doubly linked list.
 */
public final class LoopGroupingPass implements Pass {
  public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
    final Integer maxStackDepth = this.findMaxLoopVertexStackDepth(dag);
    return groupLoops(dag, maxStackDepth);
  }

  /**
   * This method finds the maximum loop vertex stack depth of a specific DAG. This is to handle nested loops.
   * @param dag DAG to observe.
   * @return The maximum stack depth of the DAG.
   * @throws Exception exceptions through the way.
   */
  private Integer findMaxLoopVertexStackDepth(final DAG<IRVertex, IREdge> dag) throws Exception {
    final OptionalInt maxDepth = dag.getVertices().stream()
        .filter(irVertex -> irVertex instanceof OperatorVertex)
        .mapToInt(irVertex -> ((OperatorVertex) irVertex).getLoopVertexStackDepth()).max();
    return maxDepth.orElse(0);
  }

  /**
   * This part groups each iteration of loops together by observing the LoopVertexStack of primitive operators,
   * which is assigned by the {@link edu.snu.vortex.compiler.frontend.beam.Visitor}. This shows in which depth of
   * nested loops each of the composite transforms are located in, indicating that it is part of an iterative transform.
   * @param dag DAG to process
   * @param depth the depth of the stack to process. Must be >=0.
   * @return processed DAG.
   * @throws Exception exceptions through the way.
   */
  private DAG<IRVertex, IREdge> groupLoops(final DAG<IRVertex, IREdge> dag, final Integer depth) throws Exception {
    if (depth <= 0) {
      return dag;
    } else {
      final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
      for (IRVertex irVertex : dag.getTopologicalSort()) {
        if (irVertex instanceof SourceVertex) { // Source vertex: no incoming edges
          builder.addVertex(irVertex);

        } else if (irVertex instanceof OperatorVertex) { // Operator vertex
          final OperatorVertex operatorVertex = (OperatorVertex) irVertex;
          // If this is Composite && depth is appropriate. == If this belongs to a loop.
          if (operatorVertex.isComposite() && operatorVertex.getLoopVertexStackDepth().equals(depth)) {
            final LoopVertex assignedLoopVertex = operatorVertex.getAssignedLoopVertex();
            connectElementToLoop(dag, builder, operatorVertex, assignedLoopVertex); // something -> loop
          } else { // Otherwise: it is not composite || depth inappropriate. == If this is just an operator.
            builder.addVertex(operatorVertex);
            dag.getIncomingEdgesOf(operatorVertex).forEach(irEdge -> {
              if (isEdgeSourceLoop(irEdge)) {
                // connecting with a loop: loop -> operator.
                final LoopVertex srcLoopVertex = getEdgeSourceLoop(irEdge);
                srcLoopVertex.addDagOutgoingEdge(irEdge);
                final IREdge edgeFromLoop = new IREdge(irEdge.getType(), srcLoopVertex, irEdge.getDst());
                builder.connectVertices(edgeFromLoop);
              } else { // connecting outside the composite loop: operator -> operator.
                builder.connectVertices(irEdge);
              }
            });
          }

        } else if (irVertex instanceof LoopVertex) { // Loop vertices of higher depth (nested loops).
          final LoopVertex loopVertex = (LoopVertex) irVertex;
          if (loopVertex.getAssignedLoopVertex() != null) { // the loopVertex belongs to another loop.
            final LoopVertex assignedLoopVertex = loopVertex.getAssignedLoopVertex();
            connectElementToLoop(dag, builder, loopVertex, assignedLoopVertex); // something -> loop
          } else { // it cannot be just at the operator level, as it had more depth.
            throw new UnsupportedOperationException("This loop (" + loopVertex + ") shouldn't be of this depth");
          }

        } else {
          throw new UnsupportedOperationException("Unknown vertex type: " + irVertex);
        }
      }

      // Recursive calls for lower depths.
      return groupLoops(loopRolling(builder.build()), depth - 1);
    }
  }

  /**
   * Method for connecting an element to a loop. That is, loop -> loop OR operator -> loop.
   * @param dag to observe the inEdges from.
   * @param builder to add the new edge to.
   * @param dstVertex the destination vertex that belongs to a certain loop.
   * @param assignedLoopVertex the loop that dstVertex belongs to.
   */
  private static void connectElementToLoop(final DAG<IRVertex, IREdge> dag, final DAGBuilder<IRVertex, IREdge> builder,
                                           final IRVertex dstVertex, final LoopVertex assignedLoopVertex) {
    builder.addVertex(assignedLoopVertex);
    assignedLoopVertex.getBuilder().addVertex(dstVertex);

    dag.getIncomingEdgesOf(dstVertex).forEach(irEdge -> {
      if (isEdgeSourceLoop(irEdge)) {
        final LoopVertex srcLoopVertex = getEdgeSourceLoop(irEdge);
        if (srcLoopVertex.equals(assignedLoopVertex)) { // connecting within the composite loop DAG.
          assignedLoopVertex.getBuilder().connectVertices(irEdge);
        } else { // loop -> loop connection
          assignedLoopVertex.addDagIncomingEdge(irEdge);
          final IREdge edgeToLoop = new IREdge(irEdge.getType(), srcLoopVertex, assignedLoopVertex);
          builder.connectVertices(edgeToLoop);
        }
      } else { // operator -> loop
        assignedLoopVertex.addDagIncomingEdge(irEdge);
        final IREdge edgeToLoop = new IREdge(irEdge.getType(), irEdge.getSrc(), assignedLoopVertex);
        builder.connectVertices(edgeToLoop);
      }
    });
  }

  /**
   * This static method distinguishes if the source of the edge is inside a loop.
   * @param irEdge edge to observe.
   * @return whether or not the source of the edge is inside a loop.
   */
  private static boolean isEdgeSourceLoop(final IREdge irEdge) {
    return (irEdge.getSrc() instanceof OperatorVertex && ((OperatorVertex) irEdge.getSrc()).isComposite()) ||
        (irEdge.getSrc() instanceof LoopVertex && ((LoopVertex) irEdge.getSrc()).getAssignedLoopVertex() != null);
  }

  /**
   * This static method gets the loop of the source of the edge.
   * @param irEdge edge to retrieve the loop of the source from.
   * @return loop of the source of the edge.
   */
  private static LoopVertex getEdgeSourceLoop(final IREdge irEdge) {
    if (irEdge.getSrc() instanceof OperatorVertex) {
      return ((OperatorVertex) irEdge.getSrc()).getAssignedLoopVertex();
    } else if (irEdge.getSrc() instanceof LoopVertex) {
      return ((LoopVertex) irEdge.getSrc()).getAssignedLoopVertex();
    } else {
      throw new UnsupportedOperationException("Edge source not a LoopVertex or OpeatorVertex: " + irEdge);
    }
  }

  /**
   * This part rolls the repetitive LoopVertices into a single one, leaving only the root LoopVertex.
   * Following iterations can be generated with the information included in the LoopVertex.
   * @param dag DAG to process.
   * @return Processed DAG.
   * @throws Exception exceptions through the way.
   */
  private DAG<IRVertex, IREdge> loopRolling(final DAG<IRVertex, IREdge> dag) throws Exception {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    final HashMap<LoopVertex, List<LoopVertex>> loopVerticesOfSameLoop = new HashMap<>();
    final HashMap<LoopVertex, HashMap<IRVertex, List<IRVertex>>> equivalentVerticesOfLoops = new HashMap<>();
    LoopVertex rootLoopVertex = null;

    // observe the DAG in a topological order.
    for (IRVertex irVertex : dag.getTopologicalSort()) {
      if (irVertex instanceof SourceVertex) { // source vertex
        builder.addVertex(irVertex);

      } else if (irVertex instanceof OperatorVertex) { // operator vertex
        addToBuilder(builder, irVertex, dag, loopVerticesOfSameLoop);

      } else if (irVertex instanceof LoopVertex) { // loop vertex: we roll them if it is not root
        final LoopVertex loopVertex = (LoopVertex) irVertex;

        if (rootLoopVertex == null || !loopVertex.getName().contains(rootLoopVertex.getName())) { // initial root loop
          rootLoopVertex = loopVertex;
          loopVerticesOfSameLoop.putIfAbsent(rootLoopVertex, new ArrayList<>());
          loopVerticesOfSameLoop.get(rootLoopVertex).add(rootLoopVertex);
          equivalentVerticesOfLoops.putIfAbsent(rootLoopVertex, new HashMap<>());
          for (IRVertex vertex : rootLoopVertex.getDAG().getTopologicalSort()) {
            equivalentVerticesOfLoops.get(rootLoopVertex).putIfAbsent(vertex, new ArrayList<>());
            equivalentVerticesOfLoops.get(rootLoopVertex).get(vertex).add(vertex);
          }
          addToBuilder(builder, loopVertex, dag, loopVerticesOfSameLoop);
        } else { // following loops
          final LoopVertex finalRootLoopVertex = rootLoopVertex;
          finalRootLoopVertex.increaseMaxNumberOfIterations();

          // Add the loop to the list
          loopVerticesOfSameLoop.get(finalRootLoopVertex).add(loopVertex);

          // Zip current vertices together. We assume getTolopogicalSort() brings consistent results..
          final HashMap<IRVertex, List<IRVertex>> equivalentVertices =
              equivalentVerticesOfLoops.get(finalRootLoopVertex);
          final Iterator<IRVertex> rootVertices = finalRootLoopVertex.getDAG().getTopologicalSort().iterator();
          final Iterator<IRVertex> vertices = loopVertex.getDAG().getTopologicalSort().iterator();
          while (rootVertices.hasNext() && vertices.hasNext()) {
            equivalentVertices.get(rootVertices.next()).add(vertices.next());
          }

          // reset non iterative incoming edges.
          finalRootLoopVertex.getNonIterativeIncomingEdges().clear();

          // incoming edges to the DAG.
          loopVertex.getDagIncomingEdges().forEach((dstVertex, edges) -> edges.forEach(edge -> {
            final IRVertex srcVertex = edge.getSrc();
            final IRVertex equivalentDstRootVertex = equivalentVertices.values().stream()
                .filter(list -> list.contains(dstVertex)).map(list -> list.get(0)).findFirst()
                .orElseThrow(() -> new RuntimeException("dstVertex should have been added while zipping"));

            if (equivalentVertices.values().stream().anyMatch(list -> list.contains(srcVertex))) {
              // src is from the previous loop. vertex in previous loop -> DAG.
              final IRVertex equivalentSrcRootVertex = equivalentVertices.values().stream()
                  .filter(list -> list.contains(srcVertex)).map(list -> list.get(0)).findFirst()
                  .orElseThrow(() -> new RuntimeException("anyMatch didn't work correctly"));

              if (finalRootLoopVertex.getIterativeIncomingEdges().values()
                  .stream().allMatch(irEdges -> irEdges.stream().allMatch(e ->
                      !e.getSrc().equals(equivalentSrcRootVertex) || !e.getDst().equals(equivalentDstRootVertex)))) {
                // doesn't already exist in rootVertex iterative incoming edge list.

                // add the new IREdge to the iterative incoming edges list.
                final IREdge newIrEdge = new IREdge(edge.getType(), equivalentSrcRootVertex, equivalentDstRootVertex);
                finalRootLoopVertex.addIterativeIncomingEdge(newIrEdge);

                // remove them from rootLoopVertex's DAG outgoing edges and replace them.
                finalRootLoopVertex.getDagOutgoingEdges().values().stream().filter(irEdges -> irEdges.contains(edge))
                    .forEach(irEdges -> {
                      irEdges.remove(edge);
                      irEdges.add(newIrEdge);
                    });
              }

              // remove overlapping DAG outgoing edges.
              finalRootLoopVertex.getDagOutgoingEdges().values().stream().forEach(irEdges ->
                  irEdges.removeIf(e -> e.getSrc().equals(equivalentSrcRootVertex) && e.getDst().equals(dstVertex)));
            } else {
              // src is from outside the previous loop. vertex outside previous loop -> DAG.
              final IREdge newIrEdge = new IREdge(edge.getType(), srcVertex, equivalentDstRootVertex);
              finalRootLoopVertex.addNonIterativeIncomingEdge(newIrEdge);
            }
          }));

          // outgoing edges from the DAG
          loopVertex.getDagOutgoingEdges().forEach((srcVertex, edges) -> edges.forEach(edge -> {
            final IRVertex dstVertex = edge.getDst();
            final IRVertex equivalentSrcRootVertex = equivalentVertices.values().stream()
                .filter(list -> list.contains(srcVertex)).map(list -> list.get(0)).findFirst()
                .orElseThrow(() -> new RuntimeException("srcVertex should have been added while zipping"));

            if (finalRootLoopVertex.getDagOutgoingEdges().values().stream()
                .allMatch(irEdges -> irEdges.stream().allMatch(e ->
                    !e.getSrc().equals(equivalentSrcRootVertex) || !e.getDst().equals(dstVertex)))) {
              // doesn't already exist in rootVertex DAG outgoing edges
              finalRootLoopVertex.addDagOutgoingEdge(new IREdge(edge.getType(), equivalentSrcRootVertex, dstVertex));
            }
          }));
        }

      } else {
        throw new UnsupportedOperationException("Unknown vertex type: " + irVertex);
      }
    }

    return builder.build();
  }

  /**
   * Adds the vertex and the incoming edges of the vertex to the builder.
   * @param builder Builder that it adds to.
   * @param irVertex Vertex to add.
   * @param dag DAG to observe the incoming edges of the vertex.
   * @param loopVerticesOfSameLoop List that keeps track of the iterations of the identical loop.
   */
  private static void addToBuilder(final DAGBuilder<IRVertex, IREdge> builder, final IRVertex irVertex,
                                   final DAG<IRVertex, IREdge> dag,
                                   final Map<LoopVertex, List<LoopVertex>> loopVerticesOfSameLoop) {
    builder.addVertex(irVertex);
    dag.getIncomingEdgesOf(irVertex).forEach(edge -> {

      // find first LoopVertex of the loop, if it exists. Otherwise just use the src.
      final IRVertex firstEquivalentVertex;
      if (edge.getSrc() instanceof LoopVertex) {
        final Optional<LoopVertex> equivalentVertexCandidate = loopVerticesOfSameLoop.values().stream()
            .filter(list -> list.contains(edge.getSrc())).map(list -> list.get(0)).findFirst();
        if (equivalentVertexCandidate.isPresent()) {
          firstEquivalentVertex = equivalentVertexCandidate.get();
        } else {
          firstEquivalentVertex = edge.getSrc();
        }
      } else {
        firstEquivalentVertex = edge.getSrc();
      }

      if (edge.getSrc().equals(firstEquivalentVertex)) {
        builder.connectVertices(edge);
      } else {
        builder.connectVertices(new IREdge(edge.getType(), firstEquivalentVertex, irVertex));
      }
    });
  }
}
