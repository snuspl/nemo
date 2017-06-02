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
package edu.snu.vortex.compiler.optimizer.passes.optimization;

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.Transform;
import edu.snu.vortex.compiler.optimizer.passes.Pass;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Pass for Common Subexpression Elimination optimization.
 */
public final class CommonSubexpressionEliminationPass implements Pass {
  @Override
  public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
    // find and collect vertices with equivalent transforms
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    final Map<Transform, List<OperatorVertex>> operatorVerticesToBeMerged = new HashMap<>();
    final Map<OperatorVertex, List<IREdge>> inEdges = new HashMap<>();
    final Map<OperatorVertex, List<IREdge>> outEdges = new HashMap<>();

    dag.topologicalDo(irVertex -> {
      if (irVertex instanceof OperatorVertex) {
        final OperatorVertex operatorVertex = (OperatorVertex) irVertex;
        operatorVerticesToBeMerged.putIfAbsent(operatorVertex.getTransform(), new ArrayList<>());
        operatorVerticesToBeMerged.get(operatorVertex.getTransform()).add(operatorVertex);

        dag.getIncomingEdgesOf(operatorVertex).forEach(irEdge -> {
          inEdges.putIfAbsent(operatorVertex, new ArrayList<>());
          inEdges.get(operatorVertex).add(irEdge);
          if (irEdge.getSrc() instanceof OperatorVertex) {
            final OperatorVertex source = (OperatorVertex) irEdge.getSrc();
            outEdges.putIfAbsent(source, new ArrayList<>());
            outEdges.get(source).add(irEdge);
          }
        });
      } else {
        builder.addVertex(irVertex, dag);
        dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          if (irEdge.getSrc() instanceof OperatorVertex) {
            final OperatorVertex source = (OperatorVertex) irEdge.getSrc();
            outEdges.putIfAbsent(source, new ArrayList<>());
            outEdges.get(source).add(irEdge);
          } else {
            builder.connectVertices(irEdge);
          }
        });
      }
    });

    // merge them if they are not dependent on each other, and add IRVertices to the builder.
    operatorVerticesToBeMerged.forEach(((transform, operatorVertices) -> {
      final Map<List<IRVertex>, List<OperatorVertex>> verticesToBeMergedWithIdenticalSources = new HashMap<>();

      operatorVertices.forEach(operatorVertex -> {
        final List<IRVertex> incomingSources = dag.getIncomingEdgesOf(operatorVertex).stream().map(IREdge::getSrc)
            .collect(Collectors.toList());
        if (verticesToBeMergedWithIdenticalSources.keySet().stream().anyMatch(lst ->
            lst.containsAll(incomingSources) && incomingSources.containsAll(lst))) {
          final List<IRVertex> foundKey = verticesToBeMergedWithIdenticalSources.keySet().stream().filter(vs ->
              vs.containsAll(incomingSources) && incomingSources.containsAll(vs)).findFirst().get();
          verticesToBeMergedWithIdenticalSources.get(foundKey).add(operatorVertex);
        } else {
          verticesToBeMergedWithIdenticalSources.putIfAbsent(incomingSources, new ArrayList<>());
          verticesToBeMergedWithIdenticalSources.get(incomingSources).add(operatorVertex);
        }
      });
      verticesToBeMergedWithIdenticalSources.values().forEach(ovs ->
          mergeAndAddToBuilder(ovs, builder, dag, inEdges, outEdges));
    }));

    // process IREdges
    operatorVerticesToBeMerged.values().forEach(operatorVertices -> {
      operatorVertices.forEach(operatorVertex -> {
        inEdges.getOrDefault(operatorVertex, new ArrayList<>()).forEach(builder::connectVertices);
        outEdges.getOrDefault(operatorVertex, new ArrayList<>()).forEach(builder::connectVertices);
      });
    });

    return dag;
  }

  /**
   * merge equivalent operator vertices and add them to the provided builder.
   * @param ovs operator vertices that are to be merged (if there are no dependencies between them).
   * @param builder builder to add the merged vertices to.
   * @param dag dag to observe while adding them.
   * @param inEdges incoming edges information.
   * @param outEdges outgoing edges information.
   */
  private static void mergeAndAddToBuilder(final List<OperatorVertex> ovs, final DAGBuilder<IRVertex, IREdge> builder,
                                           final DAG<IRVertex, IREdge> dag,
                                           final Map<OperatorVertex, List<IREdge>> inEdges,
                                           final Map<OperatorVertex, List<IREdge>> outEdges) {
    if (ovs.size() > 0) {
      final OperatorVertex operatorVertexToUse = ovs.get(0);
      final List<OperatorVertex> dependencyFailedOperatorVertices = new ArrayList<>();

      builder.addVertex(operatorVertexToUse);
      ovs.forEach(ov -> {
        if (dag.pathExistsBetween(operatorVertexToUse, ov)) {
          dependencyFailedOperatorVertices.add(ov);
        } else {
          if (!ov.equals(operatorVertexToUse)) {
            final List<IREdge> inListToModify = inEdges.getOrDefault(ov, new ArrayList<>());
            inEdges.getOrDefault(ov, new ArrayList<>()).forEach(e -> {
              inListToModify.remove(e);
              final IREdge newIrEdge = new IREdge(e.getType(), e.getSrc(), operatorVertexToUse, e.getCoder());
              inListToModify.add(newIrEdge);
            });
            inEdges.remove(ov);
            inEdges.putIfAbsent(operatorVertexToUse, new ArrayList<>());
            inEdges.get(operatorVertexToUse).addAll(inListToModify);

            final List<IREdge> outListToModify = outEdges.getOrDefault(ov, new ArrayList<>());
            outEdges.getOrDefault(ov, new ArrayList<>()).forEach(e -> {
              outListToModify.remove(e);
              final IREdge newIrEdge = new IREdge(e.getType(), operatorVertexToUse, e.getDst(), e.getCoder());
              outListToModify.add(newIrEdge);
            });
            outEdges.remove(ov);
            outEdges.putIfAbsent(operatorVertexToUse, new ArrayList<>());
            outEdges.get(operatorVertexToUse).addAll(outListToModify);
          }
        }
      });
      mergeAndAddToBuilder(dependencyFailedOperatorVertices, builder, dag, inEdges, outEdges);
    }
  }
}
