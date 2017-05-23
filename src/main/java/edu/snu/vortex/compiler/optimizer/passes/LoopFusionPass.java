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

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.LoopVertex;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;

import java.util.*;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

/**
 * Pass for Loop Fusion.
 */
public final class LoopFusionPass implements Pass {
  @Override
  public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
    final Map<IntPredicate, List<LoopVertex>> loopVertices = new HashMap<>();
    final Map<LoopVertex, List<IREdge>> inEdges = new HashMap<>();
    final Map<LoopVertex, List<IREdge>> outEdges = new HashMap<>();
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    // Collect loop vertices.
    dag.topologicalDo(irVertex -> {
      if (irVertex instanceof LoopVertex) {
        final LoopVertex loopVertex = (LoopVertex) irVertex;
        loopVertices.putIfAbsent(loopVertex.getTerminationCondition(), new ArrayList<>());
        loopVertices.get(loopVertex.getTerminationCondition()).add(loopVertex);

        dag.getIncomingEdgesOf(loopVertex).forEach(irEdge -> {
          inEdges.putIfAbsent(loopVertex, new ArrayList<>());
          inEdges.get(loopVertex).add(irEdge);
        });
      } else {
        builder.addVertex(irVertex, dag);
        dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          if (irEdge.getSrc() instanceof LoopVertex) {
            final LoopVertex loopVertex = (LoopVertex) irEdge.getSrc();
            outEdges.putIfAbsent(loopVertex, new ArrayList<>());
            outEdges.get(loopVertex).add(irEdge);
          } else {
            builder.connectVertices(irEdge);
          }
        });
      }
    });

    // Refactor those with same data scan / operation, without dependencies in the loop.
    loopVertices.values().forEach(loops -> loops.forEach(loopVertex -> {
      final List<Map.Entry<IRVertex, Set<IREdge>>> candidates = loopVertex.getNonIterativeIncomingEdges().entrySet()
          .stream().filter(entry ->
              loopVertex.getDAG().getIncomingEdgesOf(entry.getKey()).size() == 0// no internal inEdges
              && loopVertex.getIterativeIncomingEdges().get(entry.getKey()).size() == 0) // no external inEdges
          .collect(Collectors.toList());
      candidates.forEach(candidate -> {
        // add refactored vertex to builder.
        builder.addVertex(candidate.getKey());
        // connect incoming edges.
        candidate.getValue().forEach(builder::connectVertices);
        // connect outgoing edges.
        loopVertex.getDAG().getOutgoingEdgesOf(candidate.getKey()).forEach(loopVertex::addNonIterativeIncomingEdge);
        // clear garbage.
        loopVertex.getBuilder().removeVertex(candidate.getKey());
        loopVertex.getNonIterativeIncomingEdges().remove(candidate.getKey());
      });
    }));

    // Merge those with same termination condition.
    loopVertices.forEach((terminationCondition, loops) -> {
      if (loops.size() > 1 && loops.stream().allMatch(loopVertex ->
          loops.get(0).getMaxNumberOfIterations().equals(loopVertex.getMaxNumberOfIterations()))) {
        final LoopVertex newLoopVertex = mergeLoopVertices(loops);
        builder.addVertex(newLoopVertex, dag);
        loops.forEach(loopVertex -> {
          // inEdges.
          inEdges.getOrDefault(loopVertex, new ArrayList<>()).forEach(irEdge -> {
            final IREdge newIREdge = new IREdge(irEdge.getType(), irEdge.getSrc(), newLoopVertex, irEdge.getCoder());
            IREdge.copyAttributes(irEdge, newIREdge);
            builder.connectVertices(newIREdge);
          });
          // outEdges.
          outEdges.getOrDefault(loopVertex, new ArrayList<>()).forEach(irEdge -> {
            final IREdge newIREdge = new IREdge(irEdge.getType(), newLoopVertex, irEdge.getDst(), irEdge.getCoder());
            IREdge.copyAttributes(irEdge, newIREdge);
            builder.connectVertices(newIREdge);
          });
        });
      }
    });

    return builder.build();
  }

  private static LoopVertex mergeLoopVertices(final List<LoopVertex> loopVertices) {
    final String newName =
        String.join("+", loopVertices.stream().map(LoopVertex::getName).collect(Collectors.toList()));
    final LoopVertex mergedLoopVertex = new LoopVertex(newName);
    loopVertices.forEach(loopVertex -> {
      final DAG<IRVertex, IREdge> dagToCopy = loopVertex.getDAG();
      dagToCopy.topologicalDo(v -> {
        mergedLoopVertex.getBuilder().addVertex(v);
        dagToCopy.getIncomingEdgesOf(v).forEach(mergedLoopVertex.getBuilder()::connectVertices);
      });
      loopVertex.getDagIncomingEdges().forEach((v, es) -> es.forEach(mergedLoopVertex::addDagIncomingEdge));
      loopVertex.getIterativeIncomingEdges().forEach((v, es) -> es.forEach(mergedLoopVertex::addIterativeIncomingEdge));
      loopVertex.getNonIterativeIncomingEdges().forEach((v, es) ->
          es.forEach(mergedLoopVertex::addNonIterativeIncomingEdge));
      loopVertex.getDagOutgoingEdges().forEach((v, es) -> es.forEach(mergedLoopVertex::addDagOutgoingEdge));
    });
    return mergedLoopVertex;
  }
}
