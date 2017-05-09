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
package edu.snu.vortex.compiler.ir;

import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;

import java.util.*;
import java.util.function.IntPredicate;

/**
 * IRVertex that contains a partial DAG that is iterative.
 */
public final class LoopVertex extends IRVertex {
  private final DAGBuilder<IRVertex, IREdge> builder; // Contains DAG information
  private final String compositeTransformFullName;
  private final LoopVertex assignedLoopVertex; // loop vertex that encloses this loop vertex.

  private final Map<IRVertex, Set<IREdge>> dagIncomingEdges; // for the initial iteration
  private final Map<IRVertex, Set<IREdge>> iterativeIncomingEdges; // for iterations
  private final Map<IRVertex, Set<IREdge>> nonIterativeIncomingEdges; // for iterations
  private final Map<IRVertex, Set<IREdge>> dagOutgoingEdges; // for the final iteration
  private final HashMap<IRVertex, Iterator<IRVertex>> equivalentVerticesOfLoop;

  private Integer maxNumberOfIterations;
  private IntPredicate terminationCondition;

  public LoopVertex(final String compositeTransformFullName, final Stack<LoopVertex> loopVertexStack) {
    super();
    this.builder = new DAGBuilder<>();
    this.compositeTransformFullName = compositeTransformFullName;
    this.dagIncomingEdges = new HashMap<>();
    this.iterativeIncomingEdges = new HashMap<>();
    this.nonIterativeIncomingEdges = new HashMap<>();
    this.dagOutgoingEdges = new HashMap<>();
    this.equivalentVerticesOfLoop = new HashMap<>();
    this.maxNumberOfIterations = 1; // 1 is the default number of iterations.
    this.terminationCondition = (integer -> false); // nothing much yet.

    if (loopVertexStack.empty()) {
      this.assignedLoopVertex = null;
    } else {
      this.assignedLoopVertex = loopVertexStack.peek();
    }
  }

  public DAGBuilder<IRVertex, IREdge> getBuilder() {
    return builder;
  }

  public DAG<IRVertex, IREdge> getDAG() {
    return builder.build();
  }

  public String getName() {
    return compositeTransformFullName;
  }

  public LoopVertex getAssignedLoopVertex() {
    return assignedLoopVertex;
  }

  public void addDagIncomingEdge(final IREdge edge) {
    this.dagIncomingEdges.putIfAbsent(edge.getDst(), new HashSet<>());
    this.dagIncomingEdges.get(edge.getDst()).add(edge);
  }
  public Map<IRVertex, Set<IREdge>> getDagIncomingEdges() {
    return this.dagIncomingEdges;
  }

  public void addIterativeIncomingEdge(final IREdge edge) {
    this.iterativeIncomingEdges.putIfAbsent(edge.getDst(), new HashSet<>());
    this.iterativeIncomingEdges.get(edge.getDst()).add(edge);
  }
  public Map<IRVertex, Set<IREdge>> getIterativeIncomingEdges() {
    return this.iterativeIncomingEdges;
  }

  public void addNonIterativeIncomingEdge(final IREdge edge) {
    this.nonIterativeIncomingEdges.putIfAbsent(edge.getDst(), new HashSet<>());
    this.nonIterativeIncomingEdges.get(edge.getDst()).add(edge);
  }
  public Map<IRVertex, Set<IREdge>> getNonIterativeIncomingEdges() {
    return this.nonIterativeIncomingEdges;
  }

  public void addDagOutgoingEdge(final IREdge edge) {
    this.dagOutgoingEdges.putIfAbsent(edge.getSrc(), new HashSet<>());
    this.dagOutgoingEdges.get(edge.getSrc()).add(edge);
  }

  public Map<IRVertex, Set<IREdge>> getDagOutgoingEdges() {
    return this.dagOutgoingEdges;
  }

  public void setEquivalentVerticesOfLoop(final HashMap<IRVertex, IRVertex> equivalentVerticesOfLoop) {
    final HashMap<IRVertex, List<IRVertex>> mapToList = new HashMap<>();
    equivalentVerticesOfLoop.forEach((vertex, rootVertex) -> {
      mapToList.putIfAbsent(rootVertex, new LinkedList<>());
      mapToList.get(rootVertex).add(vertex);
    });
    mapToList.forEach((vertex, list) -> {
      Collections.sort(list, Comparator.comparingInt(v -> Integer.parseInt(v.getId().substring("vertex".length()))));
      this.equivalentVerticesOfLoop.putIfAbsent(vertex, list.iterator());
    });
  }

  public LoopVertex unRollIteration(final DAGBuilder<IRVertex, IREdge> dagBuilder) {
    final HashMap<IRVertex, IRVertex> originalToNewIRVertex = new HashMap<>();
    final DAG<IRVertex, IREdge> dagToAdd = getDAG();

    decreaseMaxNumberOfIterations();

    // add the DAG and internal edges to the dagBuilder.
    dagToAdd.topologicalDo(irVertex -> {
      final IRVertex newIrVertex = equivalentVerticesOfLoop.get(irVertex).next();
      originalToNewIRVertex.putIfAbsent(irVertex, newIrVertex);

      dagBuilder.addVertex(newIrVertex);
      dagToAdd.getIncomingEdgesOf(irVertex).forEach(edge -> {
        final IRVertex newSrc = originalToNewIRVertex.get(edge.getSrc());
        final IREdge newIrEdge = new IREdge(edge.getType(), newSrc, newIrVertex);
        IREdge.copyAttributes(edge, newIrEdge);
        dagBuilder.connectVertices(newIrEdge);
      });
    });

    // process DAG incoming edges.
    getDagIncomingEdges().forEach((dstVertex, irEdges) -> irEdges.forEach(edge -> {
      final IREdge newIrEdge = new IREdge(edge.getType(), edge.getSrc(), originalToNewIRVertex.get(dstVertex));
      IREdge.copyAttributes(edge, newIrEdge);
      dagBuilder.connectVertices(newIrEdge);
    }));

    if (loopTerminationConditionMet()) {
      // if termination condition met, we process the DAG outgoing edge.
      getDagOutgoingEdges().forEach((srcVertex, irEdges) -> irEdges.forEach(edge -> {
        final IREdge newIrEdge = new IREdge(edge.getType(), originalToNewIRVertex.get(srcVertex), edge.getDst());
        IREdge.copyAttributes(edge, newIrEdge);
        dagBuilder.connectVertices(newIrEdge);
      }));
    }

    // process next iteration's DAG incoming edges
    this.getDagIncomingEdges().clear();
    this.nonIterativeIncomingEdges.forEach((dstVertex, irEdges) -> irEdges.forEach(this::addDagIncomingEdge));
    this.iterativeIncomingEdges.forEach((dstVertex, irEdges) -> irEdges.forEach(edge -> {
      final IREdge newIrEdge = new IREdge(edge.getType(), originalToNewIRVertex.get(edge.getSrc()), dstVertex);
      IREdge.copyAttributes(edge, newIrEdge);
      this.addDagIncomingEdge(newIrEdge);
    }));

    return this;
  }

  public Boolean loopTerminationConditionMet() {
    return loopTerminationConditionMet(0);
  }
  public Boolean loopTerminationConditionMet(final Integer intPredicateInput) {
    return maxNumberOfIterations <= 0 || terminationCondition.test(intPredicateInput);
  }

  public void setMaxNumberOfIterations(final Integer maxNum) {
    this.maxNumberOfIterations = maxNum;
  }
  public void increaseMaxNumberOfIterations() {
    this.maxNumberOfIterations++;
  }
  private void decreaseMaxNumberOfIterations() {
    this.maxNumberOfIterations--;
  }

  public void setTerminationCondition(final IntPredicate terminationCondition) {
    this.terminationCondition = terminationCondition;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(irVertexPropertiesToString());
    sb.append(", \"Remaining Iteration\": ");
    sb.append(this.maxNumberOfIterations);
    sb.append(", \"DAG\": ");
    sb.append(getDAG());
    sb.append("}");
    return sb.toString();
  }
}
