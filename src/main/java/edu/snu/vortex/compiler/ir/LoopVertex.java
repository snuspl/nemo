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

  public LoopVertex(final String compositeTransformFullName, final Stack<LoopVertex> loopVertexStack) {
    super();
    this.builder = new DAGBuilder<>();
    this.compositeTransformFullName = compositeTransformFullName;
    this.dagIncomingEdges = new HashMap<>();
    this.iterativeIncomingEdges = new HashMap<>();
    this.nonIterativeIncomingEdges = new HashMap<>();
    this.dagOutgoingEdges = new HashMap<>();

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
    this.nonIterativeIncomingEdges.putIfAbsent(edge.getDst(), new HashSet<>());
    this.nonIterativeIncomingEdges.get(edge.getDst()).add(edge);
  }

  public Map<IRVertex, Set<IREdge>> getDagIncomingEdges() {
    return this.dagIncomingEdges;
  }

  public void addIterativeIncomingEdge(final IREdge edge) {
    this.iterativeIncomingEdges.putIfAbsent(edge.getDst(), new HashSet<>());
    this.iterativeIncomingEdges.get(edge.getDst()).add(edge);
    this.nonIterativeIncomingEdges.get(edge.getDst()).remove(edge);
  }

  public Map<IRVertex, Set<IREdge>> getIterativeIncomingEdges() {
    return this.iterativeIncomingEdges;
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

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(irVertexPropertiesToString());
//    sb.append(", \"Remaining Iteration\": ");
//    sb.append(getNumberOfIterations());
    sb.append(", \"DAG\": ");
    sb.append(getDAG());
    sb.append("}");
    return sb.toString();
  }
}
