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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * IRVertex that contains a partial DAG that is iterative.
 */
public final class LoopVertex extends IRVertex {
  private final DAGBuilder<IRVertex, IREdge> builder;
  private final Map<IRVertex, Set<IREdge>> incomingEdges;
  private final Map<IRVertex, Set<IREdge>> outgoingEdges;
  private final String compositeTransformFullName;
  private Integer iterationNum;

  public LoopVertex(final String compositeTransformFullName) {
    super();
    this.builder = new DAGBuilder<>();
    this.incomingEdges = new HashMap<>();
    this.outgoingEdges = new HashMap<>();
    this.compositeTransformFullName = compositeTransformFullName;
    this.iterationNum = 1;
  }

  public void setIterationNum(final Integer iterationNum) {
    this.iterationNum = iterationNum;
  }

  public DAGBuilder<IRVertex, IREdge> getBuilder() {
    return builder;
  }

  public DAG<IRVertex, IREdge> getDAG() {
    return builder.build();
  }

  public void addIncomingEdge(final IREdge edge) {
    this.incomingEdges.putIfAbsent(edge.getDst(), new HashSet<>());
    this.incomingEdges.get(edge.getDst()).add(edge);
  }

  public Map<IRVertex, Set<IREdge>> getIncomingEdges() {
    return this.incomingEdges;
  }

  public void addOutgoingEdge(final IREdge edge) {
    this.outgoingEdges.putIfAbsent(edge.getSrc(), new HashSet<>());
    this.outgoingEdges.get(edge.getSrc()).add(edge);
  }

  public Map<IRVertex, Set<IREdge>> getOutgoingEdges() {
    return this.outgoingEdges;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(", name: " + compositeTransformFullName);
    sb.append(", DAG:\n<<  ");
    sb.append(getDAG());
    sb.append(">>");
    return sb.toString();
  }
}
