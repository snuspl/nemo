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
package edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.SourceVertex;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.vertex.executionproperty.ParallelismProperty;

import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static edu.snu.onyx.common.ir.executionproperty.ExecutionProperty.Key.DataCommunicationPattern;

/**
 * Optimization pass for tagging parallelism execution property.
 */
public final class DefaultParallelismPass extends AnnotatingPass {
  final int desiredSourceParallelism;

  /**
   * Default constructor with desired number of source parallelism 1.
   */
  public DefaultParallelismPass() {
    this(1);
  }

  /**
   * Default constructor.
   *
   * @param desiredSourceParallelism the desired number of source parallelism.
   */
  public DefaultParallelismPass(final Integer desiredSourceParallelism) {
    super(ExecutionProperty.Key.Parallelism, Collections.singleton(DataCommunicationPattern));
    this.desiredSourceParallelism = desiredSourceParallelism;
  }


  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    // Propagate forward source parallelism
    dag.topologicalDo(vertex -> {
      try {
        final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex).stream()
            .filter(edge -> !Boolean.TRUE.equals(edge.isSideInput()))
            .collect(Collectors.toList());
        if (inEdges.isEmpty() && vertex instanceof SourceVertex) {
          // For source vertices, we try to split the source reader by the desired source parallelism.
          // After that, we set the parallelism as the number of split readers.
          // (It can be greater than the desired value.)
          final SourceVertex sourceVertex = (SourceVertex) vertex;
          vertex.setProperty(ParallelismProperty.of(sourceVertex.getReaders(desiredSourceParallelism).size()));
        } else if (!inEdges.isEmpty()) {
          final OptionalInt parallelism = inEdges.stream()
              // No reason to propagate via Broadcast edges, as the data streams that will use the broadcasted data
              // as a sideInput will have their own number of parallelism
              .filter(edge -> !edge.getProperty(DataCommunicationPattern)
                  .equals(DataCommunicationPatternProperty.Value.BroadCast))
              .mapToInt(edge -> edge.getSrc().getProperty(ExecutionProperty.Key.Parallelism))
              .max();
          if (parallelism.isPresent()) {
            vertex.setProperty(ParallelismProperty.of(parallelism.getAsInt()));
          }
        } else {
          throw new RuntimeException("There is a non-source vertex that doesn't have any inEdges other than SideInput");
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);
    return builder.build();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final DefaultParallelismPass that = (DefaultParallelismPass) o;

    return desiredSourceParallelism == that.desiredSourceParallelism;
  }

  @Override
  public int hashCode() {
    return desiredSourceParallelism;
  }
}
