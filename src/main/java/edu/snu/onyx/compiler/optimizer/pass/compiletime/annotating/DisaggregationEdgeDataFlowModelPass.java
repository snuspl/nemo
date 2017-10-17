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

import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.compiler.ir.IREdge;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.compiler.ir.executionproperty.edge.DataFlowModelProperty;
import edu.snu.onyx.runtime.executor.datatransfer.communication.ScatterGather;

import java.util.List;

/**
 * A pass to support Disaggregated Resources by tagging edges.
 * This pass handles the DataFlowModel ExecutionProperty.
 */
public final class DisaggregationEdgeDataFlowModelPass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "DisaggregationEdgeDataFlowModelPass";

  public DisaggregationEdgeDataFlowModelPass() {
    super(ExecutionProperty.Key.DataFlowModel);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge -> {
        if (ScatterGather.class.equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))) {
          edge.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Push)); // Push to the merger vertex.
        } else {
          edge.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));
        }
      });
    });
    return dag;
  }
}
