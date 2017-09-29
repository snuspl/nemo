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
package edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.MetricCollectionBarrierVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.MetricCollectionProperty;
import edu.snu.vortex.compiler.optimizer.pass.runtime.DataSkewRuntimePass;

/**
 * Pass to annotate the DAG for a job to perform data skew.
 * It specifies the outgoing ScatterGather edges from MetricCollectionVertices with a MetricCollection ExecutionProperty
 * which lets the edge to know what metric collection it should perform.
 */
public final class DataSkewEdgeMetricCollectionPass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "DataSkewEdgeMetricCollectionPass";

  public DataSkewEdgeMetricCollectionPass() {
    super(ExecutionProperty.Key.MetricCollection);
  }

  @Override
  public String getName() {
    return SIMPLE_NAME;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(v -> {
      // we only care about metric collection barrier vertices.
      if (v instanceof MetricCollectionBarrierVertex) {
        dag.getOutgoingEdgesOf(v).forEach(edge -> {
          if (IREdge.Type.ScatterGather.equals(edge.getType())) { // double checking.
            edge.setProperty(MetricCollectionProperty.of(DataSkewRuntimePass.class));
          }
        });
      }
    });
    return dag;
  }
}
