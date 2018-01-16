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
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.CompressionProperty;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;


/**
 * A pass for applying compression algorithm for data flowing between vertices.
 */
public final class CompressionPass extends AnnotatingPass {
  private final CompressionProperty.Compressor compressor;

  public CompressionPass(CompressionProperty.Compressor compressor) {
    super(ExecutionProperty.Key.Compressor);
    this.compressor = compressor;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex).stream()
        .filter(e -> !vertex.getProperty(ExecutionProperty.Key.StageId)
            .equals(e.getSrc().getProperty(ExecutionProperty.Key.StageId)))
        .forEach(edge -> edge.setProperty(CompressionProperty.of(compressor))));

    return dag;
  }
}
