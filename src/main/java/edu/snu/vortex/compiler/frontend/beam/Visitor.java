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
package edu.snu.vortex.compiler.frontend.beam;

import edu.snu.vortex.compiler.frontend.beam.transform.DoFn;
import edu.snu.vortex.compiler.frontend.beam.transform.GroupByKeyFn;
import edu.snu.vortex.compiler.frontend.beam.transform.WindowFn;
import edu.snu.vortex.compiler.ir.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Visitor class.
 * This class visits every operator in the dag to translate the BEAM program to the Vortex IR.
 */
final class Visitor extends Pipeline.PipelineVisitor.Defaults {
  private final DAGBuilder builder;
  private final Map<PValue, Vertex> pValueToOpOutput;
  private final PipelineOptions options;

  Visitor(final DAGBuilder builder, final PipelineOptions options) {
    this.builder = builder;
    this.pValueToOpOutput = new HashMap<>();
    this.options = options;
  }

  @Override
  public void visitPrimitiveTransform(final TransformHierarchy.Node beamOperator) {
    // Print if needed for development
    // System.out.println("visitp " + beamOperator.getTransform());
    if (beamOperator.getOutputs().size() > 1 || beamOperator.getInputs().size() > 1) {
      throw new UnsupportedOperationException(beamOperator.toString());
    }

    final Vertex vortexVertex = convertToVertex(beamOperator);
    builder.addVertex(vortexVertex);

    beamOperator.getOutputs()
        .forEach(output -> pValueToOpOutput.put(output, vortexVertex));

    if (vortexVertex instanceof OperatorVertex) {
      beamOperator.getInputs().stream()
          .filter(pValueToOpOutput::containsKey)
          .map(pValueToOpOutput::get)
          .forEach(src -> builder.connectVertices(src, vortexVertex, getInEdgeType((OperatorVertex) vortexVertex)));
    }
  }

  /**
   * Create vertex.
   * @param beamOperator input beam operator.
   * @param <I> input type.
   * @param <O> output type.
   * @return vertex.
   */
  private <I, O> Vertex convertToVertex(final TransformHierarchy.Node beamOperator) {
    final PTransform transform = beamOperator.getTransform();
    if (transform instanceof Read.Bounded) {
      final Read.Bounded<O> read = (Read.Bounded) transform;
      final Vertex sourceVertex = new BoundedSourceVertex<>(read.getSource());
      return sourceVertex;
    } else if (transform instanceof GroupByKey) {
      return new OperatorVertex(new GroupByKeyFn());
    } else if (transform instanceof Window.Bound) {
      final Window.Bound<I> window = (Window.Bound<I>) transform;
      final WindowFn vortexOperator = new WindowFn(window.getWindowFn());
      return new OperatorVertex(vortexOperator);
    } else if (transform instanceof Write.Bound) {
      throw new UnsupportedOperationException(transform.toString());
    } else if (transform instanceof ParDo.Bound) {
      final ParDo.Bound<I, O> parDo = (ParDo.Bound<I, O>) transform;
      final DoFn vortexOperator = new DoFn(parDo.getNewFn(), options);
      return new OperatorVertex(vortexOperator);
    } else {
      throw new UnsupportedOperationException(transform.toString());
    }
  }

  private Edge.Type getInEdgeType(final OperatorVertex vertex) {
    final Transform transform = vertex.getTransform();
    if (transform instanceof GroupByKeyFn) {
      return Edge.Type.ScatterGather;
    } else {
      return Edge.Type.OneToOne;
    }
  }
}
