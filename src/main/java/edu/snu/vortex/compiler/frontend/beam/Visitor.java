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

import edu.snu.vortex.compiler.frontend.beam.operator.*;
import edu.snu.vortex.compiler.frontend.beam.operator.DoFn;
import edu.snu.vortex.compiler.ir.DAGBuilder;
import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.Vertex;
import edu.snu.vortex.compiler.ir.Operator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PValue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

    final List<Vertex> vortexVertices = createOperator(beamOperator);
    vortexVertices.forEach(vortexOperator -> {
      builder.addOperator(vortexOperator);

      beamOperator.getOutputs()
          .forEach(output -> pValueToOpOutput.put(output, vortexOperator));

      beamOperator.getInputs().stream()
          .filter(pValueToOpOutput::containsKey)
          .map(pValueToOpOutput::get)
          .forEach(src -> builder.connectOperators(src, vortexOperator, getInEdgeType(vortexOperator)));
    });
  }

  /**
   * The function creates the nodes accordingly by each of the types.
   * @param beamOperator input beam operator.
   * @return output Vortex IR operator.
   */
  private <I, O> List<Vertex> createOperator(final TransformHierarchy.Node beamOperator) {
    final PTransform transform = beamOperator.getTransform();
    if (transform instanceof Read.Bounded) {
      final Read.Bounded<O> read = (Read.Bounded) transform;
      final BoundedSource<O> source = new BoundedSource<>(read.getSource());
      return Arrays.asList(source);
    } else if (transform instanceof GroupByKey) {
      // TODO: Partition and Merge
      final Vertex partitionVertex = new Vertex(new PartitionKV());
      final Vertex mergeVertex = new Vertex(new MergeKV());
      return Arrays.asList(partitionVertex, mergeVertex);
    } else if (transform instanceof Window.Bound) {
      final Window.Bound<I> window = (Window.Bound<I>) transform;
      final WindowFn vortexOperator = new WindowFn(window.getWindowFn());
      return Arrays.asList(new Vertex(vortexOperator));
    } else if (transform instanceof Write.Bound) {
      throw new UnsupportedOperationException(transform.toString());
    } else if (transform instanceof ParDo.Bound) {
      final ParDo.Bound<I, O> parDo = (ParDo.Bound<I, O>) transform;
      final DoFn vortexOperator = new DoFn(parDo.getNewFn(), options);
      return Arrays.asList(new Vertex(vortexOperator));
    } else {
      throw new UnsupportedOperationException(transform.toString());
    }
  }

  private Edge.Type getInEdgeType(final Vertex vertex) {
    final Operator udf = vertex.getOperator();
    if (udf instanceof MergeKV) {
      return Edge.Type.ScatterGather;
    } else {
      return Edge.Type.OneToOne;
    }
  }
}
