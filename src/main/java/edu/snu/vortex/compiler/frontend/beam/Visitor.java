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

import edu.snu.vortex.compiler.frontend.beam.operator.BroadcastImpl;
import edu.snu.vortex.compiler.frontend.beam.operator.DoImpl;
import edu.snu.vortex.compiler.frontend.beam.operator.SourceImpl;
import edu.snu.vortex.compiler.optimizer.ir.DAGBuilder;
import edu.snu.vortex.compiler.optimizer.ir.Edge;
import edu.snu.vortex.compiler.optimizer.ir.operator.Broadcast;
import edu.snu.vortex.compiler.optimizer.ir.operator.Operator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.PValue;

import java.util.HashMap;
import java.util.Map;

class Visitor implements Pipeline.PipelineVisitor {
  private final DAGBuilder builder;
  private final Map<PValue, Operator> pValueToOpOutput;

  Visitor(final DAGBuilder builder) {
    this.builder = builder;
    this.pValueToOpOutput = new HashMap<>();
  }

  @Override
  public CompositeBehavior enterCompositeTransform(final TransformHierarchy.Node beamOp) {
    // Print if needed for development
    // System.out.println("enter composite " + node.getTransform());
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(final TransformHierarchy.Node beamOp) {
    // Print if needed for development
    // System.out.println("leave composite " + node.getTransform());
  }

  @Override
  public void visitPrimitiveTransform(final TransformHierarchy.Node beamOp) {
    // Print if needed for development
    // System.out.println("visitp " + beamOp.getTransform());
    if (beamOp.getOutputs().size() > 1 || beamOp.getInputs().size() > 1)
      throw new UnsupportedOperationException(beamOp.toString());

    final Operator newOp = createOperator(beamOp);
    builder.addOp(newOp);

    beamOp.getOutputs()
        .forEach(output -> pValueToOpOutput.put(output, newOp));

    beamOp.getInputs().stream()
        .filter(pValueToOpOutput::containsKey)
        .map(pValueToOpOutput::get)
        .forEach(src -> builder.connectOps(src, newOp, getInEdgeType(newOp)));
  }

  @Override
  public void visitValue(final PValue value, final TransformHierarchy.Node producer) {
    // Print if needed for development
    // System.out.println("visitv value " + value);
    // System.out.println("visitv producer " + producer.getTransform());
  }

  private <I, O> Operator createOperator(final TransformHierarchy.Node beamOp) {
    final PTransform transform = beamOp.getTransform();
    if (transform instanceof Read.Bounded) {
      final Read.Bounded<O> read = (Read.Bounded)transform;
      final SourceImpl<O> source = new SourceImpl<>(read.getSource());
      return source;
    } else if (transform instanceof GroupByKey) {
      return new edu.snu.vortex.compiler.optimizer.ir.operator.GroupByKey();
    } else if (transform instanceof View.CreatePCollectionView) {
      final View.CreatePCollectionView view = (View.CreatePCollectionView)transform;
      final Broadcast newOp = new BroadcastImpl(view.getView());
      pValueToOpOutput.put(view.getView(), newOp);
      return newOp;
    } else if (transform instanceof Write.Bound) {
      throw new UnsupportedOperationException(transform.toString());
    } else if (transform instanceof ParDo.Bound) {
      final ParDo.Bound<I, O> parDo = (ParDo.Bound<I, O>)transform;
      final DoImpl<I, O> newOp = new DoImpl<>(parDo.getNewFn());
      parDo.getSideInputs().stream()
          .filter(pValueToOpOutput::containsKey)
          .map(pValueToOpOutput::get)
          .forEach(src -> builder.connectOps(src, newOp, Edge.Type.O2O)); // Broadcasted = O2O
      return newOp;
    } else {
      throw new UnsupportedOperationException(transform.toString());
    }
  }

  private Edge.Type getInEdgeType(final Operator op) {
    if (op instanceof edu.snu.vortex.compiler.optimizer.ir.operator.GroupByKey) {
      return Edge.Type.M2M;
    } else if (op instanceof Broadcast) {
      return Edge.Type.O2M;
    } else {
      return Edge.Type.O2O;
    }
  }
}
