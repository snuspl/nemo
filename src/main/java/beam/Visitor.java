package beam;

import beam.node.BeamBroadcast;
import beam.node.BeamDo;
import beam.node.BeamSource;
import dag.*;
import dag.node.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.PValue;

import java.util.HashMap;

class Visitor implements Pipeline.PipelineVisitor {
  private final DAGBuilder builder;
  private final HashMap<PValue, Node> PValueToNodeOutput;

  Visitor(final DAGBuilder builder) {
    this.builder = builder;
    this.PValueToNodeOutput = new HashMap<>();
  }

  @Override
  public CompositeBehavior enterCompositeTransform(final TransformHierarchy.Node node) {
    // System.out.println("enter composite " + node.getTransform());
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(final TransformHierarchy.Node node) {
    // System.out.println("leave composite " + node.getTransform());
  }

  @Override
  public void visitPrimitiveTransform(final TransformHierarchy.Node beamNode) {
    // System.out.println("visitp " + beamNode.getTransform());
    if (beamNode.getOutputs().size() > 1 || beamNode.getInputs().size() > 1)
      throw new UnsupportedOperationException(beamNode.toString());

    final Node newNode = createNode(beamNode);
    builder.addNode(newNode);

    beamNode.getOutputs()
        .forEach(output -> PValueToNodeOutput.put(output, newNode));

    beamNode.getInputs().stream()
        .filter(PValueToNodeOutput::containsKey)
        .map(PValueToNodeOutput::get)
        .forEach(src -> builder.connectNodes(src, newNode, getInEdgeType(newNode)));
  }

  @Override
  public void visitValue(final PValue value, final TransformHierarchy.Node  producer) {
    // System.out.println("visitv value " + value);
    // System.out.println("visitv producer " + producer.getTransform());
  }

  private <I, O> Node createNode(final TransformHierarchy.Node beamNode) {
    final PTransform transform = beamNode.getTransform();
    if (transform instanceof Read.Bounded) {
      // Source
      final Read.Bounded<O> read = (Read.Bounded)transform;
      final BeamSource<O> source = new BeamSource<>(read.getSource());
      return source;
    } else if (transform instanceof GroupByKey) {
      return new dag.node.GroupByKey();
    } else if (transform instanceof View.CreatePCollectionView) {
      final View.CreatePCollectionView view = (View.CreatePCollectionView)transform;
      final Broadcast newNode = new BeamBroadcast(view.getView());
      PValueToNodeOutput.put(view.getView(), newNode);
      return newNode;
    } else if (transform instanceof Write.Bound) {
      // Sink
      /*
      final Write.Bound<I> write = (Write.Bound)transform;
      final Sink<I> sink = write.getSink();
      final Sink.WriteOperation wo = sink.createWriteOperation();
      final Sink.Writer<I, ?> writer = wo.createWriter(
      */
      throw new UnsupportedOperationException(transform.toString());
    } else if (transform instanceof ParDo.Bound) {
      //  Internal
      final ParDo.Bound<I, O> parDo = (ParDo.Bound<I, O>)transform;
      final BeamDo<I, O> newNode = new BeamDo<>(parDo.getNewFn());
      parDo.getSideInputs().stream()
          .filter(PValueToNodeOutput::containsKey)
          .map(PValueToNodeOutput::get)
          .forEach(src -> builder.connectNodes(src, newNode, Edge.Type.O2O)); // Broadcasted = O2O
      return newNode;
    } else {
      throw new UnsupportedOperationException(transform.toString());
    }
  }


  private Edge.Type getInEdgeType(final Node node) {
    if (node instanceof dag.node.GroupByKey) {
      return Edge.Type.M2M;
    } else if (node instanceof dag.node.Broadcast) {
      return Edge.Type.O2M;
    } else {
      return Edge.Type.O2O;
    }
  }
}
