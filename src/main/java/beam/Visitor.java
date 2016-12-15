package beam;

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
  private final HashMap<Integer, Node> hashToNodeOutput;

  Visitor(final DAGBuilder builder) {
    this.builder = builder;
    this.hashToNodeOutput = new HashMap<>();
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
        .forEach(output -> hashToNodeOutput.put(output.hashCode(), newNode));

    beamNode.getInputs().stream()
        .filter(input -> hashToNodeOutput.containsKey(input.hashCode()))
        .map(input -> hashToNodeOutput.get(input.hashCode()))
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
      final ParDo.Bound<I, O> pd = (ParDo.Bound<I, O>)transform;
      return new BeamDo<>(pd.getNewFn());
    } else {
      throw new UnsupportedOperationException(transform.toString());
    }
  }

  private Edge.Type getInEdgeType(final Node node) {
    if (node instanceof dag.node.GroupByKey) {
      return Edge.Type.M2M;
    } else {
      return Edge.Type.O2O;
    }
  }
}
