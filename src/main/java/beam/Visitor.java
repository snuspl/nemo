package beam;

import dag.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PValue;
import util.Pair;

import java.util.HashMap;

class Visitor implements Pipeline.PipelineVisitor {
  private final DAGBuilder db;
  private final HashMap<Integer, Pair<Node, Edge.Type>> hashToInNode;

  Visitor(final DAGBuilder db) {
    this.db = db;
    this.hashToInNode = new HashMap<>();
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
    if (beamNode.getTransform() instanceof GroupByKey) {
      if (beamNode.getInputs().size() != 1)
        throw new IllegalStateException();
      final Pair<Node, Edge.Type> src = hashToInNode.get(beamNode.getInputs().iterator().next().hashCode());
      src.val = Edge.Type.M2M;
      if (beamNode.getOutputs().size() != 1)
        throw new IllegalStateException();
      hashToInNode.put(beamNode.getOutputs().iterator().next().hashCode(), src);
    } else {
      final Node newNode = createNode(beamNode);
      beamNode.getOutputs()
          .forEach(output -> hashToInNode.put(output.hashCode(), new Pair<>(newNode, Edge.Type.O2O)));
      beamNode.getInputs().stream()
          .filter(input -> hashToInNode.containsKey(input.hashCode()))
          .map(input -> hashToInNode.get(input.hashCode()))
          .forEach(pair -> db.connectNodes(pair.key, newNode, pair.val));
    }
  }

  @Override
  public void visitValue(final PValue value, final TransformHierarchy.Node  producer) {
    // System.out.println("visitv value " + value);
    // System.out.println("visitv producer " + producer.getTransform());
  }

  private <I, O> Node createNode(final TransformHierarchy.Node  beamNode) {
    final PTransform transform = beamNode.getTransform();
    if (transform instanceof Read.Bounded) {
      // Source
      final Read.Bounded<O> read = (Read.Bounded)transform;
      return db.createNode(new Source<>(read.getSource()));
    } else if (transform instanceof Write.Bound) {
      // Sink
      /*
      final Write.Bound<I> write = (Write.Bound)transform;
      final Sink<I> sink = write.getSink();
      final Sink.WriteOperation wo = sink.createWriteOperation();
      final Sink.Writer<I, ?> writer = wo.createWriter(
      */
      throw new UnsupportedOperationException();
    } else if (transform instanceof ParDo.Bound) {
      //  Internal
      final ParDo.Bound<I, O> pd = (ParDo.Bound<I, O>)transform;
      final DoFn<I, O> fn = pd.getNewFn();
      return db.createNode(new DoFnOperator<>(fn));
    } else {
      throw new IllegalArgumentException("Unknown Transform: " + transform);
    }
  }
}
