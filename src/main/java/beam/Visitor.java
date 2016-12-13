package beam;

import dag.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.runners.TransformTreeNode;
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
  public CompositeBehavior enterCompositeTransform(final TransformTreeNode node) {
    System.out.println("enter composite " + node.getTransform());
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(final TransformTreeNode node) {
    System.out.println("leave composite " + node.getTransform());
  }

  @Override
  public void visitPrimitiveTransform(final TransformTreeNode beamNode) {
    System.out.println("visitp " + beamNode.getTransform());
    if (beamNode.getTransform() instanceof GroupByKey) {
      final Pair<Node, Edge.Type> src = hashToInNode.get(beamNode.getInput().hashCode());
      src.val = Edge.Type.M2M;
      hashToInNode.put(beamNode.getOutput().hashCode(), src);
    } else {
      final Node newNode = createNode(beamNode);
      beamNode.getExpandedOutputs()
          .forEach(output -> hashToInNode.put(output.hashCode(), new Pair<>(newNode, Edge.Type.O2O)));
      beamNode.getInputs().keySet().stream()
          .filter(input -> hashToInNode.containsKey(input.hashCode()))
          .map(input -> hashToInNode.get(input.hashCode()))
          .forEach(pair -> db.connectNodes(pair.key, newNode, pair.val));
    }
  }

  @Override
  public void visitValue(final PValue value, final TransformTreeNode producer) {
    // System.out.println("visitv value " + value);
    // System.out.println("visitv producer " + producer.getTransform());
  }

  private <I, O> Node createNode(final TransformTreeNode beamNode) {
    final PTransform transform = beamNode.getTransform();
    if (transform instanceof Read.Bounded) {
      // Source
      final Read.Bounded<O> read = (Read.Bounded)transform;
      return db.createNode(new Source<>(read.getSource()));
    } else if (transform instanceof Write.Bound) {
      // Sink
      final Write.Bound write = (Write.Bound)transform;
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
