package dag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class DAGBuilder {
  private HashMap<String, List<Edge>> nodeIdToinEdges = new HashMap<>();
  private HashMap<String, List<Edge>> nodeIdTooutEdges = new HashMap<>();
  private List<InternalNode> nodes = new ArrayList<>();

  public InternalNode createNode(final Operator operator) {
    final InternalNode node = new InternalNode(operator);
    nodes.add(node);
    return node;
  }

  public <I, O> Edge<I, O> connectNodes(final InternalNode<?, I> src, final InternalNode<O, ?> dst, final Edge.Type type) {
    final Edge<I, O> edge = new Edge<>(type, src, dst);
    src.addOutEdge(edge);
    dst.addInEdge(edge);
    return edge;
  }

  public DAG build() {
    // TODO: Check graph and see everything is connected and correct
    final List<InternalNode> sources = nodes.stream()
        .filter(node -> node.getType() == InternalNode.Type.Source)
        .collect(Collectors.toList());
    return new DAG(sources);
  }
}
