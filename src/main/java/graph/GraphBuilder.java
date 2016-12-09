package graph;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GraphBuilder {
  private List<Node> nodes = new ArrayList<>();

  public Node createNode(final Operator operator) {
    final Node node = new Node(operator);
    nodes.add(node);
    return node;
  }

  public <K, V> Edge<K, V> connectNodes(final Node<?, ?, K, V> src, final Node<K, V, ?, ?> dst, final Edge.Type type) {
    final Edge<K, V> edge = new Edge<>(type, src, dst);
    src.addOutEdge(edge);
    dst.addInEdge(edge);
    return edge;
  }

  public Graph build() {
    // TODO: check graph and see everything is connected and correct
    final List<Node> sources = nodes.stream().filter(node -> node.isSource()).collect(Collectors.toList());
    return new Graph(sources);
  }
}
