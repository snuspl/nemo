package graph;

import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

public class Graph {
  private final List<Node> sources;

  public Graph(final List<Node> sources) {
    this.sources = sources;
  }

  public List<Node> getSources() {
    return sources;
  }

  ////////// Auxiliary functions for graph construction and view

  public static void print(final Graph graph) {
    doDFS(graph, (node -> System.out.println(node)), VisitOrder.Pre);
  }

  ////////// DFS Traversal
  public enum VisitOrder {
    Pre,
    Post
  }

  private static HashSet visited;

  public static void doDFS(final Graph graph,
                           final Consumer<Node> function,
                           final VisitOrder visitOrder) {
    visited = new HashSet();
    for (final Node node : graph.getSources()) {
      if (!visited.contains(node)) {
        DFSVisit(node, function, visitOrder);
      }
    }
    visited = null;
  }

  private static <K, V> void DFSVisit(final Node node,
                                      final Consumer<Node> nodeConsumer,
                                      final VisitOrder visitOrder) {
    if (visitOrder == VisitOrder.Pre) {
      nodeConsumer.accept(node);
    }
    final List<Edge> outEdges = node.getOutEdges();
    for (final Edge<K, V> outEdge : outEdges) {
      final Node outNode = outEdge.getDst();
      if (!visited.contains(outNode)) {
        DFSVisit(outNode, nodeConsumer, visitOrder);
      }
    }
    if (visitOrder == VisitOrder.Post) {
      nodeConsumer.accept(node);
    }
  }
}

