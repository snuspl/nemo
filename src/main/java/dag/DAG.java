package dag;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class DAG {
  private final List<InternalNode> sources;
  private final Map<String, Object> attributes;

  public DAG(final List<InternalNode> sources) {
    this.sources = sources;
    this.attributes = new HashMap<>();
  }

  public List<InternalNode> getSources() {
    return sources;
  }

  List<Edge<?, I>> getInEdges();

  List<Edge<O, ?>> getOutEdges();

  void addInEdge(final Edge<?, I> edge);

  void addOutEdge(final Edge<O, ?> edge);

  ////////// Auxiliary functions for graph construction and view

  public static void print(final DAG DAG) {
    doDFS(DAG, (node -> System.out.println(node)), VisitOrder.Pre);
  }

  ////////// DFS Traversal
  public enum VisitOrder {
    Pre,
    Post
  }

  private static HashSet visited;

  public static void doDFS(final DAG DAG,
                           final Consumer<InternalNode> function,
                           final VisitOrder visitOrder) {
    visited = new HashSet();
    for (final InternalNode node : DAG.getSources()) {
      if (!visited.contains(node)) {
        DFSVisit(node, function, visitOrder);
      }
    }
    visited = null;
  }

  private static <K, V> void DFSVisit(final InternalNode node,
                                      final Consumer<InternalNode> nodeConsumer,
                                      final VisitOrder visitOrder) {
    if (visitOrder == VisitOrder.Pre) {
      nodeConsumer.accept(node);
    }
    final List<Edge> outEdges = node.getOutEdges();
    for (final Edge<K, V> outEdge : outEdges) {
      final InternalNode outNode = outEdge.getDst();
      if (!visited.contains(outNode)) {
        DFSVisit(outNode, nodeConsumer, visitOrder);
      }
    }
    if (visitOrder == VisitOrder.Post) {
      nodeConsumer.accept(node);
    }
  }
}

