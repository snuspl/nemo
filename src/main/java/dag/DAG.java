package dag;

import dag.node.Node;
import dag.node.Source;

import java.util.*;
import java.util.function.Consumer;

/**
 * Public API
 */
public class DAG {
  private final HashMap<String, List<Edge>> id2inEdges;
  private final HashMap<String, List<Edge>> id2outEdges;
  private final List<Source> sources;
  private final Map<String, Object> attributes;

  public DAG(final List<Source> sources,
             final HashMap<String, List<Edge>> id2inEdges,
             final HashMap<String, List<Edge>> id2outEdges) {
    this.sources = sources;
    this.attributes = new HashMap<>();
    this.id2inEdges = id2inEdges;
    this.id2outEdges = id2outEdges;
  }

  public List<Source> getSources() {
    return sources;
  }

  public Optional<List<Edge>> getInEdges(final Node node) {
    final List<Edge> inEdges = id2inEdges.get(node.getId());
    return inEdges == null ? Optional.empty() : Optional.of(inEdges);
  }

  public Optional<List<Edge>> getOutEdges(final Node node) {
    final List<Edge> outEdges = id2outEdges.get(node.getId());
    return outEdges == null ? Optional.empty() : Optional.of(outEdges);
  }

  ////////// Auxiliary functions for graph construction and view

  public static void print(final DAG dag) {
    doDFS(dag, (node -> System.out.println("<node> " + node + " / <inEdges> " + dag.getInEdges(node))), VisitOrder.Pre);
  }

  ////////// DFS Traversal
  public enum VisitOrder {
    Pre,
    Post
  }

  private static HashSet<Node> visited;

  public static void doDFS(final DAG dag,
                           final Consumer<Node> function,
                           final VisitOrder visitOrder) {
    visited = new HashSet<>();
    dag.getSources().stream()
        .filter(source -> !visited.contains(source))
        .forEach(source -> DFSVisit(dag, source, function, visitOrder));
    visited = null;
  }

  private static void DFSVisit(final DAG dag,
                               final Node node,
                               final Consumer<Node> nodeConsumer,
                               final VisitOrder visitOrder) {
    visited.add(node);
    if (visitOrder == VisitOrder.Pre) {
      nodeConsumer.accept(node);
    }
    final Optional<List<Edge>> outEdges = dag.getOutEdges(node);
    if (outEdges.isPresent()) {
      outEdges.get().stream()
          .map(outEdge -> outEdge.getDst())
          .filter(outNode -> !visited.contains(outNode))
          .forEach(outNode -> DFSVisit(dag, outNode, nodeConsumer, visitOrder));
    }
    if (visitOrder == VisitOrder.Post) {
      nodeConsumer.accept(node);
    }
  }
}

