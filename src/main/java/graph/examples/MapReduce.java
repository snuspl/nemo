package graph.examples;

import graph.*;

import java.util.LinkedList;
import java.util.List;

public class MapReduce {
  public static void main(final String[] args) {
    final EmptyOp<String, String, String, Integer> mapOp = new EmptyOp<>("MapOperator");
    final EmptyOp<String, Integer, String, Integer> reduceOp = new EmptyOp<>("ReduceOperator");
    final GraphBuilder gb = new GraphBuilder();




    final Node<String, String, String, Integer> mapNode = gb.createNode(mapOp);
    final Node<String, Integer, String, Integer> reduceNode = gb.createNode(reduceOp);

    // Before
    gb.connectNodes(mapNode, reduceNode, Edge.Type.NtoN);

    final Graph graph = gb.build();
    System.out.println("Before Operator Placement");
    Graph.print(graph);

    // Optimize
    final List<Node> topoSorted = new LinkedList<>();
    Graph.doDFS(graph, (node -> topoSorted.add(0, node)), Graph.VisitOrder.Post);
    topoSorted.forEach(node -> {
      if (node.isSource()) {
        node.getAttributes().put(Attributes.placement, "transient");
      } else {
        final List<Edge> inEdges = node.getInEdges();
        if (inEdges.stream().filter(edge -> edge.getType() == Edge.Type.NtoN).count() == 0) {
          node.getAttributes().put(Attributes.placement, "transient");
        } else {
          node.getAttributes().put(Attributes.placement, "reserved");
        }
      }
    });

    // After
    System.out.println("After Operator Placement");
    Graph.print(graph);
  }
}
