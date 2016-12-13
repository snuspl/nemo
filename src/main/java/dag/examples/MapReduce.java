package dag.examples;

import dag.*;
import util.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class MapReduce {
  public static void main(final String[] args) {
    final EmptyOperator<String, Pair<String, Integer>> mapOp = new EmptyOperator<>("MapOperator");
    final EmptyOperator<Pair<String, Iterable<Integer>>, String> reduceOp = new EmptyOperator<>("ReduceOperator");
    final DAGBuilder db = new DAGBuilder();

    final Node<String, Pair<String, Integer>> mapNode = db.createNode(mapOp);
    final Node<Pair<String, Iterable<Integer>>, String> reduceNode = db.createNode(reduceOp);

    // Before
    db.connectNodes(mapNode, reduceNode, Edge.Type.M2M);
    final DAG dag = db.build();
    System.out.println("Before DoFnOperator Placement");
    DAG.print(dag);

    // Optimize
    final List<Node> topoSorted = new LinkedList<>();
    DAG.doDFS(dag, (node -> topoSorted.add(0, node)), DAG.VisitOrder.Post);
    topoSorted.forEach(node -> {
      final Optional<List<Edge>> inEdges = dag.getInEdges(node);
      if (!inEdges.isPresent()) {
        node.getAttributes().put(Attributes.placement, "transient");
      } else {
        if (inEdges.get().stream().filter(edge -> edge.getType() == Edge.Type.M2M).count() == 0) {
          node.getAttributes().put(Attributes.placement, "transient");
        } else {
          node.getAttributes().put(Attributes.placement, "reserved");
        }
      }
    });

    // After
    System.out.println("After DoFnOperator Placement");
    DAG.print(dag);
  }
}
