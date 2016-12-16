package dag.examples;

import dag.*;
import dag.node.Node;
import util.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class MapReduce {
  public static void main(final String[] args) {
    final EmptyDo<String, Pair<String, Integer>, Void> map = new EmptyDo<>("MapOperator");
    final EmptyDo<Pair<String, Iterable<Integer>>, String, Void> reduce = new EmptyDo<>("ReduceOperator");

    // Before
    final DAGBuilder builder = new DAGBuilder();
    builder.addNode(map);
    builder.addNode(reduce);
    builder.connectNodes(map, reduce, Edge.Type.M2M);
    final DAG dag = builder.build();
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
