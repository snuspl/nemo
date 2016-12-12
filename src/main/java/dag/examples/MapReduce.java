package dag.examples;

import dag.*;
import util.Pair;

import java.util.LinkedList;
import java.util.List;

public class MapReduce {
  public static void main(final String[] args) {
    final EmptyOperator<String, Pair<String, Integer>> mapOp = new EmptyOperator<>("MapOperator");
    final EmptyOperator<Pair<String, Iterable<Integer>>, String> reduceOp = new EmptyOperator<>("ReduceOperator");
    final DAGBuilder gb = new DAGBuilder();

    final InternalNode<String, Pair<String, Integer>> mapNode = gb.createNode(mapOp);
    final InternalNode<Pair<String, Iterable<Integer>>, String> reduceNode = gb.createNode(reduceOp);

    // Before
    gb.connectNodes(mapNode, reduceNode, Edge.Type.M2M);

    final DAG DAG = gb.build();
    System.out.println("Before DoFnOperator Placement");
    DAG.print(DAG);

    // Optimize
    final List<InternalNode> topoSorted = new LinkedList<>();
    DAG.doDFS(DAG, (node -> topoSorted.add(0, node)), dag.DAG.VisitOrder.Post);
    topoSorted.forEach(node -> {
      if (node.isSource()) {
        node.getAttributes().put(Attributes.placement, "transient");
      } else {
        final List<Edge> inEdges = node.getInEdges();
        if (inEdges.stream().filter(edge -> edge.getType() == Edge.Type.M2M).count() == 0) {
          node.getAttributes().put(Attributes.placement, "transient");
        } else {
          node.getAttributes().put(Attributes.placement, "reserved");
        }
      }
    });

    // After
    System.out.println("After DoFnOperator Placement");
    DAG.print(DAG);
  }
}
