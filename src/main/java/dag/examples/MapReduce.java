/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dag.examples;

import dag.*;
import dag.node.Node;

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

  private class Pair<K, V> {
    public K key;
    public V val;

    public Pair(final K key, final V val) {
      this.key = key;
      this.val = val;
    }
  }
}
