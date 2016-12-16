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
package dag;

import dag.node.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Public API
 */
public class DAGBuilder {
  private HashMap<String, List<Edge>> id2inEdges = new HashMap<>();
  private HashMap<String, List<Edge>> id2outEdges = new HashMap<>();
  private List<Node> nodes = new ArrayList<>();

  public DAGBuilder() {
  }

  public void addNode(final Node node) {
    nodes.add(node);
  }

  public <I, O> Edge<I, O> connectNodes(final Node<?, I> src, final Node<O, ?> dst, final Edge.Type type) {
    final Edge<I, O> edge = new Edge<>(type, src, dst);
    addToEdgeList(id2inEdges, dst.getId(), edge);
    addToEdgeList(id2outEdges, src.getId(), edge);
    return edge;
  }

  private void addToEdgeList(final HashMap<String, List<Edge>> map, final String id, final Edge edge) {
    if (map.containsKey(id)) {
      map.get(id).add(edge);
    } else {
      final List<Edge> inEdges = new ArrayList<>(1);
      inEdges.add(edge);
      map.put(id, inEdges);
    }
  }

  public DAG build() {
    // TODO: Check graph's correctness before returning
    final List<Source> sources = nodes.stream()
        .filter(node -> !id2inEdges.containsKey(node.getId()))
        .map(node -> (Source)node)
        .collect(Collectors.toList());
    return new DAG(sources, id2inEdges, id2outEdges);
  }
}
