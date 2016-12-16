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
package engine;

import dag.*;
import dag.node.*;
import dag.node.Do;
import org.apache.beam.sdk.values.KV;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SimpleEngine {

  public static void executeDAG(final DAG dag) throws Exception {
    final List<Node> topoSorted = new LinkedList<>();
    DAG.doDFS(dag, (node -> topoSorted.add(0, node)), DAG.VisitOrder.Post);

    final Map<String, List<Iterable>> edgeIdToData = new HashMap<>();
    final Map<String, Object> edgeIdToBroadcast = new HashMap<>();

    for (final Node node : topoSorted) {
      if (node instanceof Source) {
        final List<Source.Reader> readers = ((Source)node).getReaders(10); // 10 Bytes per Reader
        final List<Iterable> data = new ArrayList<>(readers.size());
        for (final Source.Reader reader : readers) {
          data.add(reader.read());
        }
        dag.getOutEdges(node).get().stream()
            .map(outEdge -> outEdge.getId())
            .forEach(id -> edgeIdToData.put(id, data));
      } else if (node instanceof Do) {
        final Do op = (Do)node;
        final Map<Object, Object> broadcastInput = new HashMap<>();
        dag.getInEdges(node).get().stream()
            .filter(inEdge -> inEdge.getSrc() instanceof Broadcast)
            .forEach(inEdge -> broadcastInput.put(((Broadcast)inEdge.getSrc()).getTag(), edgeIdToBroadcast.get(inEdge.getId())));
        final List<Iterable> mainInput = dag.getInEdges(node).get().stream()
            .filter(inEdge -> !(inEdge.getSrc() instanceof Broadcast))
            .map(inEdge -> edgeIdToData.get(inEdge.getId()))
            .findFirst()
            .get();
        final List<Iterable> output = mainInput.stream()
            .map(iterable -> op.compute(iterable, broadcastInput))
            .collect(Collectors.toList());

        if (dag.getOutEdges(node).isPresent()) { // TODO: Refactor
          edgeIdToData.put(getSingleEdgeId(dag, node, EdgeDirection.Out), output); // TODO: Support multi outedges
        }
      } else if (node instanceof GroupByKey) {
        final List<Iterable> data = shuffle(edgeIdToData.get(getSingleEdgeId(dag, node, EdgeDirection.In)));
        edgeIdToData.put(getSingleEdgeId(dag, node, EdgeDirection.Out), data);
      } else if (node instanceof Broadcast) {
        final List<Iterable> inEdgeData = edgeIdToData.get(getSingleEdgeId(dag, node, EdgeDirection.In));
        edgeIdToBroadcast.put(getSingleEdgeId(dag, node, EdgeDirection.Out), broadcast(inEdgeData));
      } else if (node instanceof Sink) {
        throw new UnsupportedOperationException();
      } else {
        throw new UnsupportedOperationException();
      }

      System.out.println("All Data after " + node.getId() + ": " + edgeIdToData);
    }
  }

  private enum EdgeDirection {
    In,
    Out
  }

  private static String getSingleEdgeId(final DAG dag, final Node node, final EdgeDirection ed) {
    final Optional<List<Edge>> optional = (ed == EdgeDirection.In) ? dag.getInEdges(node) : dag.getOutEdges(node);
    if (optional.isPresent()) {
      final List<Edge> edges = optional.get();
      if (edges.size() != 1) {
        throw new IllegalArgumentException();
      } else {
        return edges.get(0).getId();
      }
    } else {
      throw new IllegalArgumentException();
    }
  }


  private static Object broadcast(final List<Iterable> iterables) {
    return iterables.stream()
        .flatMap(iterable -> StreamSupport.stream(iterable.spliterator(), false))
        .collect(Collectors.toList());
  }


  private static List<Iterable> shuffle(final List<Iterable> data) {
    final HashMap<Integer, HashMap<Object, KV<Object, List>>> dstIdToCombined = new HashMap<>();
    final int numDest = 3;

    data.forEach(iterable -> iterable.forEach(element -> {
      final KV kv = (KV) element;
      final int dstId = kv.getKey().hashCode() % numDest;
      dstIdToCombined.putIfAbsent(dstId, new HashMap<>());
      final HashMap<Object, KV<Object, List>> combined = dstIdToCombined.get(dstId);
      combined.putIfAbsent(kv.getKey(), KV.of(kv.getKey(), new ArrayList()));
      combined.get(kv.getKey()).getValue().add(kv.getValue());
    }));

    return dstIdToCombined.values().stream()
        .map(map -> map.values().stream().collect(Collectors.toList()))
        .collect(Collectors.toList());
  }
}
