/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.engine;

import edu.snu.vortex.compiler.ir.*;

import java.util.*;
import java.util.stream.IntStream;

/**
 * A simple engine that prints intermediate results to stdout.
 */
public final class SimpleEngine {
  public void executeDAG(final DAG dag) throws Exception {
    final List<Vertex> topoSorted = new LinkedList<>();
    dag.doTopological(node -> topoSorted.add(node));

    final Map<String, List<Iterable>> edgeIdToData = new HashMap<>();
    final Map<String, Object> edgeIdToBroadcast = new HashMap<>();

    for (final Vertex vertex : topoSorted) {
      if (vertex instanceof Source) {
        /*
        final List<Source.Reader> readers = ((Source) vertex).getReaders(10); // 10 Bytes per Reader
        final List<Iterable> data = new ArrayList<>(readers.size());
        for (final Source.Reader reader : readers) {
          data.add(reader.read());
        }
        dag.getOutEdgesOf(vertex).get().stream()
            .map(outEdge -> outEdge.getId())
            .forEach(id -> edgeIdToData.put(id, data));
            */
      } else if (vertex instanceof Vertex) {
        final List<Edge> inEdges = dag.getInEdgesOf(vertex).get(); // must be at least one edge
        final List<Edge> outEdges = dag.getOutEdgesOf(vertex).orElse(new ArrayList<>(0)); // empty lists for sinks
        final Operator operator = vertex.getOperator();

        IntStream.range(0, inEdges.size())
            .forEach(i -> {
              // Process each input edge
              // TODO: Handle broadcasts differently? // Partitioning? (Number of input/outputs)
              final Edge inEdge = inEdges.get(i);
              final Iterable inData = edgeIdToData.get(inEdge.getId());
              operator.onData(dataContext);

              // Save the results to each output edge
              final HashMap<Integer, List> outputMap = dataContext.getOutputs();
              IntStream.range(0, outEdges.size()).forEach(j -> {
                final Edge outEdge = outEdges.get(j);
                final List outData = outputMap.get(j);
                edgeIdToData.put(outEdge.getId(), outData);
              });
              System.out.println("Output of vertex " + vertex.getId() + " / index" + i + ": " + outputMap);
            });
      } else {
        throw new UnsupportedOperationException(vertex.toString());
      }
    }

    System.out.println("## Job completed.");
  }

  private enum EdgeDirection {
    In,
    Out
  }

  private String getSingleEdgeId(final DAG dag, final Vertex node, final EdgeDirection ed) {
    final Optional<List<Edge>> optional = (ed == EdgeDirection.In) ? dag.getInEdgesOf(node) : dag.getOutEdgesOf(node);
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

}
