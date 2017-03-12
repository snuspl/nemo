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
 * A simple engine that prints operator outputs to stdout.
 */
public final class SimpleEngine {
  public void executeDAG(final DAG dag) throws Exception {
    final Map<String, List<Iterable>> edgeIdToData = new HashMap<>();

    final List<Vertex> topoSorted = new LinkedList<>();
    dag.doTopological(node -> topoSorted.add(node));
    for (final Vertex vertex : topoSorted) {
      if (vertex instanceof SourceVertex) {
        final SourceVertex sourceVertex = (SourceVertex) vertex;
        final List<Reader> readers = sourceVertex.getReaders(10); // 10 Bytes per BoundedSourceReader
        final List<Iterable> data = new ArrayList<>(readers.size());
        for (final Reader reader : readers) {
          data.add(reader.read());
        }
        dag.getOutEdgesOf(vertex).get().stream()
            .map(outEdge -> outEdge.getId())
            .forEach(id -> edgeIdToData.put(id, data));
      } else if (vertex instanceof OperatorVertex) {
        final OperatorVertex operatorVertex = (OperatorVertex) vertex;
        final Transform transform = operatorVertex.getTransform();
        final List<Edge> inEdges = dag.getInEdgesOf(vertex).get(); // must be at least one edge
        final List<Edge> outEdges = dag.getOutEdgesOf(vertex).orElse(new ArrayList<>(0)); // empty lists for sinks

        IntStream.range(0, inEdges.size())
            .forEach(i -> {
              // Process each input edge
              final Edge inEdge = inEdges.get(i);
              final Iterable inData = edgeIdToData.get(inEdge.getId());

              final OutputCollectorImpl outputCollector = new OutputCollectorImpl();
              transform.prepare(outputCollector);
              transform.onData(dataContext);
              transform.close();

              // Save the results to each output edge
              final HashMap<Integer, List> outputMap = outputCollector.getOutputList();
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

  private void partition(final Element element) {
    final int numOfDsts = outputCollector.getDstOperatorIds().size();
    final List<List<KV>> dsts = new ArrayList<>(numOfDsts);
    IntStream.range(0, numOfDsts).forEach(x -> dsts.add(new ArrayList<>()));
    data.forEach(element -> {
      final KV kv = (KV) element;
      final int dstIndex = Math.abs(kv.getKey().hashCode() % numOfDsts);
      dsts.get(dstIndex).add(kv);
    });
    IntStream.range(0, numOfDsts).forEach(dstIndex -> outputCollector.emit(dstIndex, dsts.get(dstIndex)));

  }
}
