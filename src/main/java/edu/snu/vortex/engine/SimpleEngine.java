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
 * Simply prints out intermediate results.
 */
public final class SimpleEngine {
  public void executeDAG(final DAG dag) throws Exception {
    final List<Operator> topoSorted = new LinkedList<>();
    dag.doTopological(node -> topoSorted.add(node));

    final Map<String, List<Iterable>> edgeIdToData = new HashMap<>();
    final Map<String, Object> edgeIdToBroadcast = new HashMap<>();

    for (final Operator operator : topoSorted) {
      if (operator instanceof Source) {
        /*
        final List<Source.Reader> readers = ((Source) operator).getReaders(10); // 10 Bytes per Reader
        final List<Iterable> data = new ArrayList<>(readers.size());
        for (final Source.Reader reader : readers) {
          data.add(reader.read());
        }
        dag.getOutEdgesOf(operator).get().stream()
            .map(outEdge -> outEdge.getId())
            .forEach(id -> edgeIdToData.put(id, data));
            */
      } else if (operator instanceof Operator) {
        final List<Edge> inEdges = dag.getInEdgesOf(operator).get(); // must be at least one edge
        final List<Edge> outEdges = dag.getOutEdgesOf(operator).orElse(new ArrayList<>(0)); // empty lists for sinks
        final UserDefinedFunction udf = operator.getUDF();

        // TODO: do this on outEdges
        inEdges.forEach(inEdge -> {
          if (inEdge.getType() == Edge.Type.OneToOne) {

          } else if () {

          } else if () {

          } else {
            throw new UnsupportedOperationException(inEdge.getType().toString());
          }

        });

        IntStream.range(0, inEdges.size())
            .forEach(i -> {
              // Process each input edge
              // TODO: Handle broadcasts differently? // Partitioning? (Number of input/outputs)
              final Edge inEdge = inEdges.get(i);
              final Iterable inData = edgeIdToData.get(inEdge.getId());
              final DataContext dataContext = new DataContext(inData, i, ou) {
                @Override
                public List getBroadcastedData() {
                  return null;
                }
              };
              udf.onData(dataContext);

              // Save the results to each output edge
              final HashMap<Integer, List> outputMap = dataContext.getOutputs();
              IntStream.range(0, outEdges.size()).forEach(j -> {
                final Edge outEdge = outEdges.get(j);
                final List outData = outputMap.get(j);
                edgeIdToData.put(outEdge.getId(), outData);
              });
              System.out.println("Output of operator " + operator.getId() + " / index" + i + ": " + outputMap);
            });
      } else {
        throw new UnsupportedOperationException(operator.toString());
      }
    }

    System.out.println("## Job completed.");
  }

  private enum EdgeDirection {
    In,
    Out
  }

  private String getSingleEdgeId(final DAG dag, final Operator node, final EdgeDirection ed) {
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
