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
package edu.snu.vortex.compiler.optimizer.examples;

import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.compiler.optimizer.Optimizer;

import java.util.List;

/**
 * A sample MapReduce application.
 */
public final class MapReduce {
  private MapReduce() {
  }

  public static void main(final String[] args) throws Exception {
    final Vertex source = new Vertex(new EmptyOperator("Source"));
    final Vertex map = new Vertex(new EmptyOperator("MapVertex"));
    final Vertex reduce = new Vertex(new EmptyOperator("ReduceVertex"));

    // Before
    final DAGBuilder builder = new DAGBuilder();
    builder.addVertex(source);
    builder.addVertex(map);
    builder.addVertex(reduce);
    builder.connectVertices(source, map, Edge.Type.OneToOne);
    builder.connectVertices(map, reduce, Edge.Type.ScatterGather);
    final DAG dag = builder.build();
    System.out.println("Before Optimization");
    System.out.println(dag);

    // Optimize
    final Optimizer optimizer = new Optimizer();
    final DAG optimizedDAG = optimizer.optimize(dag, Optimizer.PolicyType.Disaggregation);

    // After
    System.out.println("After Optimization");
    System.out.println(optimizedDAG);
  }

  /**
   * An empty operator.
   */
  private static class EmptyOperator implements Transform {
    private final String name;

    EmptyOperator(final String name) {
      this.name = name;
    }

    @Override
    public final String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(super.toString());
      sb.append(", name: ");
      sb.append(name);
      return sb.toString();
    }

    @Override
    public void prepare(final OutputCollector outputCollector) {
    }

    @Override
    public void onData(final List data, final int from) {
    }

    @Override
    public void close() {
    }
  }
}
