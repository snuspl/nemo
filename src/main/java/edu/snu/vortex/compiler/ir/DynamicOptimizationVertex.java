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
package edu.snu.vortex.compiler.ir;

import edu.snu.vortex.common.dag.DAG;

import java.util.*;

/**
 * IRVertex that transforms input data, and collects statistics to send them to the optimizer for dynamic optimization.
 * Usage of this can be seen through {@link edu.snu.vortex.compiler.optimizer.passes.DataSkewPass}.
 */
public final class DynamicOptimizationVertex extends IRVertex {
  private final Map<String, List<Object>> metricData;
  // this DAG snapshot lets the vertex know the state of the DAG and where in the DAG it should optimize.
  private DAG<IRVertex, IREdge> dagSnapshot;

  /**
   * Constructor for dynamic optimization vertex.
   */
  public DynamicOptimizationVertex() {
    this.metricData = new HashMap<>();
    this.dagSnapshot = null;
  }

  @Override
  public DynamicOptimizationVertex getClone() {
    final DynamicOptimizationVertex that = new DynamicOptimizationVertex();
    that.setDAGSnapshot(dagSnapshot);
    IRVertex.copyAttributes(this, that);
    return that;
  }

  /**
   * @param dag DAG to set on the vertex.
   */
  public void setDAGSnapshot(final DAG<IRVertex, IREdge> dag) {
    this.dagSnapshot = dag;
  }

  /**
   * @return the DAG set to the vertex, or throws an exception otherwise.
   */
  public DAG<IRVertex, IREdge> getDAGSnapshot() {
    if (this.dagSnapshot == null) {
      throw new RuntimeException("DynamicOptimizationVertex must have been set with a DAG.");
    }
    return this.dagSnapshot;
  }

  /**
   * Method for accumulating metrics in the vertex.
   * @param key key of the metric data.
   * @param value value of the metric data.
   * @return whether or not it has been successfully accumulated.
   */
  public boolean accumulateMetrics(final String key, final Object value) {
    // TODO #315: collect metrics, optimization, resubmit DAG to runtime.
    metricData.putIfAbsent(key, new ArrayList<>());
    return metricData.get(key).add(value);
  }

  /**
   * Method for triggering dynamic optimization.
   */
  public void triggerDynamicOptimization() {
    // TODO #315: resubmitting DAG to runtime.
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(irVertexPropertiesToString());
    sb.append("}");
    return sb.toString();
  }
}
