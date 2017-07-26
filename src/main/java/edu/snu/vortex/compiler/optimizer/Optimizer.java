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
package edu.snu.vortex.compiler.optimizer;

import edu.snu.vortex.common.dag.DAGBuilder;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.compiler.optimizer.passes.*;
import edu.snu.vortex.compiler.optimizer.passes.optimization.LoopOptimizations;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStage;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStageEdge;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.*;

/**
 * Optimizer class.
 */
public final class Optimizer {
  /**
   * Optimize function.
   * @param dag input DAG.
   * @param policyType type of the instantiation policy that we want to use to optimize the DAG.
   * @param dagDirectory directory to save the DAG information.
   * @return optimized DAG, tagged with attributes.
   * @throws Exception throws an exception if there is an exception.
   */
  public DAG<IRVertex, IREdge> optimize(final DAG<IRVertex, IREdge> dag, final PolicyType policyType,
                                        final String dagDirectory) throws Exception {
    if (policyType == null) {
      throw new RuntimeException("Policy has not been provided for the policyType");
    }
    return process(dag, POLICIES.get(policyType), dagDirectory);
  }

  /**
   * A recursive method to process each pass one-by-one to the given DAG.
   * @param dag DAG to process.
   * @param passes passes to apply.
   * @param dagDirectory directory to save the DAG information.
   * @return the processed DAG.
   * @throws Exception Exceptionso n the way.
   */
  private static DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag, final List<Pass> passes,
                                               final String dagDirectory) throws Exception {
    if (passes.isEmpty()) {
      return dag;
    } else {
      final DAG<IRVertex, IREdge> processedDAG = passes.get(0).process(dag);
      processedDAG.storeJSON(dagDirectory, "ir-after-" + passes.get(0).getClass().getSimpleName(),
          "DAG after optimization");
      return process(processedDAG, passes.subList(1, passes.size()), dagDirectory);
    }
  }

  /**
   * Enum for different types of instantiation policies.
   */
  public enum PolicyType {
    Default,
    Pado,
    Disaggregation,
    DataSkew,
  }

  /**
   * A HashMap to match each of instantiation policies with a combination of instantiation passes.
   * Each policies are run in the order with which they are defined.
   */
  private static final Map<PolicyType, List<Pass>> POLICIES = new HashMap<>();
  static {
    POLICIES.put(PolicyType.Default,
        Arrays.asList(
            new ParallelismPass() // Provides parallelism information.
        ));
    POLICIES.put(PolicyType.Pado,
        Arrays.asList(
            new ParallelismPass(), // Provides parallelism information.
            new LoopGroupingPass(),
            LoopOptimizations.getLoopFusionPass(),
            LoopOptimizations.getLoopInvariantCodeMotionPass(),
            new LoopUnrollingPass(), // Groups then unrolls loops. TODO #162: remove unrolling pt.
            new PadoVertexPass(), new PadoEdgePass() // Processes vertices and edges with Pado algorithm.
        ));
    POLICIES.put(PolicyType.Disaggregation,
        Arrays.asList(
            new ParallelismPass(), // Provides parallelism information.
            new LoopGroupingPass(),
            LoopOptimizations.getLoopFusionPass(),
            LoopOptimizations.getLoopInvariantCodeMotionPass(),
            new LoopUnrollingPass(), // Groups then unrolls loops. TODO #162: remove unrolling pt.
            new DisaggregationPass() // Processes vertices and edges with Disaggregation algorithm.
        ));
    POLICIES.put(PolicyType.DataSkew,
        Arrays.asList(
            new ParallelismPass(), // Provides parallelism information.
            new LoopGroupingPass(),
            LoopOptimizations.getLoopFusionPass(),
            LoopOptimizations.getLoopInvariantCodeMotionPass(),
            new LoopUnrollingPass(), // Groups then unrolls loops. TODO #162: remove unrolling pt.
            new DataSkewPass()
        ));
  }

  /**
   * A HashMap to convert string names for each policy type to receive as arguments.
   */
  public static final Map<String, PolicyType> POLICY_NAME = new HashMap<>();
  static {
    POLICY_NAME.put("default", PolicyType.Default);
    POLICY_NAME.put("pado", PolicyType.Pado);
    POLICY_NAME.put("disaggregation", PolicyType.Disaggregation);
    POLICY_NAME.put("dataskew", PolicyType.DataSkew);
  }

  /**
   * Dynamic optimization method to process the dag with an appropriate pass, decided by the stats.
   * @param originalPlan original physical execution plan.
   * @param metricData metric data and statistic information to decide which pass to perform.
   * @param dynamicOptimizationType type of dynamic optimization to perform.
   * @return processed DAG.
   */
  public static PhysicalPlan dynamicOptimization(final PhysicalPlan originalPlan,
                                                 final Map<String, ?> metricData,
                                                 final Attribute dynamicOptimizationType) {
    switch (dynamicOptimizationType) {
      case DataSkew:
        final DescriptiveStatistics stats = new DescriptiveStatistics();

        metricData.forEach((k, v) -> stats.addValue(((Long) v).doubleValue()));

        final double median = stats.getPercentile(50);
        final double q1 = stats.getPercentile(25);
        final double q3 = stats.getPercentile(75);

        final double interquartile = q3 - q1;
        final double outerfence = interquartile * interquartile * 2;

        final DAGBuilder<PhysicalStage, PhysicalStageEdge> physicalDAGBuilder =
            new DAGBuilder<>(originalPlan.getStageDAG());

        metricData.forEach((partitionId, partitionSize) -> {
          final String runtimeEdgeId = RuntimeIdGenerator.parsePartitionId(partitionId)[0];
          final DAG<PhysicalStage, PhysicalStageEdge> stageDAG = originalPlan.getStageDAG();
          final PhysicalStageEdge optimizationEdge = stageDAG.getVertices().stream()
              .flatMap(physicalStage -> stageDAG.getIncomingEdgesOf(physicalStage).stream())
              .filter(physicalStageEdge -> physicalStageEdge.getId().equals(runtimeEdgeId))
              .findFirst().orElseThrow(() ->
                  new RuntimeException("physical stage DAG doesn't contain this edge: " + runtimeEdgeId));

          final PhysicalStage optimizationStage = optimizationEdge.getDst();
          final PhysicalStageEdge postOptimizationEdge =
              originalPlan.getStageDAG().getOutgoingEdgesOf(optimizationStage).stream().findFirst().orElseThrow(() ->
                  new RuntimeException("Optimization stage must have at least one outgoing edge"));

          if (((Long) partitionSize).doubleValue() > median + outerfence) { // outlier.
            // TODO #362: handle outliers using the new method of observing hash histogram.
            postOptimizationEdge.getAttributes();
          }
        });

        return new PhysicalPlan(originalPlan.getId(), physicalDAGBuilder.build(), originalPlan.getTaskIRVertexMap());
      default:
        return originalPlan;
    }
  }
}
