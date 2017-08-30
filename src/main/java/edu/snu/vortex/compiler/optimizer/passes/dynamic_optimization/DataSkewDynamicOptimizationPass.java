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
package edu.snu.vortex.compiler.optimizer.passes.dynamic_optimization;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.common.dag.DAGBuilder;
import edu.snu.vortex.compiler.exception.DynamicOptimizationException;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStage;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.executor.data.HashRange;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Dynamic optimization pass for handling data skew.
 */
public final class DataSkewDynamicOptimizationPass implements DynamicOptimizationPass<Long> {
  @Override
  public PhysicalPlan process(final PhysicalPlan originalPlan, final Map<String, List<Long>> metricData) {
    // Builder to create new stages.
    final DAGBuilder<PhysicalStage, PhysicalStageEdge> physicalDAGBuilder =
        new DAGBuilder<>(originalPlan.getStageDAG());

    // NOTE: metricData is made up of a map of partitionId to blockSizes.
    // Count the hash range.
    final int hashRangeCount = metricData.values().stream().findFirst().orElseThrow(() ->
        new DynamicOptimizationException("no valid metric data.")).size();

    // Aggregate metric data. TODO #???: aggregate metric data beforehand.
    final List<Long> aggregatedMetricData = new ArrayList<>(hashRangeCount);
    IntStream.range(0, hashRangeCount).forEach(i ->
        aggregatedMetricData.add(i, metricData.values().stream().mapToLong(lst -> lst.get(i)).sum()));

    // get edges to optimize
    final Stream<String> optimizationEdgeIds = metricData.keySet().stream().map(partitionId ->
        RuntimeIdGenerator.parsePartitionId(partitionId)[0]);
    final DAG<PhysicalStage, PhysicalStageEdge> stageDAG = originalPlan.getStageDAG();
    final Stream<PhysicalStageEdge> optimizationEdges = stageDAG.getVertices().stream()
        .flatMap(physicalStage -> stageDAG.getIncomingEdgesOf(physicalStage).stream())
        .filter(physicalStageEdge -> optimizationEdgeIds.anyMatch(optimizationEdgeId ->
            optimizationEdgeId.equals(physicalStageEdge.getId())));

    // Get number of evaluators of the next stage
    final Integer taskGroupListSize = optimizationEdges.findFirst().orElseThrow(() ->
        new RuntimeException("optimization edges is empty")).getDst().getTaskGroupList().size();

    // Do the optimization using the information derived above.
    final Long totalSize = aggregatedMetricData.stream().mapToLong(n -> n).sum();
    final Long idealSizePerTaskGroup = totalSize / taskGroupListSize;

    // find HashRanges to apply
    final List<HashRange> hashRanges = new ArrayList<>(taskGroupListSize);
    int startingHashValue = 0;
    int finishingHashValue = 1;
    for (int i = 1; i <= taskGroupListSize; i++) {
      if (i != taskGroupListSize) {
        final Long endPoint = idealSizePerTaskGroup * i;
        // find the point
        while (sumBetween(aggregatedMetricData, startingHashValue, finishingHashValue) < endPoint) {
          finishingHashValue++;
        }
        // assign appropriately
        if (sumBetween(aggregatedMetricData, startingHashValue, finishingHashValue) - endPoint
            < endPoint - sumBetween(aggregatedMetricData, startingHashValue, finishingHashValue - 1)) {
          hashRanges.add(i - 1, HashRange.of(startingHashValue, finishingHashValue));
          startingHashValue = finishingHashValue;
        } else {
          hashRanges.add(i - 1, HashRange.of(startingHashValue, finishingHashValue - 1));
          startingHashValue = finishingHashValue - 1;
        }
      } else { // last one
        hashRanges.add(i - 1, HashRange.of(startingHashValue, hashRangeCount));
      }
    }

    // Assign the hash value range to each receiving task group.
    optimizationEdges.forEach(optimizationEdge -> {
      final List<TaskGroup> taskGroups = optimizationEdge.getDst().getTaskGroupList();
      final Map<String, HashRange> taskGroupIdToHashRangeMap = optimizationEdge.getTaskGroupIdToHashRangeMap();
      IntStream.range(0, taskGroupListSize).forEach(i ->
          // Update the information.
          taskGroupIdToHashRangeMap.put(taskGroups.get(i).getTaskGroupId(), hashRanges.get(i)));
    });

    return new PhysicalPlan(originalPlan.getId(), physicalDAGBuilder.build(), originalPlan.getTaskIRVertexMap());
  }

  private Long sumBetween(final List<Long> listOfLong, final int startInclusive, final int endExclusive) {
    return listOfLong.subList(startInclusive, endExclusive).stream().mapToLong(n -> n).sum();
  }
}
