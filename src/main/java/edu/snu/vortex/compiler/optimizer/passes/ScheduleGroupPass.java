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
package edu.snu.vortex.compiler.optimizer.passes;

import com.google.common.collect.Lists;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;

import java.util.*;
import java.util.stream.Collectors;

import static edu.snu.vortex.compiler.ir.attribute.Attribute.IntegerKey.ScheduleGroupId;
import static edu.snu.vortex.compiler.ir.attribute.Attribute.IntegerKey.StageId;

/**
 * A pass for assigning each stages in schedule groups.
 * We traverse the DAG topologically to find the dependency information between stages and number them appropriately
 * to give correct order or schedule groups.
 */
public final class ScheduleGroupPass implements StaticOptimizationPass {
  @Override
  public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) {
    final int initialScheduleGroup = 0;

    // We assume that the input dag is tagged with stage ids.
    if (dag.getVertices().stream().anyMatch(irVertex -> irVertex.getAttr(StageId) == null)) {
      throw new RuntimeException("There exists an IR vertex going through ScheduleGroupPass "
          + "without stage id tagged.");
    }

    // Map of stage id to the stage ids that it depends on.
    final Map<Integer, Set<Integer>> dependentStagesMap = new HashMap<>();
    dag.topologicalDo(irVertex -> {
      final Integer currentStageId = irVertex.getAttr(StageId);
      dependentStagesMap.putIfAbsent(currentStageId, new HashSet<>());
      // while traversing, we find the stages that point to the current stage and add them to the list.
      dag.getIncomingEdgesOf(irVertex).stream()
          .map(IREdge::getSrc)
          .map(vertex -> vertex.getAttr(StageId))
          .filter(n -> !n.equals(currentStageId))
          .forEach(n -> dependentStagesMap.get(currentStageId).add(n));
    });

    // Map to put our results in.
    final Map<Integer, Integer> stageIdToScheduleGroupIdMap = new HashMap<>();

    // Calculate schedule group number of each stages step by step
    while (stageIdToScheduleGroupIdMap.size() < dependentStagesMap.size()) {
      // This is to ensure that each iteration is making progress.
      // We ensure that the stageIdToScheduleGroupIdMap is increasing in size in each iteration.
      final Integer previousSize = stageIdToScheduleGroupIdMap.size();
      dependentStagesMap.forEach((stageId, dependentStages) -> {
        if (!stageIdToScheduleGroupIdMap.keySet().contains(stageId)
            && dependentStages.size() == initialScheduleGroup) { // initial source stages
          // initial source stages are indexed with schedule group 0.
          stageIdToScheduleGroupIdMap.put(stageId, initialScheduleGroup);
        } else if (!stageIdToScheduleGroupIdMap.keySet().contains(stageId)
            && dependentStages.stream().allMatch(stageIdToScheduleGroupIdMap::containsKey)) { // next stages
          // We find the maximum schedule group index from previous stages, and index current stage with that number +1.
          final Integer maxDependentSchedulerGroup =
              dependentStages.stream()
                  .mapToInt(stageIdToScheduleGroupIdMap::get)
                  .max().orElseThrow(() ->
                    new RuntimeException("A stage that is not a source stage much have dependent stages"));
          stageIdToScheduleGroupIdMap.put(stageId, maxDependentSchedulerGroup + 1);
        }
      });
      if (previousSize == stageIdToScheduleGroupIdMap.size()) {
        throw new RuntimeException("Iteration for indexing schedule groups in "
            + ScheduleGroupPass.class.getSimpleName() + " is not making progress");
      }
    }

    // Reverse topologically traverse and match schedule group ids for those that have push edges in between
    Lists.reverse(dag.getTopologicalSort()).forEach(v -> {
      // get the destination vertices of the edges that are marked as push
      final List<IRVertex> pushConnectedVertices = dag.getOutgoingEdgesOf(v).stream()
          .filter(e -> e.getAttr(Attribute.Key.ChannelTransferPolicy) == Attribute.Push)
          .map(IREdge::getDst)
          .collect(Collectors.toList());
      if (!pushConnectedVertices.isEmpty()) { // if we need to do something,
        // we find the min value of the destination schedule groups.
        final Integer newSchedulerGroupId = pushConnectedVertices.stream()
            .mapToInt(irVertex -> stageIdToScheduleGroupIdMap.get(irVertex.getAttr(StageId)))
            .min().orElseThrow(() -> new RuntimeException("a list was not empty, but produced an empty result"));
        // overwrite
        stageIdToScheduleGroupIdMap.replace(v.getAttr(StageId), newSchedulerGroupId);
      }
    });

    // do the tagging
    dag.topologicalDo(irVertex ->
        irVertex.setAttr(ScheduleGroupId, stageIdToScheduleGroupIdMap.get(irVertex.getAttr(StageId))));

    return dag;
  }
}
