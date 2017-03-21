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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.compiler.ir.Reader;
import edu.snu.vortex.runtime.common.RuntimeAttributes;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.logical.*;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.exception.IllegalEdgeOperationException;
import edu.snu.vortex.runtime.exception.IllegalVertexOperationException;
import edu.snu.vortex.runtime.exception.PhysicalPlanGenerationException;
import edu.snu.vortex.runtime.exception.UnsupportedAttributeException;

import java.util.*;
import java.util.logging.Logger;

/**
 * Runtime Master.
 */
public final class RuntimeMaster {
  private static final Logger LOG = Logger.getLogger(RuntimeMaster.class.getName());

  /**
   * Submits the {@link ExecutionPlan} to Runtime.
   * @param executionPlan to execute.
   */
  public void execute(final ExecutionPlan executionPlan) {
    generatePhysicalPlan(executionPlan);
  }

  private Map<String, List<TaskGroup>> generatePhysicalPlan(final ExecutionPlan executionPlan) {
    final Map<String, List<TaskGroup>> physicalExecutionPlan = new HashMap<>();

    try {
      // a temporary map to easily find the runtime vertex given the id.
      final Map<String, RuntimeVertex> runtimeVertexMap = new HashMap<>();

      for (final RuntimeStage runtimeStage : executionPlan.getRuntimeStages()) {
        final List<RuntimeVertex> runtimeVertices = runtimeStage.getRuntimeVertices();
        final Map<String, Set<RuntimeEdge>> incomingEdges = runtimeStage.getStageIncomingEdges();
        final Map<String, Set<RuntimeEdge>> outgoingEdges = runtimeStage.getStageOutgoingEdges();

        // TODO #103: Integrity check in execution plan.
        // This code simply assumes that all vertices follow the first vertex's parallelism.
        final int parallelism = (int) runtimeVertices.get(0).getVertexAttributes()
            .get(RuntimeAttributes.RuntimeVertexAttribute.PARALLELISM);
        final List<TaskGroup> taskGroupList = new ArrayList<>(parallelism);

        for (int taskGroupIdx = 0; taskGroupIdx < parallelism; taskGroupIdx++) {
          final TaskGroup newTaskGroup =
              new TaskGroup(RuntimeIdGenerator.generateTaskGroupId(), incomingEdges, outgoingEdges);

          Task newTaskToAdd;
          for (final RuntimeVertex vertex : runtimeVertices) {
            runtimeVertexMap.put(vertex.getId(), vertex);
            if (vertex instanceof RuntimeBoundedSourceVertex) {
              final RuntimeBoundedSourceVertex boundedSourceVertex = (RuntimeBoundedSourceVertex) vertex;

              // TODO #104: Change the interface of getReaders() in SourceVertex.
              // This code assumes that the issue #104 has been resolved.
              final List<Reader> readers = boundedSourceVertex.getBoundedSourceVertex().getReaders(parallelism);
              newTaskToAdd = new BoundedSourceTask(RuntimeIdGenerator.generateTaskId(),
                  boundedSourceVertex.getId(), readers.get(taskGroupIdx));
            } else if (vertex instanceof RuntimeOperatorVertex) {
              final RuntimeOperatorVertex operatorVertex = (RuntimeOperatorVertex) vertex;
              newTaskToAdd = new OperatorTask(RuntimeIdGenerator.generateTaskId(), operatorVertex.getId(),
                  operatorVertex.getOperatorVertex().getTransform());
            } else {
              throw new IllegalVertexOperationException("This vertex type is not supported");
            }
            vertex.addTask(newTaskToAdd);
            newTaskGroup.addTask(newTaskToAdd);
          }
          taskGroupList.add(newTaskGroup);
        }

        // Now that all tasks have been created, connect the internal edges in the task group.
        // Notice that it suffices to iterate over only the internalInEdges.
        final Map<String, Set<String>> internalInEdges = runtimeStage.getInternalInEdges();
        for (int taskGroupIdx = 0; taskGroupIdx < parallelism; taskGroupIdx++) {
          for (final Map.Entry<String, Set<String>> entry : internalInEdges.entrySet()) {
            for (final String srcVertexId : entry.getValue()) {
              taskGroupList.get(taskGroupIdx)
                  .connectTasks(runtimeVertexMap.get(srcVertexId).getTaskList().get(taskGroupIdx),
                      runtimeVertexMap.get(entry.getKey()).getTaskList().get(taskGroupIdx));
            }
          }
        }

        // Now that all tasks have been created, connect the external outgoing edges from/to the task group.
        // Notice that it suffices to iterate over only the outgoingEdges.
        for (final Map.Entry<String, Set<RuntimeEdge>> entry : outgoingEdges.entrySet()) {
          for (final RuntimeEdge edge : entry.getValue()) {
            // skip if this edge has already been looked at by previous stages.
            if (edge.getChannelInfos().isEmpty()) {
              final Map<RuntimeAttributes.RuntimeEdgeAttribute, Object> edgeAttributes = edge.getEdgeAttributes();
              final RuntimeAttributes.Channel channelType =
                  (RuntimeAttributes.Channel) edgeAttributes.get(RuntimeAttributes.RuntimeEdgeAttribute.CHANNEL);
              final RuntimeAttributes.CommPattern commPattern = (RuntimeAttributes.CommPattern)
                  edgeAttributes.get(RuntimeAttributes.RuntimeEdgeAttribute.COMM_PATTERN);

              final List<? extends Task> srcTaskList = edge.getSrcRuntimeVertex().getTaskList();
              final List<? extends Task> dstTaskList = edge.getDstRuntimeVertex().getTaskList();

              switch (commPattern) {
              case ONE_TO_ONE:
                if (srcTaskList.size() == dstTaskList.size()) {
                  for (int taskIdx = 0; taskIdx < srcTaskList.size(); taskIdx++) {
                    edge.addChannelInfo(new ChannelInfo(srcTaskList.get(taskIdx).getTaskId(),
                        dstTaskList.get(taskIdx).getTaskId(), channelType));
                  }
                } else {
                  throw new IllegalEdgeOperationException(
                      "there must be an equal number of src/dst tasks for this edge type");
                }
                break;
              case SCATTER_GATHER:
                for (final Task srcTask : srcTaskList) {
                  for (final Task dstTask : dstTaskList) {
                    edge.addChannelInfo(new ChannelInfo(srcTask.getTaskId(), dstTask.getTaskId(), channelType));
                  }
                }
                break;
              case BROADCAST:
                throw new UnsupportedAttributeException("\"BROADCAST\" edge attribute is not yet supported");
              default:
                throw new UnsupportedAttributeException("this edge attribute is not yet supported");
              }
            }
          }
        }

        physicalExecutionPlan.put(runtimeStage.getStageId(), taskGroupList);
      }
    } catch (final Exception e) {
      throw new PhysicalPlanGenerationException(e.getMessage());
    }
    return physicalExecutionPlan;
  }
}
