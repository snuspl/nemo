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

import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.executor.channel.LocalChannel;

import java.util.*;
import java.util.stream.IntStream;

/**
 * A simple engine that prints task outputs to stdout.
 */
public final class SimpleEngine {
  public void executePhysicalPlan(final PhysicalPlan physicalPlan) throws Exception {
    final Map<String, List<LocalChannel>> stageEdgeIdToChannels = new HashMap<>();

    physicalPlan.getTaskGroupsByStage().forEach(stage -> {
      stage.forEach(taskGroup -> {

        Set<Task> currentTaskSet = taskGroup.getTaskDAG().getRootVertices();
        while (currentTaskSet != null) {
          final List<Iterable<Element>> partitions = new ArrayList<>();

          // compute a partition in sequence
          Iterable<Element> data = null;
          Task lastTask = null;
          for (final Task task : currentTaskSet) {
            if (task instanceof BoundedSourceTask) {
              try {
                final BoundedSourceTask boundedSourceTask = (BoundedSourceTask) task;
                final Reader reader = boundedSourceTask.getReader();
                data = reader.read();
                System.out.println(" Output of {" + task.getTaskId() + "}: ");
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            } else if (task instanceof OperatorTask) {
              final OperatorTask operatorTask = (OperatorTask) task;
              final Transform transform = operatorTask.getTransform();
              if (lastTask == null) {
                final List<StageBoundaryEdgeInfo> stageBoundaryEdgeInfoList = taskGroup.getIncomingEdges();
                stageBoundaryEdgeInfoList.forEach(edge -> {
                  final String edgeId = edge.getStageBoundaryEdgeInfoId();
                  edge.getExternalEndpointVertexId();
                });
              } else {
                final Transform.Context transformContext = new ContextImpl(null);
                final OutputCollectorImpl outputCollector = new OutputCollectorImpl();
                transform.prepare(transformContext, outputCollector);
                transform.onData(data, lastTask.getRuntimeVertexId());
                transform.close();
                data = outputCollector.getOutputList();
              }

              if (finalTask && someOuterStageEdge) {
                outEdges.forEach(outEdge -> {
                  if (outEdge.getAttr(RuntimeAttribute.Key.SideInput) == RuntimeAttribute.SideInput) {
                    if (outDataPartitions.size() != 1) {
                      throw new RuntimeException("broadcast operator must match 1");
                    }
                    outDataPartitions.get(0).forEach(element ->
                        edgeIdToBroadcast.put(outEdge.getId(), element.getData()));
                  } else {
                    edgeIdToPartitions.put(outEdge.getId(), routePartitions(outDataPartitions, outEdge));
                  }
                });

              }

            } else {
              throw new UnsupportedOperationException(task.toString());
            }
            lastTask = task;
            System.out.println(" Output of {" + task.getTaskId() + "}: " +
                (data.toString().length() > 5000 ?
                    data.toString().substring(0, 5000) + "..." : data.toString()));
          }
        }

      });
    });

    System.out.println("Job completed.");
  }

  private List<Iterable<Element>> routePartitions(final List<Iterable<Element>> partitions,
                                                  final Edge edge) {
    final Edge.Type edgeType = edge.getType();
    if (edgeType == Edge.Type.OneToOne) {
      return partitions;
    } else if (edgeType == Edge.Type.Broadcast) {
      final List<Element> iterableList = new ArrayList<>();
      partitions.forEach(iterable -> iterable.forEach(iterableList::add));
      return Arrays.asList(iterableList);
    } else if (edgeType == Edge.Type.ScatterGather) {
      final int numOfDsts = partitions.size(); // Same as the number of current partitions
      final List<List<Element>> routedPartitions = new ArrayList<>(numOfDsts);
      IntStream.range(0, numOfDsts).forEach(x -> routedPartitions.add(new ArrayList<>()));

      // Hash-based routing
      partitions.forEach(partition -> {
        partition.forEach(element -> {
          final int dstIndex = Math.abs(element.getKey().hashCode() % numOfDsts);
          routedPartitions.get(dstIndex).add(element);
        });
      });

      // for some reason, Java requires the type to be explicit
      final List<Iterable<Element>> explicitlyIterables = new ArrayList<>();
      explicitlyIterables.addAll(routedPartitions);
      return explicitlyIterables;
    } else {
      throw new UnsupportedOperationException(edgeType + " is an unsupported type of edge.");
    }
  }
}
