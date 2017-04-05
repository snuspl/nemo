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

public final class SimpleRuntime {

  /**
   * WARNING: Because the current physical plan is missing some critical information,
   * I used hacks to make this work at least with a simple MapReduce application.
   * Notice that a slight variation in the application will make the code fail,
   * and we need to eventually fix the issues in a more proper way.
   * Please also refer to SimpleEngineBackup, which do not have these issues.
   *
   * Hack #1
   * The dependency information between tasks in a stage is missing,
   * so I just assumed that a stage is a sequence of tasks that only have 0 or 1 child/parent.
   *
   * Hack #2
   * The information on which task in a taskgroup is connected to other stages is missing.
   * As a workaround, I just assumed that the first task is connected to the incoming edges,
   * and the last task is connected to the outgoing edges.
   */
  public void executePhysicalPlan(final PhysicalPlan physicalPlan) throws Exception {
    final Map<String, List<LocalChannel>> edgeIdToChannels = new HashMap<>();

    physicalPlan.getTaskGroupsByStage().forEach(stage -> {
      stage.forEach(taskGroup -> {

        Set<Task> currentTaskSet = taskGroup.getTaskDAG().getRootVertices();
        while (currentTaskSet != null) {

          // compute tasks in a taskgroup
          Iterable<Element> data = null;
          Task lastTask = null;
          int taskIndex = 0;
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
              if (taskIndex == 0) {
                final List<StageBoundaryEdgeInfo> inEdges = taskGroup.getIncomingEdges();
                if (inEdges.size() > 1) {
                  throw new UnsupportedOperationException("Multi inedge not yet supported");
                }
                final StageBoundaryEdgeInfo inEdge = inEdges.get(0);
                data = edgeIdToChannels.get(inEdge.getStageBoundaryEdgeInfoId()).get(task.getIndex()).read();



              } else {
                final Transform.Context transformContext = new ContextImpl(null);
                final OutputCollectorImpl outputCollector = new OutputCollectorImpl();
                transform.prepare(transformContext, outputCollector);
                transform.onData(data, lastTask.getRuntimeVertexId());
                transform.close();
                data = outputCollector.getOutputList();
              }
            } else {
              throw new UnsupportedOperationException(task.toString());
            }

            // If it is the final task in a stage
            if (taskIndex == currentTaskSet.size()-1) {
              final List<StageBoundaryEdgeInfo> outEdges = taskGroup.getOutgoingEdges();
              if (outEdges.size() > 1) {
                throw new UnsupportedOperationException("Multi outedge not yet supported");
              }
              final StageBoundaryEdgeInfo outEdge = outEdges.get(0);
              writeToChannels(task.getIndex(), edgeIdToChannels, outEdge, data);
            }

            // Update some hacky variables
            taskIndex++;
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

  private void writeToChannels(final int srcTaskIndex,
                               final Map<String, List<LocalChannel>> edgeIdToChannels,
                               final StageBoundaryEdgeInfo edge,
                               final Iterable<Element> data) {
    final int dstParallelism = edge.getExternalEndpointVertexAttr().get(RuntimeAttribute.IntegerKey.Parallelism);


    edgeIdToChannels.putIfAbsent(edge.getStageBoundaryEdgeInfoId(), new ArrayList<>(dstParallelism));

    final List<LocalChannel> dstChannels = edgeIdToChannels.get(edge.getStageBoundaryEdgeInfoId());
    if (dstChannels == null) {
      IntStream.range(0, dstParallelism).map(x -> routedPartitions.add(new ArrayList<>()));

    }

    final RuntimeAttribute attribute = edge.getEdgeAttributes().get(RuntimeAttribute.Key.CommPattern);
    switch (attribute) {
      case OneToOne:
        dstChannels.get(srcTaskIndex).write(data);
        break;
      case Broadcast:
        dstChannels.forEach(chan -> chan.write(data));
        break;
      case ScatterGather:
        if (edge.getEdgeAttributes().get(RuntimeAttribute.Key.Partition) == RuntimeAttribute.Hash) {
          final List<List<Element>> routedPartitions = new ArrayList<>(dstParallelism);
          IntStream.range(0, dstParallelism).forEach(x -> routedPartitions.add(new ArrayList<>()));
          data.forEach(element -> {
            final int dstIndex = Math.abs(element.getKey().hashCode() % dstParallelism);
            routedPartitions.get(dstIndex).add(element);
          });
          IntStream.range(0, dstParallelism).forEach(x -> dstChannels.get(x).write(routedPartitions.get(x)));
        } else {
          throw new UnsupportedOperationException(edge.toString());
        }
      default:
        throw new UnsupportedOperationException(edge.toString());
    }
  }
}
