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

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.Reader;
import edu.snu.vortex.compiler.ir.Transform;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.executor.channel.LocalChannel;
import edu.snu.vortex.utils.DAG;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Simple Runtime that prints intermediate results to stdout.
 */
public final class SimpleRuntime {
  private static final String HACK_DUMMY_CHAND_ID = "HACK";

  /**
   * WARNING: Because the current physical plan is missing some critical information,
   * I used hacks to make this work at least with the Beam applications we currently have.
   * Nevertheless, a slight variation in the applications will make the code fail
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
   *
   * @param physicalPlan Physical Plan.
   * @throws Exception during execution.
   */
  public void executePhysicalPlan(final PhysicalPlan physicalPlan) throws Exception {
    final Map<String, List<LocalChannel>> edgeIdToChannels = new HashMap<>();

    physicalPlan.getTaskGroupsByStage().forEach(stage -> {
      stage.forEach(taskGroup -> {
        final DAG<Task> taskDAG = taskGroup.getTaskDAG();

        // compute tasks in a taskgroup, supposedly 'rootVertices' at a time
        Iterable<Element> data = null;
        Set<Task> currentTaskSet = taskDAG.getRootVertices();
        while (!currentTaskSet.isEmpty()) {
          for (final Task task : currentTaskSet) {
            final String vertexId = task.getRuntimeVertexId();

            if (task instanceof BoundedSourceTask) {
              try {
                final BoundedSourceTask boundedSourceTask = (BoundedSourceTask) task;
                final Reader reader = boundedSourceTask.getReader();
                data = reader.read();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            } else if (task instanceof OperatorTask) {

              // It the current task has any incoming edges, it reads data from the channels associated to the edges.
              // After that, it applies its transform function to the data read.
              final List<StageBoundaryEdgeInfo> inEdges = taskGroup.getIncomingEdges()
                  .stream().filter(edge -> edge.getExternalEndpointVertexId().equals(vertexId))
                  .collect(Collectors.toList());
              if (inEdges.size() > 1) {
                throw new UnsupportedOperationException("Multi inedge not yet supported");
              } else if (inEdges.size() == 1) { // We fetch 'data' from the incoming stage
                final StageBoundaryEdgeInfo inEdge = inEdges.get(0);
                data = edgeIdToChannels.get(inEdge.getStageBoundaryEdgeInfoId()).get(task.getIndex()).read();
                System.out.println("A task (id: " + task.getTaskId() + ") reads data");
              } else {
                System.out.println("A task (id: " + task.getTaskId() + ") doesn't read data");
              }

              final OperatorTask operatorTask = (OperatorTask) task;
              final Transform transform = operatorTask.getTransform();
              final Transform.Context transformContext = new ContextImpl(new HashMap<>()); // fix empty map
              final OutputCollectorImpl outputCollector = new OutputCollectorImpl();
              transform.prepare(transformContext, outputCollector);
              System.out.println(task.getTaskId() + ": " + data.toString());
              transform.onData(data, null); // fix null
              transform.close();
              data = outputCollector.getOutputList();
              System.out.println(task.getTaskId() + ": " + data.toString());

            } else {
              throw new UnsupportedOperationException(task.toString());
            }

            System.out.println(" Output of {" + task.getTaskId() + "}: " +
                (data.toString().length() > 5000 ?
                    data.toString().substring(0, 5000) + "..." : data.toString()));

            // If the current task has any outgoing edges, it writes data to channels associated to the edges.
            final List<StageBoundaryEdgeInfo> outEdges = taskGroup.getOutgoingEdges()
                .stream().filter(outEdge -> outEdge.getExternalEndpointVertexId().equals(vertexId))
                .collect(Collectors.toList());
            outEdges.forEach(edge -> System.out.println(edge.toString()));

            if (outEdges.size() > 1) {
              throw new UnsupportedOperationException("Multi outedge not yet supported");
            } else if (outEdges.size() == 0) {
              System.out.println("No out edge");
            } else {
              System.out.println("A task (id: " + task.getTaskId() + ") writes data");
              final StageBoundaryEdgeInfo outEdge = outEdges.get(0);
              writeToChannels(task.getIndex(), edgeIdToChannels, outEdge, data);
            }
          }

          // this is the only way to 'traverse' the DAG<Task>.....
          currentTaskSet.forEach(task -> taskDAG.removeVertex(task));

          // get the next 'rootVertices'
          currentTaskSet = taskDAG.getRootVertices();
        }
      });
    });

    System.out.println("Job completed.");
  }

  private List<StageBoundaryEdgeInfo> findAttachedIncomingEdges(final Task task, final TaskGroup taskGroup) {
    final String vertexId = task.getRuntimeVertexId();
    final List<StageBoundaryEdgeInfo> inEdges = new ArrayList<>();

    taskGroup.getIncomingEdges().forEach(inEdge -> {
      if (inEdge.getExternalEndpointVertexId().equals(vertexId)) {
        inEdges.add(inEdge);
      }
    });

    return inEdges;
  }

  private void writeToChannels(final int srcTaskIndex,
                               final Map<String, List<LocalChannel>> edgeIdToChannels,
                               final StageBoundaryEdgeInfo edge,
                               final Iterable<Element> data) {
    final int dstParallelism = edge.getExternalEndpointVertexAttr().get(RuntimeAttribute.IntegerKey.Parallelism);
    final List<LocalChannel> dstChannels = edgeIdToChannels.computeIfAbsent(edge.getStageBoundaryEdgeInfoId(), s -> {
      final List<LocalChannel> newChannels = new ArrayList<>(dstParallelism);
      IntStream.range(0, dstParallelism).forEach(x -> {
        final LocalChannel newChannel = new LocalChannel(HACK_DUMMY_CHAND_ID);
        newChannel.initialize(null);
        newChannels.add(newChannel);
      });
      return newChannels;
    });

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

/*
PhysicalPlan {id='ExecPlan-1',
    taskGroupsByStage=[
      [TaskGroup{taskGroupId='TaskGroup-1',
                  taskDAG=DAGImpl{
                      rootVertices=[
                          Task{taskId='Task-1', runtimeVertexId='RVertex-vertex1', index=0}],
                      parentVertices={
                          Task{taskId='Task-2', runtimeVertexId='RVertex-vertex2', index=0}=[Task{taskId='Task-1', runtimeVertexId='RVertex-vertex1', index=0}],
                          Task{taskId='Task-1', runtimeVertexId='RVertex-vertex1', index=0}=[]},
                      childrenVertices={
                          Task{taskId='Task-2', runtimeVertexId='RVertex-vertex2', index=0}=[],
                          Task{taskId='Task-1', runtimeVertexId='RVertex-vertex1', index=0}=[Task{taskId='Task-2', runtimeVertexId='RVertex-vertex2', index=0}]}},
                      resourceType=Transient,
                      incomingEdges=[],
                      outgoingEdges=[
                          StageBoundaryEdgeInfo{stageBoundaryEdgeInfoId='REdge-edge2',
                                                edgeAttributes={ChannelTransferPolicy=Push,
                                                                CommPattern=ScatterGather,
                                                                ChannelDataPlacement=Memory},
                                                externalEndpointVertexId='RVertex-vertex3',
                                                externalEndpointVertexAttr={ResourceType=Reserved}}]
                          }
                      ],
      [TaskGroup{taskGroupId='TaskGroup-2',
                  taskDAG=DAGImpl{
                    rootVertices=[
                        Task{taskId='Task-3', runtimeVertexId='RVertex-vertex3', index=0}],
                    parentVertices={
                        Task{taskId='Task-3', runtimeVertexId='RVertex-vertex3', index=0}=[],
                        Task{taskId='Task-4', runtimeVertexId='RVertex-vertex4', index=0}=[Task{taskId='Task-3', runtimeVertexId='RVertex-vertex3', index=0}],
                        Task{taskId='Task-5', runtimeVertexId='RVertex-vertex5', index=0}=[Task{taskId='Task-4', runtimeVertexId='RVertex-vertex4', index=0}]},
                    childrenVertices={
                        Task{taskId='Task-3', runtimeVertexId='RVertex-vertex3', index=0}=[Task{taskId='Task-4', runtimeVertexId='RVertex-vertex4', index=0}],
                        Task{taskId='Task-4', runtimeVertexId='RVertex-vertex4', index=0}=[Task{taskId='Task-5', runtimeVertexId='RVertex-vertex5', index=0}],
                        Task{taskId='Task-5', runtimeVertexId='RVertex-vertex5', index=0}=[]}
                  },
                  resourceType=Reserved,
                  incomingEdges=[
                    StageBoundaryEdgeInfo{stageBoundaryEdgeInfoId='REdge-edge2',
                                          edgeAttributes={ChannelTransferPolicy=Push, CommPattern=ScatterGather, ChannelDataPlacement=Memory},
                                          externalEndpointVertexId='RVertex-vertex2',
                                          externalEndpointVertexAttr={ResourceType=Transient}}],
                  outgoingEdges=[]}
      ]
    ]
}

*/

