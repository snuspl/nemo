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
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeOperatorVertex;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.executor.channel.LocalChannel;
import edu.snu.vortex.utils.dag.DAG;
import org.apache.commons.lang3.SerializationUtils;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * Simple Runtime that logs intermediate results.
 */
public final class SimpleRuntime {
  private static final Logger LOG = Logger.getLogger(SimpleRuntime.class.getName());

  // TODO #91: Implement Channels
  private static final String HACK_DUMMY_CHAND_ID = "HACK";

  /**
   * Executes the given physical plan.
   * @param physicalPlan Physical Plan.
   * @throws Exception during execution.
   */
  public void executePhysicalPlan(final PhysicalPlan physicalPlan) throws Exception {
    final Map<String, List<LocalChannel>> edgeIdToChannels = new HashMap<>();
    final DAG<PhysicalStage, StageBoundaryEdgeInfo> stageDAG = physicalPlan.getStageDAG();

    // TODO #93: Implement Batch Scheduler
    stageDAG.getTopologicalSort().forEach(stage -> {
      final int stageParallelism = stage.getTaskGroupList().size();
      final Set<StageBoundaryEdgeInfo> stageIncomingEdges = stageDAG.getIncomingEdges(stage);
      final Set<StageBoundaryEdgeInfo> stageOutgoingEdges = stageDAG.getOutgoingEdges(stage);

      stage.getTaskGroupList().forEach(taskGroup -> {

        final DAG<Task, RuntimeEdge<Task>> taskDAG = taskGroup.getTaskDAG();
        final List<Task> sortedTasks = taskDAG.getTopologicalSort();
        sortedTasks.forEach(task -> {
          Iterable<Element> data = null;
          final String vertexId = task.getRuntimeVertexId();

          // TODO #141: Remove instanceof
          if (task instanceof BoundedSourceTask) {
            try {
              final BoundedSourceTask boundedSourceTask = (BoundedSourceTask) task;
              final Reader reader = boundedSourceTask.getReader();
              data = reader.read();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          } else if (task instanceof OperatorTask) {
            // Check for any incoming edge from other stages.
            final Set<StageBoundaryEdgeInfo> inEdgesFromOtherStages = stageIncomingEdges.stream().filter(
                stageInEdge -> stageInEdge.getDstVertex().getId().equals(vertexId)).collect(Collectors.toSet());

            // Check for incoming edge from this stage.
            final Set<RuntimeEdge<Task>> inEdgesWithinStage = taskDAG.getIncomingEdges(task);

            final Set<RuntimeEdge> nonSideInputEdges =
                filterInputEdges(inEdgesFromOtherStages, inEdgesWithinStage, false);
            final Set<RuntimeEdge> sideInputEdges =
                filterInputEdges(inEdgesFromOtherStages, inEdgesWithinStage, true);
            final Map<Transform, Object> sideInputs = getSideInputs(sideInputEdges, task, edgeIdToChannels);

            if (nonSideInputEdges.size() > 1) {
              // TODO #13: Implement Join Node
              throw new UnsupportedOperationException("Multi inedge not yet supported");
            } else if (nonSideInputEdges.size() == 1) { // We fetch 'data' from the incoming stage
              final RuntimeEdge inEdge = nonSideInputEdges.iterator().next();
              data = edgeIdToChannels.get(inEdge.getRuntimeEdgeId()).get(task.getIndex()).read();

              final OperatorTask operatorTask = (OperatorTask) task;

              // TODO #18: Support code/data serialization
              final Transform transform = SerializationUtils.clone(operatorTask.getTransform());

              final Transform.Context transformContext = new ContextImpl(sideInputs);
              final OutputCollectorImpl outputCollector = new OutputCollectorImpl();
              transform.prepare(transformContext, outputCollector);
              transform.onData(data, null); // hack (TODO #132: Refactor DAG)
              transform.close();
              data = outputCollector.getOutputList();
            }
          } else {
            throw new UnsupportedOperationException(task.toString());
          }

          LOG.log(Level.INFO, " Output of {" + task.getTaskId() + "}: " +
              (data.toString().length() > 5000 ?
                  data.toString().substring(0, 5000) + "..." : data.toString()));

          // Check for any outgoing edge to other stages and write output.
          final Set<StageBoundaryEdgeInfo> outEdgesToOtherStages = stageOutgoingEdges.stream().filter(
              outEdgeInfo -> outEdgeInfo.getSrcVertex().getId().equals(vertexId)).collect(Collectors.toSet());

          if (outEdgesToOtherStages != null) {
            final Iterable<Element> finalData = data;
            outEdgesToOtherStages.forEach(outEdge -> writeToChannels(task.getIndex(), edgeIdToChannels, outEdge,
                outEdge.getExternalVertexAttr().get(RuntimeAttribute.IntegerKey.Parallelism), finalData));
          }

          // Check for any outgoing edge within the stage and write output.
          final Set<RuntimeEdge<Task>> outEdgesWithinStage = taskDAG.getOutgoingEdges(task);

          if (outEdgesWithinStage != null) {
            final Iterable<Element> finalData = data;
            outEdgesWithinStage.forEach(outEdge -> writeToChannels(task.getIndex(), edgeIdToChannels, outEdge,
                stageParallelism, finalData));
          }
        });
      });
    });
  }

  /**
   * Filters input edges (either side-input, or non-side-input).
   * @param inEdgesFromOtherStages edges from other stages.
   * @param inEdgesWithinStage edges within the stage.
   * @param getSideInputEdges true if side-input edges are to be filtered, false otherwise.
   * @return the set of filtered edges.
   */
  private Set<RuntimeEdge> filterInputEdges(final Set<StageBoundaryEdgeInfo> inEdgesFromOtherStages,
                                            final Set<RuntimeEdge<Task>> inEdgesWithinStage,
                                            final boolean getSideInputEdges) {
    final Set<RuntimeEdge> filteredEdges = new HashSet<>();
    if (inEdgesFromOtherStages != null) {
      filteredEdges.addAll(inEdgesFromOtherStages.stream()
          .filter(
              inEdge -> (inEdge.getEdgeAttributes().get(RuntimeAttribute.Key.SideInput) != RuntimeAttribute.SideInput)
                  ^ getSideInputEdges)
          .collect(Collectors.toSet()));
    }
    if (inEdgesWithinStage != null) {
      filteredEdges.addAll(inEdgesWithinStage.stream()
          .filter(
              inEdge -> (inEdge.getEdgeAttributes().get(RuntimeAttribute.Key.SideInput) != RuntimeAttribute.SideInput)
                  ^ getSideInputEdges)
          .collect(Collectors.toSet()));
    }
    return filteredEdges;
  }

  /**
   * Retrieves side-inputs based on the given side-input edges.
   * @param sideInputEdges the set of side-input edges.
   * @param task the subject task.
   * @param edgeIdToChannels the map of runtime edge ID to channels.
   * @return the side-inputs.
   */
  private Map<Transform, Object> getSideInputs(final Set<RuntimeEdge> sideInputEdges,
                                               final Task task,
                                               final Map<String, List<LocalChannel>> edgeIdToChannels) {
    if (sideInputEdges != null) {
      final Map<Transform, Object> sideInputs = new HashMap<>();
      sideInputEdges.forEach(inEdge -> {
        final Iterable<Element> elementSideInput =
            edgeIdToChannels.get(inEdge.getRuntimeEdgeId()).get(task.getIndex()).read();
        final List<Object> objectSideInput = StreamSupport
            .stream(elementSideInput.spliterator(), false)
            .map(element -> element.getData())
            .collect(Collectors.toList());
        if (objectSideInput.size() != 1) {
          throw new RuntimeException("Size of out data partitions of a broadcast operator must match 1");
        }

        final Transform srcTransform;
        if (inEdge instanceof StageBoundaryEdgeInfo) {
          srcTransform = ((RuntimeOperatorVertex) ((StageBoundaryEdgeInfo) inEdge).getSrcVertex())
              .getOperatorVertex().getTransform();
        } else {
          srcTransform = ((OperatorTask) inEdge.getSrc()).getTransform();
        }
        sideInputs.put(srcTransform, objectSideInput.get(0));
      });
      return sideInputs;
    } else {
      return new HashMap<>(0);
    }
  }

  /**
   * Writes data to appropriate channels.
   * @param srcTaskIndex to be used for one-to-one edge channels.
   * @param edgeIdToChannels the map of runtime edge ID to channels.
   * @param edge to determine how data should be written to the corresponding channels.
   * @param dstParallelism to be used for finding the corresponding channel for scatter-gather edges.
   * @param data to write.
   */
  private void writeToChannels(final int srcTaskIndex,
                               final Map<String, List<LocalChannel>> edgeIdToChannels,
                               final RuntimeEdge edge,
                               final int dstParallelism,
                               final Iterable<Element> data) {
    final List<LocalChannel> dstChannels = edgeIdToChannels.computeIfAbsent(edge.getRuntimeEdgeId(), s -> {
      final List<LocalChannel> newChannels = new ArrayList<>(dstParallelism);
      IntStream.range(0, dstParallelism).forEach(x -> {
        // This is a hack to make the runtime work for now
        // In the future, channels should be passed to tasks via their methods (e.g., Task#compute)
        // TODO #91: Implement Channels
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
        final RuntimeAttribute partitioningAttribute = edge.getEdgeAttributes().get(RuntimeAttribute.Key.Partition);
        switch (partitioningAttribute) {
          case Hash:
            final List<List<Element>> routedPartitions = new ArrayList<>(dstParallelism);
            IntStream.range(0, dstParallelism).forEach(x -> routedPartitions.add(new ArrayList<>()));
            data.forEach(element -> {
              final int dstIndex = Math.abs(element.getKey().hashCode() % dstParallelism);
              routedPartitions.get(dstIndex).add(element);
            });
            IntStream.range(0, dstParallelism).forEach(x -> dstChannels.get(x).write(routedPartitions.get(x)));
            break;
          case Range:
            throw new UnsupportedOperationException("Range partitioning not yet supported");
          default:
            throw new RuntimeException("Unknown attribute: " + partitioningAttribute);
        }
        break;
      default:
        throw new UnsupportedOperationException(edge.toString());
    }
  }
}

