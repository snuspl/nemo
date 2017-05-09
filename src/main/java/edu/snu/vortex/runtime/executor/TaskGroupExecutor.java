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
package edu.snu.vortex.runtime.executor;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.Reader;
import edu.snu.vortex.compiler.ir.Transform;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.executor.datatransfer.DataTransferFactory;
import edu.snu.vortex.runtime.executor.datatransfer.InputReader;
import edu.snu.vortex.runtime.executor.datatransfer.OutputWriter;
import edu.snu.vortex.runtime.master.ContextImpl;
import edu.snu.vortex.runtime.master.OutputCollectorImpl;

import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public final class TaskGroupExecutor {

  private static final Logger LOG = Logger.getLogger(TaskGroupExecutor.class.getName());

  private final TaskGroup taskGroup;
  private final TaskGroupStateManager taskGroupStateManager;
  private final Set<PhysicalStageEdge> stageIncomingEdges;
  private final Set<PhysicalStageEdge> stageOutgoingEdges;
  private final DataTransferFactory channelFactory;

  private final Map<String, List<InputReader>> taskIdToChannelReadersMap;
  private final Map<String, List<OutputWriter>> taskIdToChannelWritersMap;

  public TaskGroupExecutor(final TaskGroup taskGroup,
                           final TaskGroupStateManager taskGroupStateManager,
                           final Set<PhysicalStageEdge> stageIncomingEdges,
                           final Set<PhysicalStageEdge> stageOutgoingEdges,
                           final DataTransferFactory channelFactory) {
    this.taskGroup = taskGroup;
    this.taskGroupStateManager = taskGroupStateManager;
    this.stageIncomingEdges = stageIncomingEdges;
    this.stageOutgoingEdges = stageOutgoingEdges;
    this.channelFactory = channelFactory;

    this.taskIdToChannelReadersMap = new HashMap<>();
    this.taskIdToChannelWritersMap = new HashMap<>();
  }

  private void initializeChannels() {
    taskGroup.getTaskDAG().topologicalDo((task -> {
      final Set<PhysicalStageEdge> inEdgesFromOtherStages = getInEdgesFromOtherStages(task);
      final Set<PhysicalStageEdge> outEdgesToOhterStages = getOutEdgesToOtherStages(task);

      inEdgesFromOtherStages.forEach(physicalStageEdge -> {
        final InputReader channelReader = channelFactory.createReader(
            task, physicalStageEdge.getSrcVertex(), physicalStageEdge);
        addChannelReader(task, channelReader);
      });

      outEdgesToOhterStages.forEach(physicalStageEdge -> {
        final OutputWriter channelWriter = channelFactory.createWriter(
            task, physicalStageEdge.getDstVertex(), physicalStageEdge);
        addChannelWriter(task, channelWriter);
      });

      final Set<RuntimeEdge<Task>> inEdgesWithinStage = taskGroup.getTaskDAG().getIncomingEdgesOf(task);
      inEdgesWithinStage.forEach(internalEdge -> createLocalChannel(task, internalEdge));
    }));
  }

  private Set<PhysicalStageEdge> getInEdgesFromOtherStages(final Task task) {
    return stageIncomingEdges.stream().filter(
        stageInEdge -> stageInEdge.getDstVertex().getId().equals(task.getRuntimeVertexId()))
        .collect(Collectors.toSet());
  }

  private Set<PhysicalStageEdge> getOutEdgesToOtherStages(final Task task) {
    return stageOutgoingEdges.stream().filter(
        stageInEdge -> stageInEdge.getSrcVertex().getId().equals(task.getRuntimeVertexId()))
        .collect(Collectors.toSet());
  }

  private void createLocalChannel(final Task task, final RuntimeEdge<Task> internalEdge) {
    final InputReader channelReader = channelFactory.createReader(task, internalEdge);
    addChannelReader(task, channelReader);

    final OutputWriter channelWriter = channelFactory.createWriter(task, internalEdge);
    addChannelWriter(task, channelWriter);
  }

  private void addChannelReader(final Task task, final InputReader channelReader) {
    taskIdToChannelReadersMap.computeIfAbsent(task.getId(), (key) -> new ArrayList<>());
    taskIdToChannelReadersMap.get(task.getId()).add(channelReader);
  }

  private void addChannelWriter(final Task task, final OutputWriter channelWriter) {
    taskIdToChannelWritersMap.computeIfAbsent(task.getId(), (key) -> new ArrayList<>());
    taskIdToChannelWritersMap.get(task.getId()).add(channelWriter);
  }

  public void execute() {
    initializeChannels();
    taskGroup.getTaskDAG().topologicalDo(task -> {
      if (task instanceof BoundedSourceTask) {
        launchBoundedSourceTask((BoundedSourceTask) task);
      } else if (task instanceof OperatorTask) {
        launchOperatorTask((OperatorTask) task);
      } else {
        throw new UnsupportedOperationException(task.toString());
      }
    });
  }

  private void launchBoundedSourceTask(final BoundedSourceTask boundedSourceTask) {
    try {
      final Reader reader = boundedSourceTask.getReader();
      final Iterable<Element> readData = reader.read();

      taskIdToChannelWritersMap.get(boundedSourceTask.getId()).forEach(writer -> writer.write(readData));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void launchOperatorTask(final OperatorTask operatorTask) {
    final Map<Transform, Object> sideInputMap = new HashMap<>();

    taskIdToChannelReadersMap.get(operatorTask.getId())
        .stream()
        .filter(InputReader::isSideInputReader)
        .forEach(channelReader -> {
          final Object sideInput = channelReader.getSideInput();
          // TODO #: Assumption!
          final Transform srcTransform = ((OperatorTask)channelReader.getRuntimeEdge().getSrc()).getTransform();
          sideInputMap.put(srcTransform, sideInput);
        });

    final Transform.Context transformContext = new ContextImpl(sideInputMap);
    final OutputCollectorImpl outputCollector = new OutputCollectorImpl();

    final Transform transform = operatorTask.getTransform();
    transform.prepare(transformContext, outputCollector);
    taskIdToChannelReadersMap.get(operatorTask.getId())
        .stream()
        .filter(channelReader -> !channelReader.isSideInputReader())
        .forEach(channelReader -> transform.onData(channelReader.read(), channelReader.getSrcRuntimeVertexId()));
    transform.close();

    final Iterable<Element> output = outputCollector.getOutputList();
    taskIdToChannelWritersMap.get(operatorTask.getId()).forEach(channelWriter -> channelWriter.write(output));
  }
}
