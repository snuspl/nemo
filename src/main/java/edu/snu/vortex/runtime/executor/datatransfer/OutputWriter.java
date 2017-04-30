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
package edu.snu.vortex.runtime.executor.datatransfer;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex;
import edu.snu.vortex.runtime.common.plan.physical.Task;
import edu.snu.vortex.runtime.exception.UnsupportedCommPatternException;
import edu.snu.vortex.runtime.exception.UnsupportedPartitionerException;
import edu.snu.vortex.runtime.executor.dataplacement.DataPlacement;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Output channel interface.
 */
public final class OutputWriter extends DataTransfer {
  private final Task srcTask;
  private final RuntimeVertex dstRuntimeVertex;
  private final RuntimeEdge runtimeEdge;
  private final DataPlacement dataPlacement;

  public OutputWriter(final Task srcTask,
                      final RuntimeVertex dstRuntimeVertex,
                      final RuntimeEdge runtimeEdge,
                      final DataPlacement dataPlacement) {
    super(runtimeEdge.getRuntimeEdgeId());
    this.srcTask = srcTask;
    this.dstRuntimeVertex = dstRuntimeVertex;
    this.runtimeEdge = runtimeEdge;
    this.dataPlacement = dataPlacement;
  }

  /**
   * write dataToWrite to the channel.
   * @param dataToWrite An iterable for elements to be written.
   */
  public void write(final Iterable<Element> dataToWrite) {
    switch (dstRuntimeVertex.getVertexAttributes().get(RuntimeAttribute.Key.CommPattern)) {
    case OneToOne:
      writeOneToOne(dataToWrite);
      break;
    case Broadcast:
      writeBroadcast(dataToWrite);
      break;
    case ScatterGather:
      writeScatterGather(dataToWrite);
      break;
    default:
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  private void writeOneToOne(final Iterable<Element> dataToWrite) {
    dataPlacement.put(runtimeEdge.getRuntimeEdgeId(), srcTask.getIndex(), dataToWrite);
  }

  private void writeBroadcast(final Iterable<Element> dataToWrite) {
    dataPlacement.put(runtimeEdge.getRuntimeEdgeId(), srcTask.getIndex(), dataToWrite);
  }

  private void writeScatterGather(final Iterable<Element> dataToWrite) {
    final RuntimeAttribute partitioner = runtimeEdge.getEdgeAttributes().get(RuntimeAttribute.Key.Partition);
    final int dstParallelism = dstRuntimeVertex.getVertexAttributes().get(RuntimeAttribute.IntegerKey.Parallelism);

    switch (partitioner) {
    case Hash:
      // First partition the data to write,
      final List<List<Element>> partitionedOutputList = new ArrayList<>(dstParallelism);
      IntStream.range(0, dstParallelism).forEach(partitionIdx -> partitionedOutputList.add(new ArrayList<>()));
      dataToWrite.forEach(element -> {
        // Hash the data by its key, and "modulo" the number of destination tasks.
        final int dstIdx = Math.abs(element.getKey().hashCode() % dstParallelism);
        partitionedOutputList.get(dstIdx).add(element);
      });

      // Then write each partition appropriately to the target data placement.
      IntStream.range(0, dstParallelism).forEach(partitionIdx ->
        dataPlacement.put(
            runtimeEdge.getRuntimeEdgeId(), srcTask.getIndex(), partitionIdx, partitionedOutputList.get(partitionIdx)));
      break;
    case Range:
    default:
      throw new UnsupportedPartitionerException(
          new Exception(partitioner + " partitioning not yet supported"));
    }
  }
}
