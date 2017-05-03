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
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex;
import edu.snu.vortex.runtime.common.plan.physical.Task;
import edu.snu.vortex.runtime.exception.UnsupportedCommPatternException;
import edu.snu.vortex.runtime.exception.UnsupportedPartitionerException;
import edu.snu.vortex.runtime.executor.block.BlockManagerWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Represents the output data transfer from a task.
 */
public final class OutputWriter extends DataTransfer {
  /**
   * Attributes that specify how we should write the output.
   */
  private final int dstParallelism;
  private final RuntimeAttribute partitionAttribute;
  private final RuntimeAttribute commPatternAttribute;

  /**
   * The Block Manager Worker.
   */
  private final BlockManagerWorker blockManagerWorker;

  /**
   * Block id of this output.
   * For communication patterns that 'partitions' the block, this is used to generate each sub-block id.
   */
  private final String blockId;

  public OutputWriter(final Task srcTask,
                      final RuntimeVertex dstRuntimeVertex,
                      final RuntimeEdge runtimeEdge,
                      final BlockManagerWorker blockManagerWorker) {
    super(runtimeEdge.getRuntimeEdgeId());
    this.dstParallelism = dstRuntimeVertex.getVertexAttributes().get(RuntimeAttribute.IntegerKey.Parallelism);
    this.commPatternAttribute = runtimeEdge.getEdgeAttributes().get(RuntimeAttribute.Key.CommPattern);
    this.partitionAttribute = runtimeEdge.getEdgeAttributes().get(RuntimeAttribute.Key.Partition);
    this.blockManagerWorker = blockManagerWorker;
    this.blockId = RuntimeIdGenerator.generateBlockId(srcTask.getId(), runtimeEdge.getRuntimeEdgeId());
  }

  /**
   * Writes output data depending on the communication pattern of the dstRuntimeVertex.
   * @param dataToWrite An iterable for the elements to be written.
   */
  public void write(final Iterable<Element> dataToWrite) {
    switch (commPatternAttribute) {
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
    blockManagerWorker.writeBlock(blockId, dataToWrite);
  }

  private void writeBroadcast(final Iterable<Element> dataToWrite) {
    blockManagerWorker.writeBlock(blockId, dataToWrite);
  }

  private void writeScatterGather(final Iterable<Element> dataToWrite) {
    switch (partitionAttribute) {
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
      IntStream.range(0, dstParallelism).forEach(partitionIdx -> {
        // Give each partition its own 'subBlockId'
        final String subBlockId = RuntimeIdGenerator.generateSubBlockId(blockId, partitionIdx);
        blockManagerWorker.writeBlock(subBlockId, partitionedOutputList.get(partitionIdx));
      });
      break;
    case Range:
    default:
      throw new UnsupportedPartitionerException(new Exception(partitionAttribute + " partitioning not yet supported"));
    }
  }
}
