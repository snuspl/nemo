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

import edu.snu.vortex.common.Pair;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.exception.UnsupportedCommPatternException;
import edu.snu.vortex.runtime.exception.UnsupportedPartitionerException;
import edu.snu.vortex.runtime.executor.data.PartitionManagerWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Represents the output data transfer from a task.
 */
public final class OutputWriter extends DataTransfer {
  private final int hashRangeMultiplier;
  private final int srcTaskIdx;
  private final RuntimeEdge runtimeEdge;
  private final String srcVertexId;
  private final IRVertex dstVertex;

  /**
   * The Block Manager Worker.
   */
  private final PartitionManagerWorker partitionManagerWorker;

  public OutputWriter(final int hashRangeMultiplier,
                      final int srcTaskIdx,
                      final String srcRuntimeVertexId,
                      final IRVertex dstRuntimeVertex,
                      final RuntimeEdge runtimeEdge,
                      final PartitionManagerWorker partitionManagerWorker) {
    super(runtimeEdge.getId());
    this.hashRangeMultiplier = hashRangeMultiplier;
    this.runtimeEdge = runtimeEdge;
    this.srcVertexId = srcRuntimeVertexId;
    this.dstVertex = dstRuntimeVertex;
    this.partitionManagerWorker = partitionManagerWorker;
    this.srcTaskIdx = srcTaskIdx;
  }

  /**
   * Writes output data depending on the communication pattern of the edge.
   * @param dataToWrite An iterable for the elements to be written.
   */
  public void write(final Iterable<Element> dataToWrite) {
    final Boolean isDataSizeMetricCollectionEdge =
        runtimeEdge.getAttributes().get(Attribute.Key.DataSizeMetricCollection) != null;
    final Attribute writeOptAtt = runtimeEdge.getAttributes().get(Attribute.Key.WriteOptimization);
    final Boolean isIFileWriteEdge =
        writeOptAtt != null && writeOptAtt.equals(Attribute.IFileWrite);
    switch (runtimeEdge.getAttributes().get(Attribute.Key.CommunicationPattern)) {
      case OneToOne:
        writeOneToOne(dataToWrite);
        break;
      case Broadcast:
        writeBroadcast(dataToWrite);
        break;
      case ScatterGather:
        // If the dynamic optimization which detects data skew is enabled, sort the data and write it.
        if (isDataSizeMetricCollectionEdge) {
          hashAndWrite(dataToWrite);
        } else if (isIFileWriteEdge) {
          writeIFile(dataToWrite);
        } else {
          writeScatterGather(dataToWrite);
        }
        break;
      default:
        throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  private void writeOneToOne(final Iterable<Element> dataToWrite) {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);
    partitionManagerWorker.putPartition(partitionId, dataToWrite,
        runtimeEdge.getAttributes().get(Attribute.Key.ChannelDataPlacement));
  }

  private void writeBroadcast(final Iterable<Element> dataToWrite) {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);
    partitionManagerWorker.putPartition(partitionId, dataToWrite,
        runtimeEdge.getAttributes().get(Attribute.Key.ChannelDataPlacement));
  }

  private void writeScatterGather(final Iterable<Element> dataToWrite) {
    final Attribute partition = runtimeEdge.getAttributes().get(Attribute.Key.Partitioning);
    switch (partition) {
    case Hash:
      final int dstParallelism = dstVertex.getAttributes().get(Attribute.IntegerKey.Parallelism);

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
        // Give each partition its own partition id
        final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx, partitionIdx);
        partitionManagerWorker.putPartition(partitionId, partitionedOutputList.get(partitionIdx),
            runtimeEdge.getAttributes().get(Attribute.Key.ChannelDataPlacement));
      });
      break;
    case Range:
    default:
      throw new UnsupportedPartitionerException(new Exception(partition + " partitioning not yet supported"));
    }
  }

  /**
   * Hashes an output according to the hash value and writes it as a single partition.
   * This function will be called only when we need to split or recombine an output data from a task after it is stored
   * (e.g., dynamic data skew handling).
   * We extend the hash range with the factor {@link edu.snu.vortex.client.JobConf.HashRangeMultiplier} in advance
   * to prevent the extra deserialize - rehash - serialize process.
   * Each data of this partition having same key hash value will be collected as a single block.
   * This block will be the unit of retrieval and recombination of this partition.
   * Constraint: If a partition is written by this method, it have to be read by {@link InputReader#readDataInRange()}.
   * TODO #378: Elaborate block construction during data skew pass
   * TODO 428: DynOpt-clean up the metric collection flow
   *
   * @param dataToWrite an iterable for the elements to be written.
   */
  private void hashAndWrite(final Iterable<Element> dataToWrite) {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);
    final int dstParallelism = dstVertex.getAttributes().get(Attribute.IntegerKey.Parallelism);
    // For this hash range, please check the description of HashRangeMultiplier
    final int hashRange = hashRangeMultiplier * dstParallelism;

    // Separate the data into blocks according to the hash of their key.
    final List<Pair<Integer, Iterable<Element>>> outputBlockList = new ArrayList<>(hashRange);
    IntStream.range(0, hashRange).forEach(hashVal -> outputBlockList.add(Pair.of(hashVal, new ArrayList<>())));
    dataToWrite.forEach(element -> {
      // Hash the data by its key, and "modulo" by the hash range.
      final int hashVal = Math.abs(element.getKey().hashCode() % hashRange);
      ((List) outputBlockList.get(hashVal).right()).add(element);
    });

    partitionManagerWorker.putHashedPartition(partitionId, srcVertexId, outputBlockList,
        runtimeEdge.getAttributes().get(Attribute.Key.ChannelDataPlacement));
  }

  /**
   * Hashes an output according to the hash value and constructs I-Files with the hashed data blocks.
   * Each destination task will have a single I-File to read regardless of the input parallelism.
   * To prevent the extra sort process in the source task and deserialize - merge process in the destination task,
   * we extend the hash range with the factor {@link edu.snu.vortex.client.JobConf.HashRangeMultiplier} in advance
   * and make the blocks as the unit of retrieval.
   * Constraint: If a partition is written by this method, it have to be read by {@link InputReader#readIFile()}.
   * TODO #378: Elaborate block construction during data skew pass
   *
   * @param dataToWrite an iterable for the elements to be written.
   */
  private void writeIFile(final Iterable<Element> dataToWrite) {
    final int dstParallelism = dstVertex.getAttributes().get(Attribute.IntegerKey.Parallelism);
    // For this hash range, please check the description of HashRangeMultiplier
    final int hashRange = hashRangeMultiplier * dstParallelism;
    final List<List<Pair<Integer, Iterable<Element>>>> outputList = new ArrayList<>(dstParallelism);
    // Create data blocks for each I-File.
    IntStream.range(0, dstParallelism).forEach(dstIdx -> {
      final List<Pair<Integer, Iterable<Element>>> outputBlockList = new ArrayList<>(hashRangeMultiplier);
      IntStream.range(0, hashRangeMultiplier).forEach(hashValRemainder -> {
        final int hashVal = hashRangeMultiplier * dstIdx + hashValRemainder;
        outputBlockList.add(Pair.of(hashVal, new ArrayList<>()));
      });
      outputList.add(outputBlockList);
    });

    // Assigns data to the corresponding hashed block.
    dataToWrite.forEach(element -> {
      final int hashVal = Math.abs(element.getKey().hashCode() % hashRange);
      final int dstIdx = hashVal / hashRangeMultiplier;
      final int blockIdx = Math.abs(hashVal % hashRangeMultiplier);
      ((List) outputList.get(dstIdx).get(blockIdx).right()).add(element);
    });

    // Then append each blocks to corresponding partition appropriately.
    IntStream.range(0, dstParallelism).forEach(dstIdx -> {
      final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), dstIdx);
      partitionManagerWorker.appendHashedDataToPartition(partitionId, outputList.get(dstIdx),
          runtimeEdge.getAttributes().get(Attribute.Key.ChannelDataPlacement));
    });
  }
}
