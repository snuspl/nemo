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
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.WriteOptimizationProperty;
import edu.snu.vortex.compiler.optimizer.pass.dynamic_optimization.DataSkewDynamicOptimizationPass;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.vortex.runtime.common.plan.physical.Task;
import edu.snu.vortex.runtime.exception.PartitionFetchException;
import edu.snu.vortex.runtime.exception.UnsupportedCommPatternException;
import edu.snu.vortex.runtime.executor.data.HashRange;
import edu.snu.vortex.runtime.executor.data.PartitionManagerWorker;
import edu.snu.vortex.runtime.executor.data.PartitionStore;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.Broadcast;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.OneToOne;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.ScatterGather;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Represents the input data transfer to a task.
 * TODO #492: Modularize the data communication pattern.
 * TODO #493: Detach partitioning from writing.
 */
public final class InputReader extends DataTransfer {
  private final int dstTaskIndex;
  private final String taskGroupId;

  private final PartitionManagerWorker partitionManagerWorker;

  /**
   * Attributes that specify how we should read the input.
   */
  @Nullable
  private final IRVertex srcVertex;
  private final RuntimeEdge runtimeEdge;

  public InputReader(final int dstTaskIndex,
                     final String taskGroupId,
                     final IRVertex srcVertex,
                     final RuntimeEdge runtimeEdge,
                     final PartitionManagerWorker partitionManagerWorker) {

    super(runtimeEdge.getId());
    this.dstTaskIndex = dstTaskIndex;
    this.taskGroupId = taskGroupId;
    this.srcVertex = srcVertex;
    this.runtimeEdge = runtimeEdge;
    this.partitionManagerWorker = partitionManagerWorker;
  }

  /**
   * Reads input data depending on the communication pattern of the srcVertex.
   *
   * @return the read data.
   */
  public List<CompletableFuture<Iterable<Element>>> read() {
    final Boolean isDataSizeMetricCollectionEdge = DataSkewDynamicOptimizationPass.class
        .equals(runtimeEdge.get(ExecutionProperty.Key.MetricCollection));
    final String writeOptAtt = (String) runtimeEdge.<String>get(ExecutionProperty.Key.WriteOptimization);
    final Boolean isIFileWriteEdge =
        writeOptAtt != null && writeOptAtt.equals(WriteOptimizationProperty.IFILE_WRITE);
    try {
      switch (((Class) runtimeEdge.<Class>get(ExecutionProperty.Key.DataCommunicationPattern)).getSimpleName()) {
        case OneToOne.SIMPLE_NAME:
          return Collections.singletonList(readOneToOne());
        case Broadcast.SIMPLE_NAME:
          return readBroadcast();
        case ScatterGather.SIMPLE_NAME:
          // If the dynamic optimization which detects data skew is enabled, read the data in the assigned range.
          // TODO #492: Modularize the data communication pattern.
          if (isDataSizeMetricCollectionEdge) {
            return readDataInRange();
          } else if (isIFileWriteEdge) {
            return Collections.singletonList(readIFile());
          } else {
            return readScatterGather();
          }
        default:
          throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new PartitionFetchException(e);
    }
  }

  private CompletableFuture<Iterable<Element>> readOneToOne() throws ExecutionException, InterruptedException {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), dstTaskIndex);
    return partitionManagerWorker.retrieveDataFromPartition(partitionId, getId(),
        (Class<? extends PartitionStore>) runtimeEdge.<Class>get(ExecutionProperty.Key.DataStore), HashRange.all());
  }

  private List<CompletableFuture<Iterable<Element>>> readBroadcast()
      throws ExecutionException, InterruptedException {
    final int numSrcTasks = this.getSourceParallelism();

    final List<CompletableFuture<Iterable<Element>>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);
      futures.add(partitionManagerWorker.retrieveDataFromPartition(partitionId, getId(),
          (Class<? extends PartitionStore>) runtimeEdge.<Class>get(ExecutionProperty.Key.DataStore), HashRange.all()));
    }

    return futures;
  }

  private List<CompletableFuture<Iterable<Element>>> readScatterGather()
      throws ExecutionException, InterruptedException {
    final int numSrcTasks = this.getSourceParallelism();

    final List<CompletableFuture<Iterable<Element>>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx, dstTaskIndex);
      futures.add(partitionManagerWorker.retrieveDataFromPartition(partitionId, getId(),
          (Class<? extends PartitionStore>) runtimeEdge.<Class>get(ExecutionProperty.Key.DataStore), HashRange.all()));
    }

    return futures;
  }

  /**
   * Read data in the assigned range of hash value.
   * Constraint: If a partition is written by {@link OutputWriter#hashAndWrite(Iterable)},
   * it must be read using this method.
   *
   * @return the list of the completable future of the data.
   */
  private List<CompletableFuture<Iterable<Element>>> readDataInRange() {
    assert (runtimeEdge instanceof PhysicalStageEdge);
    final HashRange hashRangeToRead =
        ((PhysicalStageEdge) runtimeEdge).getTaskGroupIdToHashRangeMap().get(taskGroupId);
    if (hashRangeToRead == null) {
      throw new PartitionFetchException(new Throwable("The hash range to read is not assigned to " + taskGroupId));
    }

    final int numSrcTasks = this.getSourceParallelism();
    final List<CompletableFuture<Iterable<Element>>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);
      futures.add(
          partitionManagerWorker.retrieveDataFromPartition(partitionId, getId(),
              (Class<? extends PartitionStore>) runtimeEdge.<Class>get(ExecutionProperty.Key.DataStore),
              hashRangeToRead));
    }

    return futures;
  }

  /**
   * Read the I-File prepared for this task by using {@link OutputWriter#writeIFile(Iterable)}.
   *
   * @return the completable future of the data.
   */
  private CompletableFuture<Iterable<Element>> readIFile() {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), dstTaskIndex);
    return partitionManagerWorker.retrieveDataFromPartition(partitionId, getId(),
        (Class<? extends PartitionStore>) runtimeEdge.<Class>get(ExecutionProperty.Key.DataStore), HashRange.all());
  }

  public RuntimeEdge getRuntimeEdge() {
    return runtimeEdge;
  }

  public String getSrcVertexId() {
    // this src vertex can be either a real vertex or a task. we must check!
    if (srcVertex != null) {
      return srcVertex.getId();
    }

    return ((Task) runtimeEdge.getSrc()).getRuntimeVertexId();
  }

  public boolean isSideInputReader() {
    return Boolean.TRUE.equals(runtimeEdge.get(ExecutionProperty.Key.IsSideInput));
  }

  public CompletableFuture<Object> getSideInput() {
    if (!isSideInputReader()) {
      throw new RuntimeException();
    }
    final CompletableFuture<Iterable<Element>> future = this.read().get(0);
    return future.thenApply(f -> f.iterator().next().getData());
  }

  /**
   * Get the parallelism of the source task.
   *
   * @return the parallelism of the source task.
   */
  public int getSourceParallelism() {
    if (srcVertex != null) {
      final Integer numSrcTasks = (Integer) srcVertex.get(ExecutionProperty.Key.Parallelism);
      return numSrcTasks == null ? 1 : numSrcTasks;
    } else {
      // Memory input reader
      return 1;
    }
  }

  /**
   * Combine the given list of futures.
   *
   * @param futures to combine.
   * @return the combined iterable of elements.
   * @throws ExecutionException   when fail to get results from futures.
   * @throws InterruptedException when interrupted during getting results from futures.
   */
  public static Iterable<Element> combineFutures(final List<CompletableFuture<Iterable<Element>>> futures)
      throws ExecutionException, InterruptedException {
    final List<Element> concatStreamBase = new ArrayList<>();
    Stream<Element> concatStream = concatStreamBase.stream();
    for (int srcTaskIdx = 0; srcTaskIdx < futures.size(); srcTaskIdx++) {
      final Iterable<Element> dataFromATask = futures.get(srcTaskIdx).get();
      concatStream = Stream.concat(concatStream, StreamSupport.stream(dataFromATask.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }
}
