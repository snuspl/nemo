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

import edu.snu.vortex.common.Pair;
import edu.snu.vortex.common.StateMachine;
import edu.snu.vortex.runtime.common.ClosableBlockingIterable;
import edu.snu.vortex.runtime.common.ObservableIterableWrapper;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.state.PartitionState;
import edu.snu.vortex.runtime.exception.AbsentPartitionException;
import edu.snu.vortex.runtime.master.resource.ContainerManager;
import edu.snu.vortex.runtime.master.resource.ExecutorRepresenter;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * This class represents a partition metadata stored in the metadata server.
 */
@ThreadSafe
final class PartitionMetadata {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionManagerMaster.class.getName());
  private final ContainerManager containerManager;

  // Partition level metadata.
  private final String partitionId;
  private final PartitionState partitionState;
  private final Set<Integer> producerTaskIndices;
  // The indices of the task groups who didn't commit data yet.
  private volatile Set<Integer> remainingProducerTaskIndices;
  // The future of the location of this block.
  // It is completed when the partition is located (and can be changed when it is committed.)
  private volatile CompletableFuture<String> locationFuture;

  // Block level metadata. These information will be managed only for remote partitions.
  private volatile long writtenBytesCursor; // Indicates how many bytes are (at least, logically) written in the file.
  private volatile int publishedBlockCount; // Represents the number of published blocks.
  // When a writer reserves a file region for a block to write, the metadata of the block is stored in this list.
  // When a block in this list is committed, the committed blocks are moved to the committed iterable
  // if any pre-reserved but not committed block does not exist.
  private volatile List<BlockMetadataInServer> reserveBlockMetadataList;
  private volatile ClosableBlockingIterable<BlockMetadataInServer> committedBlockMetadataList;
  private volatile ObservableIterableWrapper<BlockMetadataInServer> observableCommittedBlockMetadata;

  /**
   * Constructs the metadata for a partition.
   *
   * @param containerManager    the container manager.
   * @param partitionId         the id of the partition.
   * @param producerTaskIndices the indices of the producer tasks.
   */
  PartitionMetadata(final ContainerManager containerManager,
                    final String partitionId,
                    final Set<Integer> producerTaskIndices) {
    this.containerManager = containerManager;
    // Initialize partition level metadata.
    this.partitionId = partitionId;
    this.partitionState = new PartitionState();
    this.locationFuture = new CompletableFuture<>();
    this.producerTaskIndices = producerTaskIndices;
    this.remainingProducerTaskIndices = new HashSet<>(producerTaskIndices);
    // Initialize block level metadata.
    initializeBlockMetadataInfo();
  }

  /**
   * Deals with state change of the corresponding partition.
   *
   * @param newState        the new state of the partition.
   * @param location        the location of the partition (e.g., worker id, remote store).
   *                        {@code null} if not created or lost.
   * @param producerTaskIdx the index of the task produced the commit. {@code null} if not committed.
   */
  synchronized void onStateChanged(final PartitionState.State newState,
                                   @Nullable final String location,
                                   @Nullable final Integer producerTaskIdx) {
    final StateMachine stateMachine = partitionState.getStateMachine();
    final Enum oldState = stateMachine.getCurrentState();
    LOG.debug("Partition State Transition: id {} from {} to {}", new Object[]{partitionId, oldState, newState});

    switch (newState) {
      case SCHEDULED:
        stateMachine.setState(newState);
        break;
      case LOST:
        LOG.info("Partition {} lost in {}", new Object[]{partitionId, location});
      case LOST_BEFORE_COMMIT:
      case REMOVED:
        // Reset the partition location and committer information.
        remainingProducerTaskIndices = new HashSet<>(producerTaskIndices);
        locationFuture.completeExceptionally(new AbsentPartitionException(partitionId, newState));
        locationFuture = new CompletableFuture<>();

        // Initialize the block metadata.
        committedBlockMetadataList.close();
        initializeBlockMetadataInfo();

        stateMachine.setState(newState);
        break;
      case CREATED:
        if (!stateMachine.getCurrentState().equals(PartitionState.State.CREATED)) {
          assert (location != null);
          locationFuture.complete(location);
          stateMachine.setState(newState);
        } // A (part of) partition can be created in multiple executors. e.g., constructing an I-File.
        break;
      case COMMITTED:
        remainingProducerTaskIndices.remove(producerTaskIdx);
        if (remainingProducerTaskIndices.isEmpty()) {
          // All committers have committed their data.
          committedBlockMetadataList.close();
          stateMachine.setState(newState);
        }
        break;
      default:
        throw new UnsupportedOperationException(newState.toString());
    }
  }

  /**
   * @return the partition id.
   */
  String getPartitionId() {
    return partitionId;
  }

  /**
   * @return the state of this partition.
   */
  PartitionState getPartitionState() {
    return partitionState;
  }

  /**
   * @return the future of the location of this partition.
   */
  synchronized CompletableFuture<String> getLocationFuture() {
    return locationFuture;
  }

  /**
   * Reserves the region for a block and get the metadata for the block.
   *
   * @param blockMetadata   the block metadata to append.
   * @return the pair of the index of reserved block and starting position of the block in the file.
   */
  synchronized Optional<Pair<Integer, Long>> reserveBlock(final ControlMessage.BlockMetadataMsg blockMetadata) {
    final PartitionState.State currentState =
        (PartitionState.State) partitionState.getStateMachine().getCurrentState();
    if (currentState == PartitionState.State.CREATED) {
      // Can reserve the block.
      final int blockSize = blockMetadata.getBlockSize();
      final long currentPosition = writtenBytesCursor;
      final int blockIdx = reserveBlockMetadataList.size() + publishedBlockCount;
      final ControlMessage.BlockMetadataMsg blockMetadataToStore =
          ControlMessage.BlockMetadataMsg.newBuilder()
              .setHashValue(blockMetadata.getHashValue())
              .setBlockSize(blockSize)
              .setOffset(currentPosition)
              .setNumElements(blockMetadata.getNumElements())
              .build();

      writtenBytesCursor += blockSize;
      reserveBlockMetadataList.add(new BlockMetadataInServer(blockMetadataToStore));
      return Optional.of(Pair.of(blockIdx, currentPosition));
    } else {
      // Cannot reserve the block. The reserving executor will noticed that the block is not reserved.
      LOG.warn("Cannot reserve a block to the partition of which state is {}.", currentState);
      return Optional.empty();
    }
  }

  /**
   * Notifies that some blocks are written.
   *
   * @param blockIndicesToCommit the indices of the blocks to commit.
   */
  synchronized void commitBlocks(final Iterable<Integer> blockIndicesToCommit) {
    // Mark the blocks committed.
    blockIndicesToCommit.forEach(idx -> reserveBlockMetadataList.get(idx - publishedBlockCount).setCommitted());

    final Iterator<BlockMetadataInServer> iterator = reserveBlockMetadataList.iterator();
    while (iterator.hasNext()) {
      final BlockMetadataInServer blockMetadata = iterator.next();
      if (blockMetadata.isCommitted()) {
        // If the block in the head of the reserved block list is committed, publish it.
        iterator.remove();
        // This block metadata will be published to all subscribers through the observableCommittedBlockMetadata.
        committedBlockMetadataList.add(blockMetadata);
        publishedBlockCount++;
      } else {
        break;
      }
    }
  }

  /**
   * Subscribe the commit of blocks in this partition.
   * It will publish all of the already committed blocks, and also publish others when they are committed through
   * {@link CommittedBlockMetadataObserver}.
   *
   * @param executorId the ID of the executor which requested the block metadata.
   * @param listenerId the ID of the listener which will handle the committed block metadata message.
   */
  synchronized void subscribeCommittedBlocks(final String executorId,
                                             final String listenerId) {
    observableCommittedBlockMetadata
        // Conducts asynchronous subscription. (Communication thread which called this method will not be blocked)
        .subscribeOn(Schedulers.io())
        .subscribe(new CommittedBlockMetadataObserver(executorId, listenerId));
  }

  /**
   * Initialize the block metadata information.
   */
  private void initializeBlockMetadataInfo() {
    this.reserveBlockMetadataList = new LinkedList<>();
    this.committedBlockMetadataList = new ClosableBlockingIterable<>();
    this.observableCommittedBlockMetadata = new ObservableIterableWrapper<>(committedBlockMetadataList);
    this.writtenBytesCursor = 0;
    this.publishedBlockCount = 0;
  }

  /**
   * The block metadata in server side.
   * These information will be managed only for remote partitions.
   */
  final class BlockMetadataInServer {
    private final ControlMessage.BlockMetadataMsg blockMetadataMsg;
    private volatile boolean committed;

    private BlockMetadataInServer(final ControlMessage.BlockMetadataMsg blockMetadataMsg) {
      this.blockMetadataMsg = blockMetadataMsg;
      this.committed = false;
    }

    private boolean isCommitted() {
      return committed;
    }

    private void setCommitted() {
      committed = true;
    }

    ControlMessage.BlockMetadataMsg getBlockMetadataMsg() {
      return blockMetadataMsg;
    }
  }

  /**
   * The {@link io.reactivex.Observer} handling block subscription.
   */
  private final class CommittedBlockMetadataObserver implements io.reactivex.Observer<BlockMetadataInServer> {
    private final String executorId;
    private final String listenerId;
    private Disposable disposable;

    private CommittedBlockMetadataObserver(final String executorId,
                                           final String listenerId) {
      this.executorId = executorId;
      this.listenerId = listenerId;
    }

    @Override
    public synchronized void onSubscribe(@NonNull final Disposable givenDisposable) {
      this.disposable = givenDisposable;
    }

    @Override
    public synchronized void onNext(@NonNull final BlockMetadataInServer blockMetadataInServer) {
      final ExecutorRepresenter executorRepresenter = containerManager.getExecutorRepresenterMap().get(executorId);
      if (executorRepresenter == null) {
        disposable.dispose();
        LOG.warn("The container manager doesn't have the representer for the subscribing executor having listener {}."
            + "The subscribing executor may failed. Stop the subscription form this executor.", listenerId);
      } else {
        // Send the committed block metadata to the subscribing executor.
        final ControlMessage.Message message = ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(listenerId)
            .setType(ControlMessage.MessageType.CommittedBlockMetadata)
            .setCommittedBlockMetadataMsg(
                ControlMessage.CommittedBlockMetadataMsg.newBuilder()
                    .setBlockMetadataMsg(blockMetadataInServer.getBlockMetadataMsg())
                    .build())
            .build();
        executorRepresenter.sendControlMessage(message, listenerId);
      }
    }

    @Override
    public synchronized void onError(@NonNull final Throwable throwable) {
      LOG.error(throwable.toString());
      final ExecutorRepresenter executorRepresenter = containerManager.getExecutorRepresenterMap().get(executorId);
      if (executorRepresenter == null) {
        disposable.dispose();
        LOG.warn("The container manager doesn't have the representer for the subscribing executor having listener {}."
            + "The subscribing executor may failed. Stop the subscription form this executor.", listenerId);
      } else {
        // Notify the end of subscription with cause.
        final ControlMessage.Message message = ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(listenerId)
            .setType(ControlMessage.MessageType.CommittedBlockMetadata)
            .setCommittedBlockMetadataMsg(
                ControlMessage.CommittedBlockMetadataMsg.newBuilder()
                    .setError(throwable.toString())
                    .build())
            .build();
        executorRepresenter.sendControlMessage(message, listenerId);
      }
    }

    @Override
    public synchronized void onComplete() {
      LOG.debug("Committed block metadata subscription for the listener {} is completed.", listenerId);
      final ExecutorRepresenter executorRepresenter = containerManager.getExecutorRepresenterMap().get(executorId);
      if (executorRepresenter == null) {
        disposable.dispose();
        LOG.warn("The container manager doesn't have the representer for the subscribing executor having listener {}."
            + "The subscribing executor may failed. Stop the subscription form this executor.", listenerId);
      } else {
        // Notify the end of subscription with cause.
        final ControlMessage.Message message = ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(listenerId)
            .setType(ControlMessage.MessageType.CommittedBlockMetadata)
            .setCommittedBlockMetadataMsg(
                ControlMessage.CommittedBlockMetadataMsg.newBuilder().build())
            .build();
        executorRepresenter.sendControlMessage(message, listenerId);
      }
    }
  }
}
