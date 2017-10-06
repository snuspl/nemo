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
package edu.snu.vortex.runtime.executor.data.metadata;

import edu.snu.vortex.runtime.common.ClosableBlockingIterable;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageContext;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageListener;
import edu.snu.vortex.runtime.exception.IllegalMessageException;
import edu.snu.vortex.runtime.exception.PartitionFetchException;
import edu.snu.vortex.runtime.exception.PartitionWriteException;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMasterMap;
import edu.snu.vortex.runtime.master.RuntimeMaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents a metadata for a remote file partition.
 * Because the data is stored in a remote file and globally accessed by multiple nodes,
 * each access (create - write - close, read, or deletion) for a partition needs one instance of this metadata.
 * Concurrent write for a single file is supported, but each writer in different executor
 * has to have separate instance of this class.
 * It supports concurrent write for a single partition, but each writer has to have separate instance of this class.
 * These accesses are judiciously synchronized by the metadata server in master.
 */
@ThreadSafe
public final class RemoteFileMetadata extends FileMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteFileMetadata.class.getName());
  private final String partitionId;
  private final String committedBlockListenerId;
  private final String executorId;
  private final PersistentConnectionToMasterMap connectionToMaster;
  private final MessageEnvironment messageEnvironment;
  private final ClosableBlockingIterable<BlockMetadata> blockMetadataIterable;
  // Whether the block metadata request for this file is sent or not.
  private final AtomicBoolean requestedBlockMetadata;

  /**
   * Opens a partition metadata.
   * TODO #410: Implement metadata caching for the RemoteFileMetadata. Sustain only a single listener at the same time.
   *
   * @param commitPerBlock     whether commit every block write or not.
   * @param partitionId        the id of the partition.
   * @param executorId         the id of the executor.
   * @param connectionToMaster the connection for sending messages to master.
   * @param messageEnvironment the message environment.
   */
  public RemoteFileMetadata(final boolean commitPerBlock,
                            final String partitionId,
                            final String executorId,
                            final PersistentConnectionToMasterMap connectionToMaster,
                            final MessageEnvironment messageEnvironment) {
    super(commitPerBlock);
    this.partitionId = partitionId;
    this.committedBlockListenerId = RuntimeIdGenerator.generateCommittedBlockListenerId(partitionId);
    this.executorId = executorId;
    this.connectionToMaster = connectionToMaster;
    this.messageEnvironment = messageEnvironment;
    this.blockMetadataIterable = new ClosableBlockingIterable<>();
    this.requestedBlockMetadata = new AtomicBoolean(false);
    messageEnvironment.setupListener(committedBlockListenerId, new CommittedBlockMessageListener());
  }

  /**
   * Reserves the region for a block and get the metadata for the block.
   *
   * @see FileMetadata#reserveBlock(int, int, long).
   */
  @Override
  public synchronized BlockMetadata reserveBlock(final int hashValue,
                                                 final int blockSize,
                                                 final long elementsTotal) throws PartitionWriteException {
    // Convert the block metadata to a block metadata message (without offset).
    final ControlMessage.BlockMetadataMsg blockMetadataMsg =
        ControlMessage.BlockMetadataMsg.newBuilder()
            .setHashValue(hashValue)
            .setBlockSize(blockSize)
            .setNumElements(elementsTotal)
            .build();

    // Send the block metadata to the metadata server in the master and ask where to store the block.
    final CompletableFuture<ControlMessage.Message> reserveBlockResponseFuture =
        connectionToMaster.getMessageSender(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID).request(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setListenerId(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.ReserveBlock)
                .setReserveBlockMsg(
                    ControlMessage.ReserveBlockMsg.newBuilder()
                        .setExecutorId(executorId)
                        .setPartitionId(partitionId)
                        .setBlockMetadata(blockMetadataMsg))
                .build());

    // Get the response from the metadata server.
    final ControlMessage.Message responseFromMaster;
    try {
      responseFromMaster = reserveBlockResponseFuture.get();
    } catch (final InterruptedException | ExecutionException e) {
      throw new PartitionWriteException(e);
    }

    assert (responseFromMaster.getType() == ControlMessage.MessageType.ReserveBlockResponse);
    final ControlMessage.ReserveBlockResponseMsg reserveBlockResponseMsg =
        responseFromMaster.getReserveBlockResponseMsg();
    if (!reserveBlockResponseMsg.hasPositionToWrite()) {
      throw new PartitionWriteException(new Throwable("Cannot append the block metadata."));
    }
    final int blockIndex = reserveBlockResponseMsg.getBlockIdx();
    final long positionToWrite = reserveBlockResponseMsg.getPositionToWrite();
    return new BlockMetadata(blockIndex, hashValue, blockSize, positionToWrite, elementsTotal);
  }

  /**
   * Notifies that some blocks are written.
   *
   * @see FileMetadata#commitBlocks(Iterable).
   */
  @Override
  public synchronized void commitBlocks(final Iterable<BlockMetadata> blockMetadataToCommit) {
    final List<Integer> blockIndices = new ArrayList<>();
    blockMetadataToCommit.forEach(blockMetadata -> {
      blockMetadata.setCommitted();
      blockIndices.add(blockMetadata.getBlockIdx());
    });

    // Notify that these blocks are committed to the metadata server.
    connectionToMaster.getMessageSender(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID).send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.CommitBlock)
            .setCommitBlockMsg(
                ControlMessage.CommitBlockMsg.newBuilder()
                    .setPartitionId(partitionId)
                    .addAllBlockIdx(blockIndices))
            .build());
  }

  /**
   * Gets an iterable containing the block metadata of corresponding partition.
   *
   * @see FileMetadata#getBlockMetadataIterable().
   */
  @Override
  public synchronized Iterable<BlockMetadata> getBlockMetadataIterable() {
    if (!requestedBlockMetadata.getAndSet(true)) {
      requestBlockMetadataToServer();
    }
    return blockMetadataIterable;
  }

  /**
   * Notifies that all writes are finished for the partition corresponding to this metadata.
   * Subscribers waiting for the data of the target partition are notified when the partition is committed.
   * Also, further subscription about a committed partition will not blocked but get the data in it and finished.
   */
  @Override
  public synchronized void commitPartition() {
    // Do nothing. It will be handled by the metadata server.
  }

  /**
   * Stop the committed block subscription.
   */
  private void stopSubscription() {
    messageEnvironment.removeListener(committedBlockListenerId);
    blockMetadataIterable.close();
  }

  /**
   * Requests the committed block metadata to the metadata server.
   * The server will publish all committed blocks through {@link CommittedBlockMessageListener}.
   *
   * @throws edu.snu.vortex.runtime.exception.PartitionFetchException if fail to get the metadata.
   */
  private void requestBlockMetadataToServer() {
    // Ask the metadata server in the master for the metadata
    final CompletableFuture<ControlMessage.Message> metadataResponseFuture =
        connectionToMaster.getMessageSender(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID).request(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setListenerId(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.RequestBlockMetadata)
                .setRequestBlockMetadataMsg(
                    ControlMessage.RequestBlockMetadataMsg.newBuilder()
                        .setExecutorId(executorId)
                        .setPartitionId(partitionId)
                        .setListenerId(committedBlockListenerId)
                        .build())
                .build());

    final ControlMessage.Message responseFromMaster;
    try {
      responseFromMaster = metadataResponseFuture.get();
    } catch (final InterruptedException | ExecutionException e) {
      throw new PartitionFetchException(e);
    }

    assert (responseFromMaster.getType() == ControlMessage.MessageType.MetadataResponse);
    final ControlMessage.MetadataResponseMsg metadataResponseMsg = responseFromMaster.getMetadataResponseMsg();
    if (metadataResponseMsg.hasState()) {
      // Response has an exception state.
      stopSubscription();
      throw new PartitionFetchException(new Throwable(
          "Cannot get the metadata of partition " + partitionId + " from the metadata server: "
              + "The partition state is " + RuntimeMaster.convertPartitionState(metadataResponseMsg.getState())));
    }
  }

  /**
   * Handler for committed block metadata messages received.
   */
  public final class CommittedBlockMessageListener implements MessageListener<ControlMessage.Message> {

    private volatile int blockIdx;

    private CommittedBlockMessageListener() {
      this.blockIdx = 0;
    }

    @Override
    public synchronized void onMessage(final ControlMessage.Message message) {
      switch (message.getType()) {
        case CommittedBlockMetadata:
          final ControlMessage.CommittedBlockMetadataMsg committedBlockMetadataMsg =
              message.getCommittedBlockMetadataMsg();
          if (committedBlockMetadataMsg.hasBlockMetadataMsg()) {
            // Construct the block metadata from the response.
            final ControlMessage.BlockMetadataMsg blockMetadataMsg = committedBlockMetadataMsg.getBlockMetadataMsg();
            if (!blockMetadataMsg.hasOffset()) {
              stopSubscription();
              throw new IllegalMessageException(new Throwable(
                  "The metadata of a block in the " + partitionId + " does not have offset value."));
            }

            final BlockMetadata blockMetadata = new BlockMetadata(blockIdx, blockMetadataMsg.getHashValue(),
                blockMetadataMsg.getBlockSize(), blockMetadataMsg.getOffset(), blockMetadataMsg.getNumElements());
            blockMetadata.setCommitted();
            blockIdx++;
            blockMetadataIterable.add(blockMetadata);
          } else if (committedBlockMetadataMsg.hasError()) {
            // The subscription is ended with an error.
            stopSubscription();
            throw new RuntimeException(new Throwable(
                "Block subscription is completed with cause: " + committedBlockMetadataMsg.getError()));
          } else {
            // The subscription is completed because all blocks for the partition is published.
            LOG.debug(
                "Committed block metadata subscription for the listener {} is completed.", committedBlockListenerId);
            stopSubscription();
          }
          break;
        default:
          throw new IllegalMessageException(
              new Exception("This message should not be received by "
                  + RemoteFileMetadata.class.getName() + ":" + message.getType()));
      }
    }

    @Override
    public synchronized void onMessageWithContext(final ControlMessage.Message message,
                                                  final MessageContext messageContext) {
      throw new IllegalMessageException(
          new Exception("This message should not be received by "
              + RemoteFileMetadata.class.getName() + ":" + message.getType()));
    }
  }
}
