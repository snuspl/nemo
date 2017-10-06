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
package edu.snu.vortex.runtime.executor.data.partition;

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.ObservableIterableWrapper;
import edu.snu.vortex.runtime.executor.data.HashRange;
import edu.snu.vortex.runtime.executor.data.metadata.BlockMetadata;
import edu.snu.vortex.runtime.executor.data.metadata.FileMetadata;
import edu.snu.vortex.runtime.executor.data.FileArea;
import edu.snu.vortex.runtime.executor.data.partition.observer.DeserializeBlockObserver;
import edu.snu.vortex.runtime.executor.data.partition.observer.FileAreaObserver;
import io.reactivex.schedulers.Schedulers;

import javax.annotation.concurrent.ThreadSafe;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

/**
 * This class represents a partition which is stored in (local or remote) file.
 */
@ThreadSafe
public final class FilePartition {
  private final Coder coder;
  private final String filePath;
  private final FileMetadata metadata;
  private final Queue<BlockMetadata> blockMetadataToCommit;
  private final boolean commitPerBlock;
  private final ExecutorService executorService; // Executor service for deserialization.

  public FilePartition(final Coder coder,
                       final String filePath,
                       final FileMetadata metadata,
                       final ExecutorService executorService) {
    this.coder = coder;
    this.filePath = filePath;
    this.metadata = metadata;
    this.blockMetadataToCommit = new ConcurrentLinkedQueue<>();
    this.commitPerBlock = metadata.isBlockCommitPerWrite();
    this.executorService = executorService;
  }

  /**
   * Writes the serialized data of this partition having a specific hash value as a block to the file
   * where this partition resides.
   *
   * @param serializedData the serialized data which will become a block.
   * @param numElement     the number of elements in the serialized data.
   * @param hashVal        the hash value of this block.
   * @throws IOException if fail to write.
   */
  public void writeBlock(final byte[] serializedData,
                         final long numElement,
                         final int hashVal) throws IOException {
    // Reserve a block write and get the metadata.
    final BlockMetadata blockMetadata = metadata.reserveBlock(hashVal, serializedData.length, numElement);

    try (
        final FileOutputStream fileOutputStream = new FileOutputStream(filePath, true);
        final FileChannel fileChannel = fileOutputStream.getChannel()
    ) {
      // Wrap the given serialized data (but not copy it) and write.
      fileChannel.position(blockMetadata.getOffset());
      final ByteBuffer buf = ByteBuffer.wrap(serializedData);
      fileChannel.write(buf);
    }

    // Commit if needed.
    if (commitPerBlock) {
      metadata.commitBlocks(Collections.singleton(blockMetadata));
    } else {
      blockMetadataToCommit.add(blockMetadata);
    }
  }

  /**
   * Commits the un-committed block metadata.
   */
  public void commitRemainderMetadata() {
    final List<BlockMetadata> metadataToCommit = new ArrayList<>();
    while (!blockMetadataToCommit.isEmpty()) {
      final BlockMetadata blockMetadata = blockMetadataToCommit.poll();
      if (blockMetadata != null) {
        metadataToCommit.add(blockMetadata);
      }
    }
    metadata.commitBlocks(metadataToCommit);
  }

  /**
   * Gets blocks having data in a specific {@link HashRange} from a partition and deserializes it.
   * The result will be an {@link Iterable}, and looking up for it's {@link java.util.Iterator} can be blocked.
   *
   * @param hashRange the hash range.
   * @return the future of the iterable of the (deserialized) blocks in a specific hash range of this partition.
   * @throws IOException if failed to deserialize.
   */
  public CompletableFuture<Iterable<Element>> getElementsInRange(final HashRange hashRange) throws IOException {
    final CompletableFuture<Iterable<Element>> iterableFuture = new CompletableFuture<>();
    final ObservableIterableWrapper<BlockMetadata> observableBlockMetadata =
        new ObservableIterableWrapper<>(metadata.getBlockMetadataIterable());
    observableBlockMetadata.subscribeOn(Schedulers.io()).subscribe(
        new DeserializeBlockObserver(hashRange, iterableFuture, filePath, coder, executorService));

    return iterableFuture;
  }

  /**
   * Retrieves the list of {@link FileArea}s for the specified {@link HashRange}.
   *
   * @param hashRange the hash range.
   * @return the future of the iterable of the file areas.
   */
  public CompletableFuture<Iterable<FileArea>> asFileAreas(final HashRange hashRange) {
    final CompletableFuture<Iterable<FileArea>> iterableFuture = new CompletableFuture<>();
    final ObservableIterableWrapper<BlockMetadata> observableBlockMetadata =
        new ObservableIterableWrapper<>(metadata.getBlockMetadataIterable());
    observableBlockMetadata.subscribeOn(Schedulers.io()).subscribe(
        new FileAreaObserver(hashRange, iterableFuture, filePath));

    return iterableFuture;
  }

  /**
   * Deletes the file that contains this partition data.
   * This method have to be called after all read is completed (or failed).
   *
   * @throws IOException if failed to delete.
   */
  public void deleteFile() throws IOException {
    Files.delete(Paths.get(filePath));
  }

  /**
   * Commits this partition to prevent further write.
   * If someone "subscribing" the data in this partition, it will be finished.
   */
  public void commit() {
    commitRemainderMetadata();
    metadata.commitPartition();
  }
}
