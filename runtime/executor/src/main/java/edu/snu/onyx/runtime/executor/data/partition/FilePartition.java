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
package edu.snu.onyx.runtime.executor.data.partition;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.runtime.executor.data.Block;
import edu.snu.onyx.runtime.executor.data.DataUtil;
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.FileArea;
import edu.snu.onyx.runtime.executor.data.metadata.BlockMetadata;
import edu.snu.onyx.runtime.executor.data.metadata.FileMetadata;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class represents a partition which is stored in (local or remote) file.
 */
public final class FilePartition implements Partition {

  private final Coder coder;
  private final String filePath;
  private final FileMetadata metadata;
  private final Queue<BlockMetadata> blockMetadataToCommit;
  private final boolean commitPerBlock;

  public FilePartition(final Coder coder,
                       final String filePath,
                       final FileMetadata metadata) {
    this.coder = coder;
    this.filePath = filePath;
    this.metadata = metadata;
    this.blockMetadataToCommit = new ConcurrentLinkedQueue<>();
    this.commitPerBlock = metadata.isBlockCommitPerWrite();
  }

  /**
   * Writes the serialized data of this partition having a specific hash value as a block to the file
   * where this partition resides.
   * Invariant: This method not support concurrent write for a single partition.
   *            Only one thread have to write at once.
   *
   * @param serializedBlocks the iterable of the serialized blocks to write.
   * @throws IOException if fail to write.
   */
  private void writeSerializedBlocks(final Iterable<Block> serializedBlocks) throws IOException {
    try (final FileOutputStream fileOutputStream = new FileOutputStream(filePath, true)) {
      for (final Block serializedBlock : serializedBlocks) {
        // Reserve a block write and get the metadata.
        final BlockMetadata blockMetadata = metadata.reserveBlock(
            serializedBlock.getKey(), serializedBlock.getSerializedData().length, serializedBlock.getElementsTotal());
        fileOutputStream.write(serializedBlock.getSerializedData());

        // Commit if needed.
        if (commitPerBlock) {
          metadata.commitBlocks(Collections.singleton(blockMetadata));
        } else {
          blockMetadataToCommit.add(blockMetadata);
        }
      }
    }
  }

  /**
   * Writes {@link Block}s to this partition.
   *
   * @param blocksToStore the {@link Block}s to write.
   * @throws IOException if fail to write.
   */
  @Override
  public Optional<List<Long>> putBlocks(final Iterable<Block> blocksToStore) throws IOException {
    // If there is any non-serialized block in the iterable, serialize it.
    final Iterable<Block> convertedBlocks = DataUtil.convertToSerBlocks(coder, blocksToStore);

    final List<Long> blockSizeList = new ArrayList<>();
    for (final Block convertedBlock : convertedBlocks) {
      blockSizeList.add((long) convertedBlock.getSerializedData().length);
    }
    writeSerializedBlocks(convertedBlocks);
    commitRemainderMetadata();

    return Optional.of(blockSizeList);
  }

  /**
   * Commits the un-committed block metadata.
   */
  private void commitRemainderMetadata() {
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
   * Retrieves the blocks of this partition from the file in a specific hash range and deserializes it.
   *
   * @param hashRange the hash range
   * @param serialize whether to get the {@link Block}s in a serialized form or not.
   * @return an iterable of {@link Block}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<Block> getBlocks(final HashRange hashRange,
                                   final boolean serialize) throws IOException {
    final List<Block> blocksInRange = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath);
         final BufferedInputStream bufferedInputStream = new BufferedInputStream(fileStream)) {
      for (final BlockMetadata blockMetadata : metadata.getBlockMetadataIterable()) {
        final int hashVal = blockMetadata.getHashValue();
        if (hashRange.includes(hashVal)) {
          // The hash value of this block is in the range.
          if (serialize) {
            final byte[] serializedData = new byte[blockMetadata.getBlockSize()];
            final int readBytes = fileStream.read(serializedData);
            if (readBytes != serializedData.length) {
              throw new IOException("The read data size does not match with the block size.");
            }
            blocksInRange.add(new Block(hashVal, blockMetadata.getElementsTotal(), serializedData));
          } else {
            final List deserializedData =
                DataUtil.deserializeBlock(blockMetadata.getElementsTotal(), coder, bufferedInputStream);
            blocksInRange.add(new Block(hashVal, deserializedData));
          }
        } else {
          // Have to skip this block.
          long bytesToSkip = blockMetadata.getBlockSize();
          while (bytesToSkip > 0) {
            final long skippedBytes = bufferedInputStream.skip(bytesToSkip);
            bytesToSkip -= skippedBytes;
            if (skippedBytes <= 0) {
              throw new IOException("The file stream failed to skip to the next block.");
            }
          }
        }
      }
    }

    return blocksInRange;
  }

  /**
   * Retrieves the list of {@link FileArea}s for the specified {@link HashRange}.
   *
   * @param hashRange the hash range
   * @return list of the file areas
   * @throws IOException if failed to open a file channel
   */
  public List<FileArea> asFileAreas(final HashRange hashRange) throws IOException {
    final List<FileArea> fileAreas = new ArrayList<>();
    for (final BlockMetadata blockMetadata : metadata.getBlockMetadataIterable()) {
      if (hashRange.includes(blockMetadata.getHashValue())) {
        fileAreas.add(new FileArea(filePath, blockMetadata.getOffset(), blockMetadata.getBlockSize()));
      }
    }
    return fileAreas;
  }

  /**
   * Deletes the file that contains this partition data.
   * This method have to be called after all read is completed (or failed).
   *
   * @throws IOException if failed to delete.
   */
  public void deleteFile() throws IOException {
    metadata.deleteMetadata();
    Files.delete(Paths.get(filePath));
  }

  /**
   * Commits this partition to prevent further write.
   */
  @Override
  public void commit() {
    commitRemainderMetadata();
    metadata.commitPartition();
  }
}
