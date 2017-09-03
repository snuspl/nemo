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
package edu.snu.vortex.runtime.executor.data;

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.executor.data.partition.FilePartition;
import org.apache.reef.tang.InjectionFuture;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Stores partitions in (local or remote) files.
 */
abstract class FileStore implements PartitionStore {

  private final int blockSizeInBytes;
  private final String fileDirectory;
  private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;

  protected FileStore(final int blockSizeInKb,
                      final String fileDirectory,
                      final InjectionFuture<PartitionManagerWorker> partitionManagerWorker) {
    this.blockSizeInBytes = blockSizeInKb * 1000;
    this.fileDirectory = fileDirectory;
    this.partitionManagerWorker = partitionManagerWorker;
  }

  /**
   * Gets the list of {@link FileArea}s for the specified partition.
   *
   * @param partitionId the partition id
   * @param hashRange   the hash range
   * @return the list of file areas
   */
  public abstract List<FileArea> getFileAreas(final String partitionId, final HashRange hashRange);

  /**
   * Makes the given stream to a block and write it to the given file partition.
   *
   * @param elementsInBlock the number of elements in this block.
   * @param outputStream    the output stream containing data.
   * @param partition       the partition to write the block.
   * @param hashVal         the hash value of the block.
   * @return the size of serialized block.
   * @throws IOException if fail to write.
   */
  private long writeBlock(final long elementsInBlock,
                          final ByteArrayOutputStream outputStream,
                          final FilePartition partition,
                          final int hashVal) throws IOException {
    outputStream.close();

    final byte[] serialized = outputStream.toByteArray();
    partition.writeBlock(serialized, elementsInBlock, hashVal);

    return serialized.length;
  }

  /**
   * Gets data coder from the {@link PartitionManagerWorker}.
   *
   * @param partitionId to get the coder.
   * @return the coder.
   */
  protected Coder getCoderFromWorker(final String partitionId) {
    final PartitionManagerWorker worker = partitionManagerWorker.get();
    final String runtimeEdgeId = RuntimeIdGenerator.parsePartitionId(partitionId)[0];
    return worker.getCoder(runtimeEdgeId);
  }

  /**
   * Serializes and puts the data blocks to a file partition.
   * It may divides each block into blocks according to it's size.
   *
   * @param coder      the coder used to serialize the data of this partition.
   * @param partition  to store this data.
   * @param blocks     to be stored.
   * @return the size of the data.
   * @throws IOException if fail to write the data.
   */
  protected List<Long> putBlocks(final Coder coder,
                                 final FilePartition partition,
                                 final Iterable<Block> blocks) throws IOException {
    final List<Long> blockSizeList = new ArrayList<>();
    // Serialize the given blocks
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    for (final Block block : blocks) {
      // Serialize the given data into blocks
      long blockSize = 0;
      long elementsInBlock = 0;
      for (final Element element : block.getData()) {
        coder.encode(element, outputStream);
        elementsInBlock++;

        if (outputStream.size() >= blockSizeInBytes) {
          // If this block is large enough, synchronously append it to the file and reset the buffer
          blockSize += writeBlock(elementsInBlock, outputStream, partition, block.getHashValue());

          outputStream.reset();
          elementsInBlock = 0;
        }
      }

      if (outputStream.size() > 0) {
        // If there are any remaining data in stream, write it as another block.
        blockSize += writeBlock(elementsInBlock, outputStream, partition, block.getHashValue());
      }

      blockSizeList.add(blockSize);
      outputStream.reset();
    }
    partition.flushMetadata();

    return blockSizeList;
  }

  /**
   * Converts a partition id to the corresponding file path.
   *
   * @param partitionId of the partition
   * @return the file path of the partition.
   */
  protected String partitionIdToFilePath(final String partitionId) {
    return fileDirectory + "/" + partitionId;
  }

  protected String getFileDirectory() {
    return fileDirectory;
  }
}
