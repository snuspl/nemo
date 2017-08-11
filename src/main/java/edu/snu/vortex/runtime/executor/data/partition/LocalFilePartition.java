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
import edu.snu.vortex.runtime.executor.data.metadata.BlockMetadata;
import edu.snu.vortex.runtime.executor.data.metadata.FileMetadata;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * This class implements the {@link FilePartition} which is stored in a local file.
 * This partition have to be treated as an actual file
 * (i.e., construction and removal of this partition means the creation and deletion of the file),
 * even though the actual data is stored only in the local disk.
 * Also, to prevent the memory leak, this partition have to be closed when any exception is occurred during write.
 */
public final class LocalFilePartition implements FilePartition {

  private final Coder coder;
  private final String filePath;
  private FileOutputStream fileOutputStream;
  private FileChannel fileChannel;
  private FileMetadata metadata;

  /**
   * Constructs a local file partition.
   * Corresponding {@link LocalFilePartition#finishWrite()} is required.
   *
   * @param coder    the coder used to serialize and deserialize the data of this partition.
   * @param filePath the path of the file which will contain the data of this partition.
   * @throws IOException if fail to open this partition to write.
   */
  public LocalFilePartition(final Coder coder,
                            final String filePath,
                            final FileMetadata metadata) throws IOException {
    this.coder = coder;
    this.filePath = filePath;
    this.metadata = metadata;
    fileOutputStream = new FileOutputStream(filePath, true);
    fileChannel = fileOutputStream.getChannel();
  }

  /**
   * @see FilePartition#writeBlock(byte[], long).
   */
  @Override
  public void writeBlock(final byte[] serializedData,
                         final long numElement) throws IOException {
    writeBlock(serializedData, numElement, Integer.MIN_VALUE);
  }

  /**
   * @see FilePartition#writeBlock(byte[], long, int).
   */
  @Override
  public synchronized void writeBlock(final byte[] serializedData,
                                      final long numElement,
                                      final int hashVal) throws IOException {
    metadata.appendBlockMetadata(hashVal, serializedData.length, numElement);

    // Wrap the given serialized data (but not copy it)
    final ByteBuffer buf = ByteBuffer.wrap(serializedData);

    // Write synchronously
    fileChannel.write(buf);
  }

  /**
   * Notice the end of write.
   *
   * @throws IOException if fail to close.
   */
  public void finishWrite() throws IOException {
    if (metadata.getAndSetWritten()) {
      throw new IOException("Trying to finish writing that has been already finished.");
    }
    this.close();
  }

  /**
   * Closes the file channel and stream if opened.
   * It does not mean that this partition becomes invalid, but just cannot be written anymore.
   *
   * @throws IOException if fail to close.
   */
  @Override
  public void close() throws IOException {
    if (fileChannel != null) {
      fileChannel.close();
    }
    if (fileOutputStream != null) {
      fileOutputStream.close();
    }
  }

  /**
   * @see FilePartition#deleteFile().
   */
  @Override
  public void deleteFile() throws IOException {
    if (!metadata.isWritten()) {
      throw new IOException("This partition is not written yet.");
    }
    metadata.deleteMetadata();
    Files.delete(Paths.get(filePath));
  }

  /**
   * @see FilePartition#retrieveInHashRange(int, int);
   */
  @Override
  public Iterable<Element> retrieveInHashRange(final int hashRangeStartVal,
                                               final int hashRangeEndVal) throws IOException {
    // Check whether this partition is fully written and sorted by the hash value.
    if (!metadata.isWritten()) {
      throw new IOException("This partition is not written yet.");
    } else if (!metadata.isHashed()) {
      throw new IOException("The blocks in this partition are not hashed.");
    }

    // Deserialize the data
    final ArrayList<Element> deserializedData = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      for (final BlockMetadata blockMetadata : metadata.getBlockMetadataList()) {
        final int hashVal = blockMetadata.getHashValue();
        if (hashVal >= hashRangeStartVal && hashVal < hashRangeEndVal) {
          // The hash value of this block is in the range.
          deserializeBlock(blockMetadata, fileStream, deserializedData);
        } else {
          // Have to skip this block.
          final long bytesToSkip = blockMetadata.getBlockSize();
          final long skippedBytes = fileStream.skip(bytesToSkip);
          if (skippedBytes != bytesToSkip) {
            throw new IOException("The file stream failed to skip to the next block.");
          }
        }
      }
    }

    return deserializedData;
  }

  /**
   * Read the data of this partition from the file and deserialize it.
   *
   * @return an iterable of deserialized data.
   * @throws IOException if fail to deserialize.
   */
  @Override
  public Iterable<Element> asIterable() throws IOException {
    // Read file synchronously
    if (!metadata.isWritten()) {
      throw new IOException("This partition is not written yet.");
    }

    // Deserialize the data
    final ArrayList<Element> deserializedData = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      metadata.getBlockMetadataList().forEach(blockInfo -> {
        deserializeBlock(blockInfo, fileStream, deserializedData);
      });
    }

    return deserializedData;
  }

  /**
   * Reads and deserializes a block.
   *
   * @param blockMetadata    the block metadata.
   * @param fileInputStream  the stream contains the actual data.
   * @param deserializedData the list of elements to put the deserialized data.
   * @throws IOException if fail to read and deserialize.
   */
  private void deserializeBlock(final BlockMetadata blockMetadata,
                                final FileInputStream fileInputStream,
                                final List<Element> deserializedData) {
    final int size = blockMetadata.getBlockSize();
    final long numElements = blockMetadata.getNumElements();
    if (size != 0) {
      // This stream will be not closed, but it is okay as long as the file stream is closed well.
      final BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream, size);
      for (int i = 0; i < numElements; i++) {
        deserializedData.add(coder.decode(bufferedInputStream));
      }
    }
  }
}
