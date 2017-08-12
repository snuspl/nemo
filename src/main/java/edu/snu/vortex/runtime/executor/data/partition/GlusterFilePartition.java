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
import edu.snu.vortex.runtime.executor.data.metadata.RemoteFileMetadata;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This class implements the {@link Partition} which is stored in a GlusterFS volume.
 * Because the data is stored in a remote file and globally accessed by multiple nodes,
 * each access (create - write - close, read, or deletion) for a file needs one instance of this partition.
 * It supports concurrent write for a single file, but each writer has to have separate instance of this class.
 * These accesses are judiciously synchronized by the metadata server in master.
 */
public final class GlusterFilePartition implements FilePartition {

  private final Coder coder;
  private final String dataFilePath; // The path of the file that contains the actual data of this partition.
  private FileOutputStream dataFileOutputStream;
  private FileChannel dataFileChannel;
  private RemoteFileMetadata metadata;

  /**
   * Constructs a gluster file partition.
   *
   * @param coder        the coder used to serialize and deserialize the data of this partition.
   * @param dataFilePath the path of the file which will contain the data of this partition.
   * @param metadata     the metadata for this partition.
   */
  private GlusterFilePartition(final Coder coder,
                               final String dataFilePath,
                               final RemoteFileMetadata metadata) {
    this.coder = coder;
    this.dataFilePath = dataFilePath;
    this.metadata = metadata;
  }

  /**
   * Opens partition for writing. The corresponding {@link GlusterFilePartition#finishWrite()} is required.
   *
   * @throws IOException if fail to open this partition for writing.
   */
  private void openPartitionForWrite() throws IOException {
    dataFileOutputStream = new FileOutputStream(dataFilePath, true);
    dataFileChannel = dataFileOutputStream.getChannel();

    // Prevent concurrent write by using the file lock of this file.
    // If once this lock is acquired, it have to be released to prevent the locked leftover in the remote storage.
    // Because this lock will be released when the file channel is closed, we need to close the file channel well.
    final FileLock fileLock = dataFileChannel.tryLock();
    if (fileLock == null) {
      throw new IOException("Other thread (maybe in another node) is writing on this file.");
    }
  }

  /**
   * Writes the serialized data of this partition as a block to the file where this partition resides.
   * To maintain the block information globally,
   * the size and the number of elements of the block is stored before the data in the file.
   *
   * @param serializedData the serialized data of this partition.
   * @param numElement     the number of elements in the serialized data.
   * @throws IOException if fail to write.
   */
  @Override
  public void writeBlock(final byte[] serializedData,
                         final long numElement) throws IOException {
    writeBlock(serializedData, numElement, Integer.MIN_VALUE);
  }

  /**
   * Writes the serialized data of this partition having a specific hash value as a block to the file
   * where this partition resides.
   * To maintain the block information globally,
   * the size and the number of elements of the block is stored before the data in the file.
   *
   * @param serializedData the serialized data of this partition.
   * @param numElement     the number of elements in the serialized data.
   * @param hashVal        the hash value of this block.
   * @throws IOException if fail to write.
   */
  @Override
  public void writeBlock(final byte[] serializedData,
                         final long numElement,
                         final int hashVal) throws IOException {
    if (!metadata.isOpenedToWrite()) {
      throw new IOException("Trying to write a block in a partition that has not been opened for write.");
    }
    metadata.appendBlockMetadata(hashVal, serializedData.length, numElement);

    // Wrap the given serialized data (but not copy it)
    final ByteBuffer buf = ByteBuffer.wrap(serializedData);
    // Write synchronously
    dataFileChannel.write(buf);
  }

  /**
   * Notice the end of write.
   *
   * @throws IOException if fail to close.
   */
  public void finishWrite() throws IOException {
    if (!metadata.isOpenedToWrite()) {
      throw new IOException("Trying to finish writing a partition that has not been opened for write.");
    }
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
    if (dataFileChannel != null) {
      dataFileChannel.close();
    }
    if (dataFileOutputStream != null) {
      dataFileOutputStream.close();
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
    Files.delete(Paths.get(dataFilePath));
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
    try (final FileInputStream fileStream = new FileInputStream(dataFilePath)) {
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
   * @see Partition#asIterable().
   */
  @Override
  public Iterable<Element> asIterable() throws IOException {
    // Read file synchronously
    if (!metadata.isWritten()) {
      throw new IOException("This partition is not written yet.");
    }

    // Deserialize the data
    final ArrayList<Element> deserializedData = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(dataFilePath)) {
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

  /**
   * Creates a file for this partition in the storage to write.
   * The corresponding {@link GlusterFilePartition#finishWrite()} for the returned partition is required.
   *
   * @param coder    the coder used to serialize and deserialize the data of this partition.
   * @param filePath the path of the file which will contain the data of this partition.
   * @param metadata the metadata for this partition.
   * @return the corresponding partition.
   * @throws IOException if the file exist already.
   */
  public static GlusterFilePartition create(final Coder coder,
                                            final String filePath,
                                            final RemoteFileMetadata metadata) throws IOException {
    if (!new File(filePath).isFile()) {
      final GlusterFilePartition partition = new GlusterFilePartition(coder, filePath, metadata);
      partition.openPartitionForWrite();
      return partition;
    } else {
      throw new IOException("Trying to overwrite an existing partition.");
    }
  }

  /**
   * Opens the corresponding file for this partition in the storage to read.
   *
   * @param coder    the coder used to serialize and deserialize the data of this partition.
   * @param filePath the path of the file which will contain the data of this partition.
   * @param metadata the metadata for this partition.
   * @return the partition if success to open the file and partition, or an empty optional if the file does not exist.
   */
  public static Optional<GlusterFilePartition> open(final Coder coder,
                                                    final String filePath,
                                                    final RemoteFileMetadata metadata) {
    if (new File(filePath).isFile()) {
      return Optional.of(new GlusterFilePartition(coder, filePath, metadata));
    } else {
      return Optional.empty();
    }
  }
}
