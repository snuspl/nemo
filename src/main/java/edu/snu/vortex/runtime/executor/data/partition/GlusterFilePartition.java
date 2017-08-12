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
import edu.snu.vortex.runtime.executor.data.metadata.RemoteFileMetadata;

import java.io.*;
import java.nio.channels.FileLock;
import java.util.Optional;

/**
 * This class implements the {@link Partition} which is stored in a GlusterFS volume.
 * Because the data is stored in a remote file and globally accessed by multiple nodes,
 * each access (create - write - close, read, or deletion) for a file needs one instance of this partition.
 * It supports concurrent write for a single file, but each writer has to have separate instance of this class.
 * These accesses are judiciously synchronized by the metadata server in master.
 */
public final class GlusterFilePartition extends FilePartition {

  private boolean openedToWrite; // Whether this partition is opened for write or not.

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
    super(coder, dataFilePath, metadata);
    this.openedToWrite = false;
  }

  /**
   * Opens partition for writing. The corresponding {@link FilePartition#finishWrite()} is required.
   *
   * @throws IOException if fail to open this partition for writing.
   */
  private void openPartitionForWrite() throws IOException {
    openedToWrite = true;
    openFileStream();

    // Prevent concurrent write by using the file lock of this file.
    // If once this lock is acquired, it have to be released to prevent the locked leftover in the remote storage.
    // Because this lock will be released when the file channel is closed, we need to close the file channel well.
    final FileLock fileLock = fileChannel.tryLock();
    if (fileLock == null) {
      throw new IOException("Other thread (maybe in another node) is writing on this file.");
    }
  }

  /**
   * Writes the serialized data of this partition having a specific hash value as a block to the file
   * where this partition resides.
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
    if (!openedToWrite) {
      throw new IOException("Trying to write a block in a partition that has not been opened for write.");
    }
    super.writeBlock(serializedData, numElement, hashVal);
  }

  /**
   * @see FilePartition#finishWrite().
   */
  @Override
  public void finishWrite() throws IOException {
    if (!openedToWrite) {
      throw new IOException("Trying to finish writing a partition that has not been opened for write.");
    }
    super.finishWrite();
  }

  /**
   * Creates a file for this partition in the storage to write.
   * The corresponding {@link FilePartition#finishWrite()} for the returned partition is required.
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
