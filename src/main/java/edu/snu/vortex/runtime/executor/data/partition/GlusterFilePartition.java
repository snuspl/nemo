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

import javax.annotation.Nullable;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Optional;

/**
 * This class implements the {@link Partition} which is stored in a GlusterFS volume.
 * Because the data is stored in a remote file and globally accessed by multiple nodes,
 * one instance of this class is needed for each access (create - write - close, read, or deletion) for the file,
 * and each access have to be judiciously synchronized with the {@link FileLock}.
 * To be specific, writing and deleting whole partition have to be done atomically and not interrupted by read.
 */
public final class GlusterFilePartition implements FilePartition {

  private final Coder coder;
  private final String filePath;
  private FileOutputStream fileOutputStream;
  private FileChannel fileChannel;
  /**
   * The lock which synchronize the create - write - close process from read and deletion.
   * May be null if this partition is not being written.
   * If once this lock is acquired, it have to be released to prevent the locked leftover in the remote storage.
   */
  @Nullable
  private FileLock fileWriteLock;

  /**
   * Constructs a gluster file partition.
   *
   * @param coder    the coder used to serialize and deserialize the data of this partition.
   * @param filePath the path of the file which will contain the data of this partition.
   */
  private GlusterFilePartition(final Coder coder,
                               final String filePath) {
    this.coder = coder;
    this.filePath = filePath;
  }

  /**
   * Opens partition for writing. The corresponding {@link GlusterFilePartition#finishWrite()} is required.
   *
   * @throws IOException if fail to open this partition for writing.
   */
  private void openPartitionForWrite() throws IOException {
    fileOutputStream = new FileOutputStream(filePath, true);
    fileChannel = fileOutputStream.getChannel();
    fileWriteLock = fileChannel.lock();
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
    try {
      // Store the block information.
      try (final DataOutputStream dataOutputStream = new DataOutputStream(fileOutputStream)) {
        dataOutputStream.writeInt(serializedData.length);
        dataOutputStream.writeLong(numElement);
      }

      // Wrap the given serialized data (but not copy it)
      final ByteBuffer buf = ByteBuffer.wrap(serializedData);
      // Write synchronously
      fileChannel.write(buf);
    } catch (final Exception e) {
      // If any exception is occurred during write, the write lock have to be released.
      if (fileWriteLock != null) {
        fileWriteLock.release();
      } else {
        throw new IOException("This method is not following GlusterFilePartition#openPartitionForWrite() method");
      }
      throw e;
    }
  }

  /**
   * Notice the end of write.
   *
   * @throws IOException if fail to close.
   */
  public void finishWrite() throws IOException {
    if (fileWriteLock != null) {
      fileWriteLock.release();
    } else {
      throw new IOException("This method is not following GlusterFilePartition#openPartitionForWrite() method");
    }
    fileChannel.close();
    fileOutputStream.close();
  }

  @Override
  public void deleteFile() throws IOException {
    FileLock fileLock = null;
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      fileLock = fileStream.getChannel().lock();
      Files.delete(Paths.get(filePath));
    } finally {
      if (fileLock != null) {
        fileLock.release();
      }
    }
  }

  @Override
  public Iterable<Element> asIterable() throws IOException {
    // Deserialize the data
    final ArrayList<Element> deserializedData = new ArrayList<>();
    try (
        final FileInputStream fileStream = new FileInputStream(filePath);
        final DataInputStream dataInputStream = new DataInputStream(fileStream)
    ) {
      // We have to check whether this file is not being written or deleted.
      final FileLock fileLock = fileStream.getChannel().lock();
      // However, if not, we don't need to read synchronously.
      fileLock.release();

      while (fileStream.available() > 0) {
        // Read the block information
        final int serializedDataLength = dataInputStream.readInt();
        final long numElements = dataInputStream.readLong();

        if (serializedDataLength != 0) {
          // This stream will be not closed, but it is okay as long as the file stream is closed well.
          final BufferedInputStream bufferedInputStream = new BufferedInputStream(fileStream, serializedDataLength);
          for (int i = 0; i < numElements; i++) {
            deserializedData.add(coder.decode(bufferedInputStream));
          }
        }
      }
    }

    return deserializedData;
  }

  /**
   * Creates a file for this partition in the storage.
   * The corresponding {@link GlusterFilePartition#finishWrite()} for the returned partition is required.
   *
   * @param coder    the coder used to serialize and deserialize the data of this partition.
   * @param filePath the path of the file which will contain the data of this partition.
   * @return the corresponding partition.
   * @throws IOException if the file exist already.
   */
  public static GlusterFilePartition create(final Coder coder,
                                            final String filePath) throws IOException {
    if (!new File(filePath).isFile()) {
      final GlusterFilePartition partition = new GlusterFilePartition(coder, filePath);
      partition.openPartitionForWrite();
      return partition;
    } else {
      throw new IOException("Trying to overwrite an existing partition");
    }
  }

  /**
   * Opens the corresponding file for this partition in the storage.
   *
   * @param coder    the coder used to serialize and deserialize the data of this partition.
   * @param filePath the path of the file which will contain the data of this partition.
   * @return the partition if success to open the file and partition, or an empty optional if the file does not exist.
   */
  public static Optional<GlusterFilePartition> open(final Coder coder,
                                                    final String filePath) {
    if (new File(filePath).isFile()) {
      return Optional.of(new GlusterFilePartition(coder, filePath));
    } else {
      return Optional.empty();
    }
  }
}
