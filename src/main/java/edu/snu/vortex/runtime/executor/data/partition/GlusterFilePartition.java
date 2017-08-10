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
 * These accesses are judiciously synchronized with the {@link FileLock} in here.
 */
public final class GlusterFilePartition implements FilePartition {

  private final Coder coder;
  private final boolean openedToWrite; // Whether this partition is opened for write or not.
  /**
   * A single partition will have two separate files: actual data file and metadata file.
   * The actual data file will only have the serialized data.
   * The metadata file will contain the information for this data,
   * such as whether whole data is written, each block in the partition has a single hash value or not,
   * the size, offset, and number of elements in each block, etc.
   * <p>
   * /////Meta data File/////
   * /       Written        /
   * /       Hashed         /
   * ........................
   * /     hash value       /
   * /       offset         /
   * /     Block size       /
   * /    # of elements     /
   * ........................
   * /          .           /
   * /          .           /
   * /          .           /
   * ........................
   * /     hash value       /
   * /       offset         /
   * /     Block size       /
   * /    # of elements     /
   * ////////////////////////
   */
  private final String dataFilePath; // The path of the file that contains the actual data of this partition.
  private final String metaFilePath; // The path of the file that contains the metadata for this partition.
  private FileOutputStream dataFileOutputStream;
  private FileChannel dataFileChannel;
  private FileOutputStream metaFileOutputStream;
  private DataOutputStream metaFilePrimOutputStream; // The stream to store primitive values to the metadata file.

  // hash value (int) + length (int) + # of elements (long) + offset (long) = 24 bytes.
  private static int blockMetadataSize = 24;

  /**
   * Constructs a gluster file partition.
   *
   * @param coder         the coder used to serialize and deserialize the data of this partition.
   * @param dataFilePath  the path of the file which will contain the data of this partition.
   * @param openedToWrite whether this partition is opened for write or not.
   */
  private GlusterFilePartition(final Coder coder,
                               final String dataFilePath,
                               final boolean openedToWrite) {
    this.coder = coder;
    this.openedToWrite = openedToWrite;
    this.dataFilePath = dataFilePath;
    this.metaFilePath = dataFilePath + "-metadata";
  }

  /**
   * Opens partition for writing. The corresponding {@link GlusterFilePartition#finishWrite()} is required.
   *
   * @param hashed whether each block in this partition has a single hash value or not.
   * @throws IOException if fail to open this partition for writing.
   */
  private void openPartitionForWrite(final boolean hashed) throws IOException {
    dataFileOutputStream = new FileOutputStream(dataFilePath, true);
    dataFileChannel = dataFileOutputStream.getChannel();
    metaFileOutputStream = new FileOutputStream(metaFilePath, true);
    metaFilePrimOutputStream = new DataOutputStream(metaFileOutputStream);

    // Prevent concurrent write by using the file lock of this file.
    // If once this lock is acquired, it have to be released to prevent the locked leftover in the remote storage.
    // Because this lock will be released when the file channel is closed, we need to close the file channel well.
    final FileLock fileLock = dataFileChannel.tryLock();
    if (fileLock == null) {
      throw new IOException("Other thread (maybe in another node) is writing on this file.");
    }
    try (final FileInputStream metaFileInputStream = new FileInputStream(metaFilePath)) {
      if (metaFileInputStream.available() > 0) {
        // Very rare case: multiple threads get into this method and one thread conducted
        // the create - write - close process and released the file lock before this thread tried to lock.
        throw new IOException("This partition is written and the file lock is released already.");
      }
    }
    metaFilePrimOutputStream.writeBoolean(false); // Not written yet.
    metaFilePrimOutputStream.writeBoolean(hashed); // Each block has a single hash value or not.
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
    if (!openedToWrite) {
      throw new IOException("Trying to write a block in a partition that has not been opened for write.");
    }
    // Store the block information to the metadata file.
    metaFilePrimOutputStream.writeInt(hashVal); // The offset of this block.
    metaFilePrimOutputStream.writeLong(dataFileChannel.position()); // Current data file position.
    metaFilePrimOutputStream.writeInt(serializedData.length); // The block size.
    metaFilePrimOutputStream.writeLong(numElement); // The number of elements in this block.

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
    if (!openedToWrite) {
      throw new IOException("Trying to finish writing a partition that has not been opened for write.");
    }

    // Make the written boolean true to notice that the write finished.
    try (
        final RandomAccessFile metadataFile = new RandomAccessFile(metaFilePath, "rws");
    ) {
      metadataFile.writeBoolean(true);
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
    if (metaFileOutputStream != null) {
      metaFileOutputStream.close();
    }
    if (metaFilePrimOutputStream != null) {
      metaFilePrimOutputStream.close();
    }
  }

  /**
   * @see FilePartition#deleteFile().
   */
  @Override
  public void deleteFile() throws IOException {
    try (final FileInputStream metaFileInputStream = new FileInputStream(metaFilePath);
         final DataInputStream metaFilePrimInputStream = new DataInputStream(metaFileInputStream)
    ) {
      final boolean written = metaFilePrimInputStream.readBoolean(); // Whether the whole data is written or not.
      if (!written) {
        throw new IOException("This partition is not written yet.");
      }
      Files.delete(Paths.get(dataFilePath));
      Files.delete(Paths.get(metaFilePath));
    }
  }

  /**
   * @see FilePartition#retrieveInHashRange(int, int);
   */
  @Override
  public Iterable<Element> retrieveInHashRange(final int hashRangeStartVal,
                                               final int hashRangeEndVal) throws IOException {
    final ArrayList<Element> deserializedData = new ArrayList<>();
    try (
        final FileInputStream fileInputStream = new FileInputStream(dataFilePath);
        final FileInputStream metaFileInputStream = new FileInputStream(metaFilePath);
        final DataInputStream metaFilePrimInputStream = new DataInputStream(metaFileInputStream)
    ) {
      // We have to check whether the write for this file finished or not.
      final boolean written = metaFilePrimInputStream.readBoolean(); // Whether the whole data is written or not.
      if (!written) {
        throw new IOException("This partition is not written yet.");
      }

      // We have to check whether each block in this partition has a single hash value or not.
      final boolean hashed = metaFilePrimInputStream.readBoolean();
      if (!hashed) {
        throw new IOException("The blocks in this partition are not hashed.");
      }

      while (metaFileInputStream.available() > 0) {
        final int hashVal = metaFilePrimInputStream.readInt();
        final int skippedOffsetBytes = metaFilePrimInputStream.skipBytes(8); // Skip the offset metadata.
        if (skippedOffsetBytes != 8) {
          throw new IOException("The metadata input stream cannot skip the \"offset\" metadata.");
        }
        final int serializedDataLength = metaFilePrimInputStream.readInt();
        final long numElements = metaFilePrimInputStream.readLong();
        if (hashVal >= hashRangeStartVal && hashVal < hashRangeEndVal) {
          // The hash value of this block is in the range.
          deserializeBlock(serializedDataLength, numElements, fileInputStream, deserializedData);
        } else {
          // Have to skip this block.
          final long skippedBytes = fileInputStream.skip(serializedDataLength); // Skip to the next block.
          if (skippedBytes != serializedDataLength) {
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
    final ArrayList<Element> deserializedData = new ArrayList<>();
    try (
        final FileInputStream fileInputStream = new FileInputStream(dataFilePath);
        final FileInputStream metaFileInputStream = new FileInputStream(metaFilePath);
        final DataInputStream metaFilePrimInputStream = new DataInputStream(metaFileInputStream)
    ) {
      // We have to check whether the write for this file finished or not.
      final boolean written = metaFilePrimInputStream.readBoolean(); // Whether the whole data is written or not.
      if (!written) {
        throw new IOException("This partition is not written yet.");
      }
      // We don't need to know whether each block in this file has a single hash value or not.
      final int skippedSorted = metaFilePrimInputStream.skipBytes(1);
      if (skippedSorted != 1) {
        throw new IOException("The input stream cannot skipped the \"hashed\" metadata.");
      }

      while (metaFileInputStream.available() > 0) {
        final int bytesToSkip = 12; // hash value (int) + offset (long) -> 12 bytes.
        final int skippedOffset = metaFilePrimInputStream.skipBytes(bytesToSkip);
        if (skippedOffset != bytesToSkip) {
          throw new IOException("The metadata input stream cannot skip the \"hash value\" and \"offset\" metadata.");
        }
        final int serializedDataLength = metaFilePrimInputStream.readInt();
        final long numElements = metaFilePrimInputStream.readLong();
        // Deserialize the block.
        deserializeBlock(serializedDataLength, numElements, fileInputStream, deserializedData);
      }
    }

    return deserializedData;
  }

  /**
   * Reads and deserializes a block.
   *
   * @param serializedDataLength the length of the serialized data of the block.
   * @param numElements          the number of elements in the block.
   * @param fileInputStream      the stream contains the actual data.
   * @param deserializedData     the list of elements to put the deserialized data.
   * @throws IOException if fail to read and deserialize.
   */
  private void deserializeBlock(final int serializedDataLength,
                                final long numElements,
                                final FileInputStream fileInputStream,
                                final List<Element> deserializedData) throws IOException {
    // Read the block information
    if (serializedDataLength != 0) {
      // This stream will be not closed, but it is okay as long as the file stream is closed well.
      final BufferedInputStream bufferedInputStream =
          new BufferedInputStream(fileInputStream, serializedDataLength);
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
   * @param hashed   whether each block in this partition has a single hash value or not.
   * @return the corresponding partition.
   * @throws IOException if the file exist already.
   */
  public static GlusterFilePartition create(final Coder coder,
                                            final String filePath,
                                            final boolean hashed) throws IOException {
    if (!new File(filePath).isFile()) {
      final GlusterFilePartition partition = new GlusterFilePartition(coder, filePath, true);
      partition.openPartitionForWrite(hashed);
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
   * @return the partition if success to open the file and partition, or an empty optional if the file does not exist.
   */
  public static Optional<GlusterFilePartition> open(final Coder coder,
                                                    final String filePath) {
    if (new File(filePath).isFile()) {
      return Optional.of(new GlusterFilePartition(coder, filePath, false));
    } else {
      return Optional.empty();
    }
  }
}
