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

import edu.snu.vortex.runtime.exception.UnsupportedMethodException;

import java.util.List;
import java.util.Optional;

/**
 * This class represents a metadata for a remote file partition.
 */
public final class RemoteFileMetadata extends FileMetadata {

  private RemoteFileMetadata(final boolean hashed) {
    super(hashed);
  }

  private RemoteFileMetadata(final boolean hashed,
                             final List<BlockMetadata> blockMetadataList) {
    super(hashed, blockMetadataList);
  }

  /**
   * Reserves a region for storing a block and appends a metadata for the block.
   * It will communicate with the metadata server.
   *
   * @param hashValue   of the block.
   * @param blockSize   of the block.
   * @param numElements of the block.
   */
  public void reserveBlock(final int hashValue,
                          final int blockSize,
                          final long numElements) {
    // TODO #404: Introduce metadata server model.
    // TODO #355: Support I-file write.
    throw new UnsupportedMethodException("reserveBlock(...) is not supported yet.");
  }

  /**
   * Marks that the whole data for this partition is written.
   * This method synchronizes all changes if needed.
   *
   * @return {@code true} if already set, or {@code false} if not.
   */
  @Override
  public boolean getAndSetWritten() {
    return written.getAndSet(true);
  }

  /**
   * Creates a file metadata for a partition in the remote storage to write.
   * The corresponding {@link FileMetadata#getAndSetWritten()}} for the returned metadata is required.
   *
   * @param filePath the path of the file which will contain the data of this partition.
   * @param hashed   whether each block in this partition has a single hash value or not.
   * @return the created file metadata.
   */
  public static RemoteFileMetadata create(final String filePath,
                                          final boolean hashed) {
  }

  /**
   * Opens the corresponding file metadata for a partition in the remote storage to read.
   *
   * @param filePath the path of the file which will contain the data of this partition.
   * @param hashed   whether each block in this partition has a single hash value or not.
   * @return the file metadata if success to open the metadata, or an empty optional if it does not exist.
   */
  public static Optional<RemoteFileMetadata> open(final String filePath,
                                                  final boolean hashed) {
  }
}
