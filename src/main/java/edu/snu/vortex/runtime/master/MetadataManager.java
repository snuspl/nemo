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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.executor.data.metadata.BlockMetadata;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the metadata of remote files.
 */
@ThreadSafe
final class MetadataManager {

  private final Map<String, MetadataInServer> filePathToMetadata;

  @Inject
  private MetadataManager() {
    this.filePathToMetadata = new ConcurrentHashMap<>();
  }

  /**
   * Stores a new (whole) metadata for a remote file.
   *
   * @param filePath          the path of the file.
   * @param hashed            whether each block in the file has a single hash value or not.
   * @param blockMetadataList the list of the block metadata in the file.
   * @return {@code true} if success to store, or {@code false} if it already exists.
   */
  public boolean storeMetadata(final String filePath,
                               final boolean hashed,
                               final List<BlockMetadata> blockMetadataList) {
    final MetadataInServer previousMetadata =
        filePathToMetadata.putIfAbsent(filePath, new MetadataInServer(hashed, blockMetadataList));
    return previousMetadata == null;
  }

  /**
   * Gets the metadata for a remote file.
   *
   * @param filePath the path of the file.
   * @return the metadata if exists, or an empty optional else.
   */
  public Optional<MetadataInServer> getMetadata(final String filePath) {
    final MetadataInServer metadata = filePathToMetadata.get(filePath);
    if (metadata == null) {
      return Optional.empty();
    } else {
      return Optional.of(metadata);
    }
  }

  /**
   * Removes the metadata for a remote file.
   *
   * @param filePath the path of the file.
   * @return whether success to remove or not.
   */
  public boolean removeMetadata(final String filePath) {
    return filePathToMetadata.remove(filePath) != null;
  }
}
