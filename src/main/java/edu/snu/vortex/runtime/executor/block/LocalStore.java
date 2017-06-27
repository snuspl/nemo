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
package edu.snu.vortex.runtime.executor.block;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.exception.BlockFetchException;
import edu.snu.vortex.runtime.exception.BlockWriteException;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Store data in local memory.
 */
@ThreadSafe
public final class LocalStore implements BlockStore {
  private final ConcurrentHashMap<String, Iterable<Element>> blockIdToData;

  @Inject
  public LocalStore() {
    this.blockIdToData = new ConcurrentHashMap<>();
  }

  @Override
  public Optional<Iterable<Element>> getBlock(final String blockId) {
    try {
      return Optional.ofNullable(blockIdToData.get(blockId));
    } catch (final Exception e) {
      throw new BlockFetchException(new Throwable("An error occurred while trying to get block from local store", e));
    }
  }

  @Override
  public void putBlock(final String blockId, final Iterable<Element> data) {
    if (blockIdToData.containsKey(blockId)) {
      throw new RuntimeException("Trying to overwrite an existing block");
    }
    try {
      blockIdToData.put(blockId, data);
    } catch (final Exception e) {
      throw new BlockWriteException(new Throwable("An error occurred while trying to put block into local store", e));
    }
  }

  @Override
  public Optional<Iterable<Element>> removeBlock(final String blockId) {
    return Optional.ofNullable(blockIdToData.remove(blockId));
  }
}
