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

import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.NonSerializedPartition;
import edu.snu.onyx.runtime.executor.data.SerializedPartition;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * This interface represents a partition, which is the output of a specific task.
 */
public interface TmpToBe {

  /**
   * Stores {@link NonSerializedPartition}s to this partition.
   * Invariant: This should not be invoked after this partition is committed.
   *
   * @param blocks the {@link NonSerializedPartition}s to store.
   * @return the size of the data per block (only when the data is serialized in this method).
   * @throws IOException if fail to store.
   */
  Optional<List<Long>> putBlocks(final Iterable<NonSerializedPartition> blocks) throws IOException;

  /**
   * Stores {@link SerializedPartition}s to this partition.
   * Invariant: This should not be invoked after this partition is committed.
   *
   * @param blocks the {@link SerializedPartition}s to store.
   * @return the size of the data per block.
   * @throws IOException if fail to store.
   */
  List<Long> putSerializedBlocks(final Iterable<SerializedPartition> blocks) throws IOException;

  /**
   * Retrieves the {@link NonSerializedPartition}s in a specific hash range from this partition.
   * If the data is serialized, deserializes it.
   * Invariant: This should not be invoked before this partition is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of {@link NonSerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  Iterable<NonSerializedPartition> getBlocks(final HashRange hashRange) throws IOException;

  /**
   * Retrieves the {@link SerializedPartition}s in a specific hash range.
   * Invariant: This should not be invoked before this partition is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of {@link SerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  Iterable<SerializedPartition> getSerializedBlocks(final HashRange hashRange) throws IOException;

  /**
   * Commits this partition to prevent further write.
   */
  void commit();
}
