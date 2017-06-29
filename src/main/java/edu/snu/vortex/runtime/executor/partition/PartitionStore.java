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
package edu.snu.vortex.runtime.executor.partition;

import edu.snu.vortex.compiler.ir.Element;

import java.util.Optional;

/**
 * Interface for partition placement.
 */
public interface PartitionStore {
  /**
   * Retrieves a partition.
   * @param partitionId of the partition
   * @return the data of the partition (optionally)
   */
  Optional<Iterable<Element>> getPartition(String partitionId);

  /**
   * Saves a partition.
   * @param partitionId of the partition
   * @param data of the partition
   */
  void putPartition(String partitionId, Iterable<Element> data);

  /**
   * Removes a partition.
   * @param partitionId of the partition
   * @return the data of the partition (optionally)
   */
  Optional<Iterable<Element>> removePartition(String partitionId);
}
