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
package edu.snu.vortex.runtime.executor;


import edu.snu.vortex.compiler.ir.Element;

/**
 * Interface of partition selectors that determine which partitions a given record should be written into.
 */
public interface PartitionSelector {
  /**
   * Returns the partition indexes, to which the given record should be written.
   *
   * @param record        the record to determine which partitions it is written into.
   * @param numPartitions the total number of partitions.
   * @return a (possibly empty) array of integer numbers which indicate the indices of the partitions through
   * which the record shall be written.
   */
  int[] selectPartitions(Element record, int numPartitions);
}