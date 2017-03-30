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

import java.util.List;

/**
 * This writer writes data records into appropriate partitions determined by {@link PartitionSelector}.
 *
 */
public final class RecordWriter {
  private List<Partition> partitions;
  private PartitionSelector selector;
  private int numPartitions;

  public RecordWriter(final List<Partition> partitions,
                      final PartitionSelector selector) {
    this.partitions = partitions;
    this.selector = selector;
    this.numPartitions = partitions.size();
  }

  public void write(Iterable<Element> records) {
    records.forEach(record -> {
      for (int targetPartition : selector.selectPartitions(record, numPartitions)) {
        partitions.get(targetPartition).add(record);
      }
    });

    partitions.forEach(partition -> partition.writeToChannel());
  }

  public void broadcast(Iterable<Element> records) {
    partitions.forEach(partition -> {
      partition.add(records);
      partition.writeToChannel();
    });
  }

}
