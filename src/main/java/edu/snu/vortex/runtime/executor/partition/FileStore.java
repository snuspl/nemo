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

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.Element;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Store partitions in file.
 */
final class FileStore implements PartitionStore {

  private final String fileDirectory;
  private final Map<String, Partition> partitionIdToData;

  @Inject
  FileStore(@Parameter(JobConf.FileDirectory.class) final String fileDirectory) {
    this.fileDirectory = fileDirectory;
    this.partitionIdToData = new ConcurrentHashMap<>();
  }

  @Override
  public Optional<Partition> getPartition(final String partitionId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Long> putPartition(final String partitionId, final Iterable<Element> data) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Partition> removePartition(final String partitionId) {
    throw new UnsupportedOperationException();
  }

  /**
   * This class represents a {@link Partition} which is stored in {@link FileStore}
   * and not divided in multiple partitions.
   */
  private final class UnitFilePartition implements Partition {

    private final byte[] serializedData;
    private final Coder coder;

    private UnitFilePartition(final byte[] serializedData,
                              final Coder coder) {
      this.serializedData = serializedData;
      this.coder = coder;
    }

    @Override
    public Iterable<Element> asIterable() {
      // TODO: deserialize with coder.
      return serializedData;
    }
  }
}