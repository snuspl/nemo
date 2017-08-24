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
package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * Output stream for partition transfer.
 */
public final class PartitionOutputStream implements Closeable, Flushable, PartitionTransfer.PartitionStream {
  // internally store requestId
  // internally store ConcurrentQueue and encoder for EncodingThread to encode data

  // may write FileRegion

  /**
   * Creates a partition output stream.
   */
  PartitionOutputStream() {
    // constructor with default access modifier
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void flush() throws IOException {
  }
}
