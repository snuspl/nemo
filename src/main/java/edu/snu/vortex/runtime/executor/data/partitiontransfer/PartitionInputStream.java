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

import edu.snu.vortex.compiler.ir.Element;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Input stream for partition transfer.
 */
public final class PartitionInputStream
    implements Iterable<Element>, ControlMessageToPartitionStreamCodec.PartitionStream {
  // internally store ByteBufInputStream and decoder for DecodingThread to decode data
  // internally store requestId

  // some methods are package scope

  /**
   * Creates a partition input stream.
   */
  PartitionInputStream() {
    // constructor with default access modifier
  }

  @Override
  public Iterator<Element> iterator() {
    return null;
  }

  @Override
  public void forEach(final Consumer<? super Element> consumer) {
    // use default?

  }

  @Override
  public Spliterator<Element> spliterator() {
    // use default?
    return null;
  }
}
