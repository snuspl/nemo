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
package edu.snu.onyx.runtime.executor.data;

/**
 * A collection of data elements.
 * This is a unit of write towards {@link PartitionStore}s.
 * TODO #463: Support incremental read. Consider to make the {@link Block} as a unit of read also.
 * TODO #494: Refactor HashRange to be general. int -> generic Key, and so on...
 */
public final class Block {
  private final int key;
  private final Iterable<Object> data;

  public Block(final Iterable<Object> data) {
    this(HashRange.NOT_HASHED, data);
  }

  public Block(final int key,
               final Iterable<Object> data) {
    this.key = key;
    this.data = data;
  }

  public int getKey() {
    return key;
  }

  public Iterable<Object> getData() {
    return data;
  }
}
