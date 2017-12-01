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

import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.exception.BlockTypeMismatchException;

import java.util.Collections;

/**
 * A collection of data elements.
 * This is a unit of read / write towards {@link edu.snu.onyx.runtime.executor.data.partition.Partition}s.
 * TODO #494: Refactor HashRange to be general. int -> generic Key, and so on...
 */
public final class Block {
  private final int key;
  private final boolean serialized;
  private final Iterable data;
  private final long elementsTotal;
  private final byte[] serializedData;

  /**
   * Creates a non-serialized {@link Block}.
   *
   * @param data the non-serialized data.
   */
  public Block(final Iterable data) {
    this(HashRange.NOT_HASHED, data);
  }

  /**
   * Creates a non-serialized {@link Block} having a specific key value.
   *
   * @param key  the key.
   * @param data the non-serialized data.
   */
  public Block(final int key,
               final Iterable data) {
    this.key = key;
    this.serialized = false;
    this.data = data;
    elementsTotal = -1;
    serializedData = new byte[0];
  }

  /**
   * Creates a serialized {@link Block}.
   *
   * @param elementsTotal  the total number of elements.
   * @param serializedData the serialized data.
   */
  public Block(final long elementsTotal,
               final byte[] serializedData) {
    this(HashRange.NOT_HASHED, elementsTotal, serializedData);
  }

  /**
   * Creates a serialized {@link Block} having a specific key value.
   *
   * @param key            the key.
   * @param elementsTotal  the total number of elements.
   * @param serializedData the serialized data.
   */
  public Block(final int key,
               final long elementsTotal,
               final byte[] serializedData) {
    this.key = key;
    this.serialized = true;
    this.data = Collections.emptySet();
    this.elementsTotal = elementsTotal;
    this.serializedData = serializedData;
  }

  /**
   * @return the key value.
   */
  public int getKey() {
    return key;
  }

  /**
   * @return whether the data in this {@link Block} is serialized or not.
   */
  public boolean isSerialized() {
    return serialized;
  }

  /**
   * @return the non-serialized data.
   * @throws BlockTypeMismatchException if the data in this {@link Block} is serialized.
   */
  public Iterable getElements() throws BlockTypeMismatchException {
    if (serialized) {
      throw new BlockTypeMismatchException("Non-serialized data cannot be get from this block.");
    } else {
      return data;
    }
  }

  /**
   * @return the number of elements.
   * @throws BlockTypeMismatchException if the data in this {@link Block} is not serialized.
   */
  public long getElementsTotal() throws BlockTypeMismatchException {
    if (serialized) {
      return elementsTotal;
    } else {
      throw new BlockTypeMismatchException("The number of elements cannot be get from this block.");
    }
  }

  /**
   * @return the serialized data.
   * @throws BlockTypeMismatchException if the data in this {@link Block} is not serialized.
   */
  public byte[] getSerializedData() throws BlockTypeMismatchException {
    if (serialized) {
      return serializedData;
    } else {
      throw new BlockTypeMismatchException("The serialized data cannot be get from this block.");
    }
  }
}
