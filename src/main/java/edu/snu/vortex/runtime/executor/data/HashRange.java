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
package edu.snu.vortex.runtime.executor.data;

/**
 * Descriptor for hash range.
 */
public final class HashRange {

  private final int rangeStartInclusive;
  private final int rangeEndExclusive;

  private HashRange(final int rangeStartInclusive, final int rangeEndExclusive) {
    this.rangeStartInclusive = rangeStartInclusive;
    this.rangeEndExclusive = rangeEndExclusive;
  }

  /**
   * @param rangeStartInclusive the start of the range (inclusive)
   * @param rangeEndExclusive   the end of the range (exclusive)
   * @return A hash range descriptor representing [{@code rangeStartInclusive}, {@code rangeEndExclusive})
   */
  public static HashRange of(final int rangeStartInclusive, final int rangeEndExclusive) {
    return new HashRange(rangeStartInclusive, rangeEndExclusive);
  }

  /**
   * @return the start of the range (inclusive)
   */
  public int rangeStartInclusive() {
    return rangeStartInclusive;
  }

  /**
   * @return the end of the range (exclusive)
   */
  public int rangeEndExclusive() {
    return rangeEndExclusive;
  }

  /**
   * @return the length of this range
   */
  public int length() {
    return rangeEndExclusive - rangeStartInclusive;
  }

  /**
   * @param i the value to test
   * @return {@code true} if this hash range includes the specified value, {@code false} otherwise
   */
  public boolean includes(final int i) {
    return i >= rangeStartInclusive && i < rangeEndExclusive;
  }

  @Override
  public String toString() {
    return String.format("[%d, %d)", rangeStartInclusive, rangeEndExclusive);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HashRange hashRange = (HashRange) o;
    if (rangeStartInclusive != hashRange.rangeStartInclusive) {
      return false;
    }
    return rangeEndExclusive == hashRange.rangeEndExclusive;
  }

  @Override
  public int hashCode() {
    return 31 * rangeStartInclusive + rangeEndExclusive;
  }
}
