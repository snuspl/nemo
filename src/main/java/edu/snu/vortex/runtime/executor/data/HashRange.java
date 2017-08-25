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
  private static final HashRange ALL = new HashRange(true, 0, Integer.MAX_VALUE);

  private final boolean all;
  private final int rangeStartInclusive;
  private final int rangeEndExclusive;

  private HashRange(final boolean all, final int rangeStartInclusive, final int rangeEndExclusive) {
    this.all = all;
    this.rangeStartInclusive = rangeStartInclusive;
    this.rangeEndExclusive = rangeEndExclusive;
  }

  /**
   * @return Gets a hash range descriptor representing the whole data from a partition.
   */
  public static HashRange all() {
    return ALL;
  }

  /**
   * @param rangeStartInclusive the start of the range (inclusive)
   * @param rangeEndExclusive   the end of the range (exclusive)
   * @return A hash range descriptor representing [{@code rangeStartInclusive}, {@code rangeEndExclusive})
   */
  public static HashRange of(final int rangeStartInclusive, final int rangeEndExclusive) {
    return new HashRange(false, rangeStartInclusive, rangeEndExclusive);
  }

  /**
   * @return whether this hash range descriptor represents the whole data or not
   */
  public boolean isAll() {
    return all;
  }

  /**
   * @return the start of the range (inclusive)
   */
  public int getRangeStartInclusive() {
    return rangeStartInclusive;
  }

  /**
   * @return the end of the range (exclusive)
   */
  public int getRangeEndExclusive() {
    return rangeEndExclusive;
  }
}