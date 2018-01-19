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
package edu.snu.onyx.compiler.frontend.spark.core;

import org.apache.spark.SparkConf;

/**
 * Spark context.
 */
public final class SparkContext extends org.apache.spark.SparkContext {
  /**
   * Constructor.
   * @param conf configuration of the context.
   */
  private SparkContext(final SparkConf conf) {
    super(conf);
  }

  /**
   * Get or create a Spark Context.
   * @param conf configuration for the context.
   * @return the new spark context.
   */
  public static SparkContext getOrCreate(final SparkConf conf) {
    return new SparkContext(conf);
  }
}
