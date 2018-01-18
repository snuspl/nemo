/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.onyx.compiler.frontend.spark.sql;

import java.util.ArrayList;
import java.util.List;

/**
 * A data frame reader to create the initial dataset.
 */
public final class DataFrameReader {
  private final SparkSession sparkSession;
  private String source;

  /**
   * Constructor.
   * @param sparkSession spark session.
   */
  DataFrameReader(final SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  /**
   * Set the source type.
   * @param sourceType type of the source.
   * @return the updated DataFrameReader.
   */
  private DataFrameReader format(final String sourceType) {
    this.source = sourceType;
    return this;
  }

  /**
   * Load data from source (it gives the data required for the read action).
   * @param paths paths to read from.
   * @return the dataset.
   */
  private Dataset<String> load(final String... paths) {
    final List<String> columnNames = new ArrayList<>();
    final List<List<String>> data = new ArrayList<>();

    columnNames.add("source");
    columnNames.add("value");

    for (final String path: paths) {
      final List<String> row = new ArrayList<>();
      row.add(source);
      row.add(path);
      data.add(row);
    }

    return new Dataset<>(sparkSession.sparkContext(), columnNames, data);
  }

  /**
   * Text-based reader.
   * @param paths the path to read the text from.
   * @return the dataset.
   */
  public Dataset<String> text(final String... paths) {
    return format("text").load(paths);
  }

  /**
   * Textfile-based reader.
   * @param paths the path to read the text from.
   * @return the dataset.
   */
  public Dataset<String> textFile(final String... paths) {
    return text(paths).select("value");
  }
}
