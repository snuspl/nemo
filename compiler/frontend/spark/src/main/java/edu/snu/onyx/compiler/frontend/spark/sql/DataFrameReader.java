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

public class DataFrameReader {
  private final SparkSession sparkSession;
  private String source;

  DataFrameReader(final SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  private DataFrameReader format(final String source) {
    this.source = source;
    return this;
  }

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

  public Dataset<String> text(final String... paths) {
    return format("text").load(paths);
  }

  public Dataset<String> textFile(final String... paths) {
    return text(paths).select("value");
  }
}
