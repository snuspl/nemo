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

import edu.snu.onyx.compiler.frontend.spark.core.SparkContext;
import edu.snu.onyx.compiler.frontend.spark.core.java.JavaRDD;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A dataset component: it represents relational data.
 * @param <T> type of the data.
 */
public final class Dataset<T> {
  private final List<String> columnNames;
  private final List<List<T>> data;
  private final SparkContext sparkContext;

  /**
   * Constructor.
   * @param sparkContext spark context.
   * @param columnNames name of columns.
   * @param data the relational data.
   */
  public Dataset(final SparkContext sparkContext, final List<String> columnNames, final List<T>... data) {
    this(sparkContext, columnNames, Arrays.asList(data));
  }

  /**
   * Constructor.
   * @param sparkContext spark context.
   * @param columnNames name of columns.
   * @param data the relational data.
   */
  public Dataset(final SparkContext sparkContext, final List<String> columnNames, final List<List<T>> data) {
    this.sparkContext = sparkContext;
    this.columnNames = columnNames;
    this.data = data;
  }

  /**
   * Create a javaRDD component from this data set.
   * @param <O> the type of the data.
   * @return the new javaRDD component.
   */
  public <O> JavaRDD<O> javaRDD() {
    final Integer index = this.columnNames.indexOf("value");
    final List<String> inputSourcePaths = this.data.stream().map(lst -> (String) lst.get(index))
        .collect(Collectors.toList());
    JavaRDD<O> javaRDD = JavaRDD.of(this.sparkContext, 1);
    for (final String inputSourcePath: inputSourcePaths) {
      javaRDD = javaRDD.setSource(inputSourcePath);
    }
    return javaRDD;
  }

  /**
   * Select a key (column) from the table.
   * @param key the key (name of the column).
   * @return the data set with only the specified data.
   */
  public Dataset<T> select(final String key) {
    final Integer index = columnNames.indexOf(key);
    final List<List<T>> newData = data.stream().map(lst -> Collections.singletonList(lst.get(index)))
        .collect(Collectors.toList());
    return new Dataset<>(sparkContext, Collections.singletonList(key), newData);
  }
}
