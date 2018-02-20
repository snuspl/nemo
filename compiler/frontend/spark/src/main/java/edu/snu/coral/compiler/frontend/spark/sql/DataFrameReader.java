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
package edu.snu.coral.compiler.frontend.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * A data frame reader to create the initial dataset.
 */
public final class DataFrameReader extends org.apache.spark.sql.DataFrameReader {
  /**
   * Constructor.
   * @param sparkSession spark session.
   */
  DataFrameReader(final SparkSession sparkSession) {
    super(sparkSession);
  }

  @Override
  public Dataset<Row> csv(final scala.collection.Seq<String> paths) {
    return Dataset.from(super.csv(paths));
  }

  @Override
  public Dataset<Row> csv(final String... paths) {
    return Dataset.from(super.csv(paths));
  }

  @Override
  public Dataset<Row> csv(final String path) {
    return Dataset.from(super.csv(path));
  }

  @Override
  public DataFrameReader format(String source) {
    super.format(source);
    return this;
  }

  @Override
  public Dataset<Row> jdbc(final String url, final String table, final java.util.Properties properties) {
    return Dataset.from(super.jdbc(url, table, properties));
  }

  @Override
  public Dataset<Row> jdbc(final String url, final String table,
                           final String[] predicates, final java.util.Properties connectionProperties) {
    return Dataset.from(super.jdbc(url, table, predicates, connectionProperties));
  }

  @Override
  public Dataset<Row> jdbc(final String url, final String table, final String columnName,
                           final long lowerBound, final long upperBound, final int numPartitions,
                           final java.util.Properties connectionProperties) {
    return Dataset.from(super.jdbc(
        url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties));
  }

  @Override
  public Dataset<Row> json(final JavaRDD<String> jsonRDD) {
    return Dataset.from(super.json(jsonRDD));
  }

  @Override
  public Dataset<Row> json(final RDD<String> jsonRDD) {
    return Dataset.from(super.json(jsonRDD));
  }

  @Override
  public Dataset<Row> json(final scala.collection.Seq<String> paths) {
    return Dataset.from(super.json(paths));
  }

  @Override
  public Dataset<Row> json(final String... paths) {
    return Dataset.from(super.json(paths));
  }

  @Override
  public Dataset<Row> json(final String path) {
    return Dataset.from(super.json(path));
  }

  @Override
  public Dataset<Row> load() {
    return Dataset.from(super.load());
  }

  @Override
  public Dataset<Row> load(final scala.collection.Seq<String> paths) {
    return Dataset.from(super.load(paths));
  }

  @Override
  public Dataset<Row> load(final String... paths) {
    return Dataset.from(super.load(paths));
  }

  @Override
  public Dataset<Row> load(final String path) {
    return Dataset.from(super.load(path));
  }

  @Override
  public DataFrameReader option(String key, boolean value) {
    super.option(key, value);
    return this;
  }

  @Override
  public DataFrameReader option(String key, double value) {
    super.option(key, value);
    return this;
  }

  @Override
  public DataFrameReader option(String key, long value) {
    super.option(key, value);
    return this;
  }

  @Override
  public DataFrameReader option(String key, String value) {
    super.option(key, value);
    return this;
  }

  @Override
  public DataFrameReader options(scala.collection.Map<String,String> options) {
    super.options(options);
    return this;
  }

  @Override
  public DataFrameReader options(java.util.Map<String,String> options) {
    super.options(options);
    return this;
  }

  @Override
  public Dataset<Row> orc(final scala.collection.Seq<String> paths) {
    return Dataset.from(super.orc(paths));
  }

  @Override
  public Dataset<Row> orc(final String... paths) {
    return Dataset.from(super.orc(paths));
  }

  @Override
  public Dataset<Row> orc(final String path) {
    return Dataset.from(super.orc(path));
  }

  @Override
  public Dataset<Row> parquet(final scala.collection.Seq<String> paths) {
    return Dataset.from(super.parquet(paths));
  }

  @Override
  public Dataset<Row> parquet(final String... paths) {
    return Dataset.from(super.parquet(paths));
  }

  @Override
  public Dataset<Row> parquet(final String path) {
    return Dataset.from(super.parquet(path));
  }

  @Override
  public DataFrameReader schema(StructType schema) {
    super.schema(schema);
    return this;
  }

  @Override
  public Dataset<Row> table(final String tableName) {
    return Dataset.from(super.table(tableName));
  }

  @Override
  public Dataset<Row> text(final scala.collection.Seq<String> paths) {
    return Dataset.from(super.text(paths));
  }

  @Override
  public Dataset<Row> text(final String... paths) {
    return Dataset.from(super.text(paths));
  }

  @Override
  public Dataset<Row> text(final String path) {
    return Dataset.from(super.text(path));
  }

  @Override
  public Dataset<String> textFile(final scala.collection.Seq<String> paths) {
    return Dataset.from(super.textFile(paths));
  }

  @Override
  public Dataset<String> textFile(final String... paths) {
    return Dataset.from(super.textFile(paths));
  }

  @Override
  public Dataset<String> textFile(final String path) {
    return Dataset.from(super.textFile(path));
  }
}
