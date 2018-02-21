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
package edu.snu.nemo.compiler.frontend.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * A data frame reader to create the initial dataset.
 */
public final class DataFrameReader extends org.apache.spark.sql.DataFrameReader {
  private final SparkSession sparkSession;

  /**
   * Constructor.
   * @param sparkSession spark session.
   */
  DataFrameReader(final SparkSession sparkSession) {
    super(sparkSession);
    this.sparkSession = sparkSession;
  }

  @Override
  public Dataset<Row> csv(final org.apache.spark.sql.Dataset<String> csvDataset) {
    sparkSession.appendCommand("DataFrameReader#csv", csvDataset);
    return Dataset.from(super.csv(csvDataset));
  }

  @Override
  public Dataset<Row> csv(final scala.collection.Seq<String> paths) {
    sparkSession.appendCommand("DataFrameReader#csv", paths);
    return Dataset.from(super.csv(paths));
  }

  @Override
  public Dataset<Row> csv(final String... paths) {
    sparkSession.appendCommand("DataFrameReader#csv", paths);
    return Dataset.from(super.csv(paths));
  }

  @Override
  public Dataset<Row> csv(final String path) {
    sparkSession.appendCommand("DataFrameReader#csv", path);
    return Dataset.from(super.csv(path));
  }

  @Override
  public DataFrameReader format(final String source) {
    super.format(source);
    return this;
  }

  @Override
  public Dataset<Row> jdbc(final String url, final String table, final java.util.Properties properties) {
    sparkSession.appendCommand("DataFrameReader#jdbc", url, table, properties);
    return Dataset.from(super.jdbc(url, table, properties));
  }

  @Override
  public Dataset<Row> jdbc(final String url, final String table,
                            final String[] predicates, final java.util.Properties connectionProperties) {
    sparkSession.appendCommand("DataFrameReader#jdbc", url, table, predicates, connectionProperties);
    return Dataset.from(super.jdbc(url, table, predicates, connectionProperties));
  }

  @Override
  public Dataset<Row> jdbc(final String url, final String table, final String columnName,
                            final long lowerBound, final long upperBound, final int numPartitions,
                            final java.util.Properties connectionProperties) {
    sparkSession.appendCommand("DataFrameReader#jdbc",
        url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties);
    return Dataset.from(super.jdbc(
        url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties));
  }

  @Override
  public Dataset<Row> json(final org.apache.spark.sql.Dataset<String> jsonDataset) {
    sparkSession.appendCommand("DataFrameReader#json", jsonDataset);
    return Dataset.from(super.json(jsonDataset));
  }

  @Override
  public Dataset<Row> json(final JavaRDD<String> jsonRDD) {
    sparkSession.appendCommand("DataFrameReader#json", jsonRDD);
    return Dataset.from(super.json(jsonRDD));
  }

  @Override
  public Dataset<Row> json(final RDD<String> jsonRDD) {
    sparkSession.appendCommand("DataFrameReader#json", jsonRDD);
    return Dataset.from(super.json(jsonRDD));
  }

  @Override
  public Dataset<Row> json(final scala.collection.Seq<String> paths) {
    sparkSession.appendCommand("DataFrameReader#json", paths);
    return Dataset.from(super.json(paths));
  }

  @Override
  public Dataset<Row> json(final String... paths) {
    sparkSession.appendCommand("DataFrameReader#json", paths);
    return Dataset.from(super.json(paths));
  }

  @Override
  public Dataset<Row> json(final String path) {
    sparkSession.appendCommand("DataFrameReader#json", path);
    return Dataset.from(super.json(path));
  }

  @Override
  public Dataset<Row> load() {
    sparkSession.appendCommand("DataFrameReader#load");
    return Dataset.from(super.load());
  }

  @Override
  public Dataset<Row> load(final scala.collection.Seq<String> paths) {
    sparkSession.appendCommand("DataFrameReader#load", paths);
    return Dataset.from(super.load(paths));
  }

  @Override
  public Dataset<Row> load(final String... paths) {
    sparkSession.appendCommand("DataFrameReader#load", paths);
    return Dataset.from(super.load(paths));
  }

  @Override
  public Dataset<Row> load(final String path) {
    sparkSession.appendCommand("DataFrameReader#load", path);
    return Dataset.from(super.load(path));
  }

  @Override
  public DataFrameReader option(final String key, final boolean value) {
    super.option(key, value);
    return this;
  }

  @Override
  public DataFrameReader option(final String key, final double value) {
    super.option(key, value);
    return this;
  }

  @Override
  public DataFrameReader option(final String key, final long value) {
    super.option(key, value);
    return this;
  }

  @Override
  public DataFrameReader option(final String key, final String value) {
    super.option(key, value);
    return this;
  }

  @Override
  public DataFrameReader options(final scala.collection.Map<String, String> options) {
    super.options(options);
    return this;
  }

  @Override
  public DataFrameReader options(final java.util.Map<String, String> options) {
    super.options(options);
    return this;
  }

  @Override
  public Dataset<Row> orc(final scala.collection.Seq<String> paths) {
    sparkSession.appendCommand("DataFrameReader#orc", paths);
    return Dataset.from(super.orc(paths));
  }

  @Override
  public Dataset<Row> orc(final String... paths) {
    sparkSession.appendCommand("DataFrameReader#orc", paths);
    return Dataset.from(super.orc(paths));
  }

  @Override
  public Dataset<Row> orc(final String path) {
    sparkSession.appendCommand("DataFrameReader#orc", path);
    return Dataset.from(super.orc(path));
  }

  @Override
  public Dataset<Row> parquet(final scala.collection.Seq<String> paths) {
    sparkSession.appendCommand("DataFrameReader#parquet", paths);
    return Dataset.from(super.parquet(paths));
  }

  @Override
  public Dataset<Row> parquet(final String... paths) {
    sparkSession.appendCommand("DataFrameReader#parquet", paths);
    return Dataset.from(super.parquet(paths));
  }

  @Override
  public Dataset<Row> parquet(final String path) {
    sparkSession.appendCommand("DataFrameReader#parquet", path);
    return Dataset.from(super.parquet(path));
  }

  @Override
  public DataFrameReader schema(final StructType schema) {
    super.schema(schema);
    return this;
  }

  @Override
  public Dataset<Row> table(final String tableName) {
    sparkSession.appendCommand("DataFrameReader#table", tableName);
    return Dataset.from(super.table(tableName));
  }

  @Override
  public Dataset<Row> text(final scala.collection.Seq<String> paths) {
    sparkSession.appendCommand("DataFrameReader#text", paths);
    return Dataset.from(super.text(paths));
  }

  @Override
  public Dataset<Row> text(final String... paths) {
    sparkSession.appendCommand("DataFrameReader#text", paths);
    return Dataset.from(super.text(paths));
  }

  @Override
  public Dataset<Row> text(final String path) {
    sparkSession.appendCommand("DataFrameReader#text", path);
    return Dataset.from(super.text(path));
  }

  @Override
  public Dataset<String> textFile(final scala.collection.Seq<String> paths) {
    sparkSession.appendCommand("DataFrameReader#textFile", paths);
    return Dataset.from(super.textFile(paths));
  }

  @Override
  public Dataset<String> textFile(final String... paths) {
    sparkSession.appendCommand("DataFrameReader#textFile", paths);
    return Dataset.from(super.textFile(paths));
  }

  @Override
  public Dataset<String> textFile(final String path) {
    sparkSession.appendCommand("DataFrameReader#textFile", path);
    return Dataset.from(super.textFile(path));
  }
}
