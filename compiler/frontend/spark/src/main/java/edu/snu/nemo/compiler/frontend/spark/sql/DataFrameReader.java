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
public final class DataFrameReader extends org.apache.spark.sql.DataFrameReader implements NemoSparkSQL {
  private final SparkSession sparkSession;
  private boolean userTriggered;

  /**
   * Constructor.
   * @param sparkSession spark session.
   */
  DataFrameReader(final SparkSession sparkSession) {
    super(sparkSession);
    this.sparkSession = sparkSession;
    this.userTriggered = true;
  }

  @Override
  public boolean isUserTriggered() {
    return userTriggered;
  }

  @Override
  public void setUserTriggered(boolean userTriggered) {
    this.userTriggered = userTriggered;
  }

  @Override
  public Dataset<Row> csv(final org.apache.spark.sql.Dataset<String> csvDataset) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      csvDataset));
  }

  @Override
  public Dataset<Row> csv(final scala.collection.Seq<String> paths) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      paths));
  }

  @Override
  public Dataset<Row> csv(final String... paths) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      paths));
  }

  @Override
  public Dataset<Row> csv(final String path) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      path));
  }

  @Override
  public DataFrameReader format(final String source) {
    super.format(source);
    return this;
  }

  @Override
  public Dataset<Row> jdbc(final String url, final String table, final java.util.Properties properties) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      url, table, properties));
  }

  @Override
  public Dataset<Row> jdbc(final String url, final String table,
                            final String[] predicates, final java.util.Properties connectionProperties) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      url, table, predicates, connectionProperties));
  }

  @Override
  public Dataset<Row> jdbc(final String url, final String table, final String columnName,
                            final long lowerBound, final long upperBound, final int numPartitions,
                            final java.util.Properties connectionProperties) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this,
        url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties));
  }

  @Override
  public Dataset<Row> json(final org.apache.spark.sql.Dataset<String> jsonDataset) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this,
        jsonDataset));
  }

  @Override
  public Dataset<Row> json(final JavaRDD<String> jsonRDD) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      jsonRDD));
  }

  @Override
  public Dataset<Row> json(final RDD<String> jsonRDD) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      jsonRDD));
  }

  @Override
  public Dataset<Row> json(final scala.collection.Seq<String> paths) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      paths));
  }

  @Override
  public Dataset<Row> json(final String... paths) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      paths));
  }

  @Override
  public Dataset<Row> json(final String path) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      path));
  }

  @Override
  public Dataset<Row> load() {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this));
  }

  @Override
  public Dataset<Row> load(final scala.collection.Seq<String> paths) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      paths));
  }

  @Override
  public Dataset<Row> load(final String... paths) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      paths));
  }

  @Override
  public Dataset<Row> load(final String path) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      path));
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
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      paths));
  }

  @Override
  public Dataset<Row> orc(final String... paths) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      paths));
  }

  @Override
  public Dataset<Row> orc(final String path) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      path));
  }

  @Override
  public Dataset<Row> parquet(final scala.collection.Seq<String> paths) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      paths));
  }

  @Override
  public Dataset<Row> parquet(final String... paths) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      paths));
  }

  @Override
  public Dataset<Row> parquet(final String path) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      path));
  }

  @Override
  public DataFrameReader schema(final StructType schema) {
    super.schema(schema);
    return this;
  }

  @Override
  public Dataset<Row> table(final String tableName) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      tableName));
  }

  @Override
  public Dataset<Row> text(final scala.collection.Seq<String> paths) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      paths));
  }

  @Override
  public Dataset<Row> text(final String... paths) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      paths));
  }

  @Override
  public Dataset<Row> text(final String path) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      path));
  }

  @Override
  public Dataset<String> textFile(final scala.collection.Seq<String> paths) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      paths));
  }

  @Override
  public Dataset<String> textFile(final String... paths) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      paths));
  }

  @Override
  public Dataset<String> textFile(final String path) {
    return Dataset.from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      path));
  }
}
