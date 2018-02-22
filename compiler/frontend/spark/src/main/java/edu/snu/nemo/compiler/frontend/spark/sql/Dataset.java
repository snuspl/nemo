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

import edu.snu.nemo.compiler.frontend.spark.core.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.storage.StorageLevel;

import java.util.stream.Stream;

/**
 * A dataset component: it represents relational data.
 * @param <T> type of the data.
 */
public final class Dataset<T> extends org.apache.spark.sql.Dataset<T> implements NemoSparkSQL {
  private final SparkSession sparkSession;
  private boolean userTriggered;

  /**
   * Constructor.
   * @param sparkSession spark session.
   * @param logicalPlan spark logical plan.
   * @param encoder spark encoder.
   */
  private Dataset(final SparkSession sparkSession, final LogicalPlan logicalPlan, final Encoder<T> encoder) {
    super(sparkSession, logicalPlan, encoder);
    this.sparkSession = sparkSession;
    this.userTriggered = true;
  }

  /**
   * Using the immutable property of datasets, we can downcast spark datasets to our class using this function.
   * @param dataset the Spark dataset.
   * @param <U> type of the dataset.
   * @return our dataset class.
   */
  public static <U> Dataset<U> from(final org.apache.spark.sql.Dataset<U> dataset) {
    if (dataset instanceof Dataset) {
      return (Dataset<U>) dataset;
    } else {
      return new Dataset<>((SparkSession) dataset.sparkSession(), dataset.logicalPlan(), dataset.exprEnc());
    }
  }

  /**
   * Create a javaRDD component from this data set.
   * @return the new javaRDD component.
   */
  @Override
  public JavaRDD<T> javaRDD() {
    return JavaRDD.of((SparkSession) super.sparkSession(), this);
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
  public Dataset<Row> agg(final Column expr, final Column... exprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      expr, exprs));
  }

  @Override
  public Dataset<Row> agg(final Column expr, final scala.collection.Seq<Column> exprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      expr, exprs));
  }

  @Override
  public Dataset<Row> agg(final scala.collection.immutable.Map<String, String> exprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      exprs));
  }

  @Override
  public Dataset<Row> agg(final java.util.Map<String, String> exprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      exprs));
  }

  @Override
  public Dataset<Row> agg(final scala.Tuple2<String, String> aggExpr,
                          final scala.collection.Seq<scala.Tuple2<String, String>> aggExprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      aggExpr, aggExprs));
  }

  @Override
  public Dataset<T> alias(final String alias) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      alias));
  }

  @Override
  public Dataset<T> alias(final scala.Symbol alias) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      alias));
  }

  @Override
  public Dataset<T> as(final String alias) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      alias));
  }

  @Override
  public Dataset<T> as(final scala.Symbol alias) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      alias));
  }

  @Override
  public Dataset<T> cache() {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this));
  }

  @Override
  public Dataset<T> checkpoint() {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this));
  }

  @Override
  public Dataset<T> checkpoint(final boolean eager) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      eager));
  }

  @Override
  public Dataset<T> coalesce(final int numPartitions) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      numPartitions));
  }

  @Override
  public Dataset<Row> crossJoin(final org.apache.spark.sql.Dataset<?> right) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      right));
  }

  @Override
  public Dataset<Row> describe(final scala.collection.Seq<String> cols) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      cols));
  }

  @Override
  public Dataset<Row> describe(final String... cols) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      cols));
  }

  @Override
  public Dataset<T> distinct() {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this));
  }

  @Override
  public Dataset<Row> drop(final Column col) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      col));
  }

  @Override
  public Dataset<Row> drop(final scala.collection.Seq<String> colNames) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      colNames));
  }

  @Override
  public Dataset<Row> drop(final String... colNames) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      colNames));
  }

  @Override
  public Dataset<Row> drop(final String colName) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      colName));
  }

  @Override
  public Dataset<T> dropDuplicates() {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this));
  }

  @Override
  public Dataset<T> dropDuplicates(final scala.collection.Seq<String> colNames) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      colNames));
  }

  @Override
  public Dataset<T> dropDuplicates(final String[] colNames) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      colNames));
  }

  @Override
  public Dataset<T> dropDuplicates(final String col1, final scala.collection.Seq<String> cols) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      col1, cols));
  }

  @Override
  public Dataset<T> dropDuplicates(final String col1, final String... cols) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      col1, cols));
  }

  @Override
  public Dataset<T> except(final org.apache.spark.sql.Dataset<T> other) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      other));
  }

  @Override
  public Dataset<T> filter(final Column condition) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      condition));
  }

  @Override
  public Dataset<T> filter(final String conditionExpr) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      conditionExpr));
  }

  @Override
  public Dataset<T> hint(final String name, final Object... parameters) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      name, parameters));
  }

  @Override
  public Dataset<T> hint(final String name, final scala.collection.Seq<Object> parameters) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      name, parameters));
  }

  @Override
  public Dataset<T> intersect(final org.apache.spark.sql.Dataset<T> other) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      other));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      right));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final Column joinExprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      right, joinExprs));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final Column joinExprs, final String joinType) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      right, joinExprs, joinType));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right,
                           final scala.collection.Seq<String> usingColumns) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      right, usingColumns));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final scala.collection.Seq<String> usingColumns,
                           final String joinType) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      right, usingColumns, joinType));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final String usingColumn) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      right, usingColumn));
  }

  @Override
  public Dataset<T> limit(final int n) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      n));
  }

  /**
   * Overrides super.ofRows.
   * @param sparkSession Spark Session.
   * @param logicalPlan Spark logical plan.
   * @return Dataset of the given rows.
   */
  public static Dataset<Row> ofRows(final org.apache.spark.sql.SparkSession sparkSession,
                                    final org.apache.spark.sql.catalyst.plans.logical.LogicalPlan logicalPlan) {
    return from(org.apache.spark.sql.Dataset.ofRows(sparkSession, logicalPlan));
  }

  @Override
  public Dataset<T> orderBy(final Column... sortExprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      sortExprs));
  }

  @Override
  public Dataset<T> orderBy(final scala.collection.Seq<Column> sortExprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      sortExprs));
  }

  @Override
  public Dataset<T> orderBy(final String sortCol, final scala.collection.Seq<String> sortCols) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      sortCol, sortCols));
  }

  @Override
  public Dataset<T> orderBy(final String sortCol, final String... sortCols) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      sortCol, sortCols));
  }

  @Override
  public Dataset<T> persist() {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this));
  }

  @Override
  public Dataset<T> persist(final StorageLevel newLevel) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      newLevel));
  }

  @Override
  public Dataset<T>[] randomSplit(final double[] weights) {
    return Stream.of(super.randomSplit(weights)).map(ds -> from(ds)).toArray(Dataset[]::new);
  }

  @Override
  public Dataset<T>[] randomSplit(final double[] weights, final long seed) {
    return Stream.of(super.randomSplit(weights, seed)).map(ds -> from(ds)).toArray(Dataset[]::new);
  }

//  @Override
//  public java.util.List<Dataset<T>> randomSplitAsList(double[] weights, long seed) {
//    return super.randomSplitAsList(weights, seed).stream().map(ds -> from(ds)).collect(Collectors.toList());
//  }

  @Override
  public Dataset<T> repartition(final Column... partitionExprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      partitionExprs));
  }

  @Override
  public Dataset<T> repartition(final int numPartitions) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      numPartitions));
  }

  @Override
  public Dataset<T> repartition(final int numPartitions, final Column... partitionExprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      numPartitions, partitionExprs));
  }

  @Override
  public Dataset<T> repartition(final int numPartitions, final scala.collection.Seq<Column> partitionExprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      numPartitions, partitionExprs));
  }

  @Override
  public Dataset<T> repartition(final scala.collection.Seq<Column> partitionExprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      partitionExprs));
  }

  @Override
  public Dataset<T> sample(final boolean withReplacement, final double fraction) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      withReplacement, fraction));
  }

  @Override
  public Dataset<T> sample(final boolean withReplacement, final double fraction, final long seed) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      withReplacement, fraction, seed));
  }

  @Override
  public Dataset<Row> select(final Column... cols) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      cols));
  }

  @Override
  public Dataset<Row> select(final scala.collection.Seq<Column> cols) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      cols));
  }

  @Override
  public Dataset<Row> select(final String col, final scala.collection.Seq<String> cols) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      col, cols));
  }

  @Override
  public Dataset<Row> select(final String col, final String... cols) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      col, cols));
  }

  @Override
  public Dataset<Row> selectExpr(final scala.collection.Seq<String> exprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      exprs));
  }

  @Override
  public Dataset<Row> selectExpr(final String... exprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      exprs));
  }

  @Override
  public Dataset<T> sort(final Column... sortExprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      sortExprs));
  }

  @Override
  public Dataset<T> sort(final scala.collection.Seq<Column> sortExprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      sortExprs));
  }

  @Override
  public Dataset<T> sort(final String sortCol, final scala.collection.Seq<String> sortCols) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      sortCol, sortCols));
  }

  @Override
  public Dataset<T> sort(final String sortCol, final String... sortCols) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      sortCol, sortCols));
  }

  @Override
  public Dataset<T> sortWithinPartitions(final Column... sortExprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      sortExprs));
  }

  @Override
  public Dataset<T> sortWithinPartitions(final scala.collection.Seq<Column> sortExprs) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      sortExprs));
  }

  @Override
  public Dataset<T> sortWithinPartitions(final String sortCol, final scala.collection.Seq<String> sortCols) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      sortCol, sortCols));
  }

  @Override
  public Dataset<T> sortWithinPartitions(final String sortCol, final String... sortCols) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      sortCol, sortCols));
  }

  @Override
  public SparkSession sparkSession() {
    return (SparkSession) super.sparkSession();
  }

  @Override
  public Dataset<Row> toDF() {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this));
  }

  @Override
  public Dataset<Row> toDF(final scala.collection.Seq<String> colNames) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      colNames));
  }

  @Override
  public Dataset<Row> toDF(final String... colNames) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      colNames));
  }

  @Override
  public Dataset<String> toJSON() {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this));
  }

  @Override
  public <U> Dataset<U> transform(
      final scala.Function1<org.apache.spark.sql.Dataset<T>, org.apache.spark.sql.Dataset<U>> t) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      t));
  }

  @Override
  public Dataset<T> union(final org.apache.spark.sql.Dataset<T> other) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      other));
  }

  @Override
  public Dataset<T> unpersist() {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this));
  }

  @Override
  public Dataset<T> unpersist(final boolean blocking) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      blocking));
  }

  @Override
  public Dataset<T> where(final Column condition) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      condition));
  }

  @Override
  public Dataset<T> where(final String conditionExpr) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      conditionExpr));
  }

  @Override
  public Dataset<Row> withColumn(final String colName, final Column col) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      colName, col));
  }

  @Override
  public Dataset<Row> withColumnRenamed(final String existingName, final String newName) {
    return from((org.apache.spark.sql.Dataset) SparkSession.callSuperclassMethod(this.sparkSession, this, 
      existingName, newName));
  }
}
