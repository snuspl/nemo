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
public final class Dataset<T> extends org.apache.spark.sql.Dataset<T> {
  /**
   * Constructor.
   * @param sparkSession spark session.
   * @param logicalPlan spark logical plan.
   * @param encoder spark encoder.
   */
  private Dataset(final SparkSession sparkSession, final LogicalPlan logicalPlan, final Encoder<T> encoder) {
    super(sparkSession, logicalPlan, encoder);
  }

  /**
   * Using the immutable property of datasets, we can downcast spark datasets to our class using this function.
   * @param sparkSession the Spark Session to run this dataset with.
   * @param dataset the Spark dataset.
   * @param <U> type of the dataset.
   * @return our dataset class.
   */
  public static <U> Dataset<U> from(final SparkSession sparkSession, final org.apache.spark.sql.Dataset<U> dataset) {
    return new Dataset<>(sparkSession, dataset.logicalPlan(), dataset.exprEnc());
  }

  /**
   * Using the immutable property of datasets, we can downcast spark datasets to our class using this function.
   * @param dataset the Spark dataset.
   * @param <U> type of the dataset.
   * @return our dataset class.
   */
  public static <U> Dataset<U> from(final org.apache.spark.sql.Dataset<U> dataset) {
    return Dataset.from((SparkSession) dataset.sparkSession(), dataset);
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
  public Dataset<Row> agg(final Column expr, final Column... exprs) {
    return from(super.agg(expr, exprs));
  }

  @Override
  public Dataset<Row> agg(final Column expr, final scala.collection.Seq<Column> exprs) {
    return from(super.agg(expr, exprs));
  }

  @Override
  public Dataset<Row> agg(final scala.collection.immutable.Map<String, String> exprs) {
    return from(super.agg(exprs));
  }

  @Override
  public Dataset<Row> agg(final java.util.Map<String, String> exprs) {
    return from(super.agg(exprs));
  }

  @Override
  public Dataset<Row> agg(final scala.Tuple2<String, String> aggExpr,
                          final scala.collection.Seq<scala.Tuple2<String, String>> aggExprs) {
    return from(super.agg(aggExpr, aggExprs));
  }

  @Override
  public Dataset<T> alias(final String alias) {
    return from(super.alias(alias));
  }

  @Override
  public Dataset<T> alias(final scala.Symbol alias) {
    return from(super.alias(alias));
  }

  @Override
  public Dataset<T> as(final String alias) {
    return from(super.as(alias));
  }

  @Override
  public Dataset<T> as(final scala.Symbol alias) {
    return from(super.as(alias));
  }

  @Override
  public Dataset<T> cache() {
    return from(super.cache());
  }

  @Override
  public Dataset<T> checkpoint() {
    return from(super.checkpoint());
  }

  @Override
  public Dataset<T> checkpoint(final boolean eager) {
    return from(super.checkpoint(eager));
  }

  @Override
  public Dataset<T> coalesce(final int numPartitions) {
    return from(super.coalesce(numPartitions));
  }

  @Override
  public Dataset<Row> crossJoin(final org.apache.spark.sql.Dataset<?> right) {
    return from(super.crossJoin(right));
  }

  @Override
  public Dataset<Row> describe(final scala.collection.Seq<String> cols) {
    return from(super.describe(cols));
  }

  @Override
  public Dataset<Row> describe(final String... cols) {
    return from(super.describe(cols));
  }

  @Override
  public Dataset<T> distinct() {
    return from(super.distinct());
  }

  @Override
  public Dataset<Row> drop(final Column col) {
    return from(super.drop(col));
  }

  @Override
  public Dataset<Row> drop(final scala.collection.Seq<String> colNames) {
    return from(super.drop(colNames));
  }

  @Override
  public Dataset<Row> drop(final String... colNames) {
    return from(super.drop(colNames));
  }

  @Override
  public Dataset<Row> drop(final String colName) {
    return from(super.drop(colName));
  }

  @Override
  public Dataset<T> dropDuplicates() {
    return from(super.dropDuplicates());
  }

  @Override
  public Dataset<T> dropDuplicates(final scala.collection.Seq<String> colNames) {
    return from(super.dropDuplicates(colNames));
  }

  @Override
  public Dataset<T> dropDuplicates(final String[] colNames) {
    return from(super.dropDuplicates(colNames));
  }

  @Override
  public Dataset<T> dropDuplicates(final String col1, final scala.collection.Seq<String> cols) {
    return from(super.dropDuplicates(col1, cols));
  }

  @Override
  public Dataset<T> dropDuplicates(final String col1, final String... cols) {
    return from(super.dropDuplicates(col1, cols));
  }

  @Override
  public Dataset<T> except(final org.apache.spark.sql.Dataset<T> other) {
    return from(super.except(other));
  }

  @Override
  public Dataset<T> filter(final Column condition) {
    return from(super.filter(condition));
  }

  @Override
  public Dataset<T> filter(final String conditionExpr) {
    return from(super.filter(conditionExpr));
  }

  @Override
  public Dataset<T> hint(final String name, final Object... parameters) {
    return from(super.hint(name, parameters));
  }

  @Override
  public Dataset<T> hint(final String name, final scala.collection.Seq<Object> parameters) {
    return from(super.hint(name, parameters));
  }

  @Override
  public Dataset<T> intersect(final org.apache.spark.sql.Dataset<T> other) {
    return from(super.intersect(other));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right) {
    return from(super.join(right));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final Column joinExprs) {
    return from(super.join(right, joinExprs));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final Column joinExprs, final String joinType) {
    return from(super.join(right, joinExprs, joinType));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right,
                           final scala.collection.Seq<String> usingColumns) {
    return from(super.join(right, usingColumns));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final scala.collection.Seq<String> usingColumns,
                           final String joinType) {
    return from(super.join(right, usingColumns, joinType));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final String usingColumn) {
    return from(super.join(right, usingColumn));
  }

  @Override
  public Dataset<T> limit(final int n) {
    return from(super.limit(n));
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
    return from(super.orderBy(sortExprs));
  }

  @Override
  public Dataset<T> orderBy(final scala.collection.Seq<Column> sortExprs) {
    return from(super.orderBy(sortExprs));
  }

  @Override
  public Dataset<T> orderBy(final String sortCol, final scala.collection.Seq<String> sortCols) {
    return from(super.orderBy(sortCol, sortCols));
  }

  @Override
  public Dataset<T> orderBy(final String sortCol, final String... sortCols) {
    return from(super.orderBy(sortCol, sortCols));
  }

  @Override
  public Dataset<T> persist() {
    return from(super.persist());
  }

  @Override
  public Dataset<T> persist(final StorageLevel newLevel) {
    return from(super.persist(newLevel));
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
    return from(super.repartition(partitionExprs));
  }

  @Override
  public Dataset<T> repartition(final int numPartitions) {
    return from(super.repartition(numPartitions));
  }

  @Override
  public Dataset<T> repartition(final int numPartitions, final Column... partitionExprs) {
    return from(super.repartition(numPartitions, partitionExprs));
  }

  @Override
  public Dataset<T> repartition(final int numPartitions, final scala.collection.Seq<Column> partitionExprs) {
    return from(super.repartition(numPartitions, partitionExprs));
  }

  @Override
  public Dataset<T> repartition(final scala.collection.Seq<Column> partitionExprs) {
    return from(super.repartition(partitionExprs));
  }

  @Override
  public Dataset<T> sample(final boolean withReplacement, final double fraction) {
    return from(super.sample(withReplacement, fraction));
  }

  @Override
  public Dataset<T> sample(final boolean withReplacement, final double fraction, final long seed) {
    return from(super.sample(withReplacement, fraction, seed));
  }

  @Override
  public Dataset<Row> select(final Column... cols) {
    return from(super.select(cols));
  }

  @Override
  public Dataset<Row> select(final scala.collection.Seq<Column> cols) {
    return from(super.select(cols));
  }

  @Override
  public Dataset<Row> select(final String col, final scala.collection.Seq<String> cols) {
    return from(super.select(col, cols));
  }

  @Override
  public Dataset<Row> select(final String col, final String... cols) {
    return from(super.select(col, cols));
  }

  @Override
  public Dataset<Row> selectExpr(final scala.collection.Seq<String> exprs) {
    return from(super.selectExpr(exprs));
  }

  @Override
  public Dataset<Row> selectExpr(final String... exprs) {
    return from(super.selectExpr(exprs));
  }

  @Override
  public Dataset<T> sort(final Column... sortExprs) {
    return from(super.sort(sortExprs));
  }

  @Override
  public Dataset<T> sort(final scala.collection.Seq<Column> sortExprs) {
    return from(super.sort(sortExprs));
  }

  @Override
  public Dataset<T> sort(final String sortCol, final scala.collection.Seq<String> sortCols) {
    return from(super.sort(sortCol, sortCols));
  }

  @Override
  public Dataset<T> sort(final String sortCol, final String... sortCols) {
    return from(super.sort(sortCol, sortCols));
  }

  @Override
  public Dataset<T> sortWithinPartitions(final Column... sortExprs) {
    return from(super.sortWithinPartitions(sortExprs));
  }

  @Override
  public Dataset<T> sortWithinPartitions(final scala.collection.Seq<Column> sortExprs) {
    return from(super.sortWithinPartitions(sortExprs));
  }

  @Override
  public Dataset<T> sortWithinPartitions(final String sortCol, final scala.collection.Seq<String> sortCols) {
    return from(super.sortWithinPartitions(sortCol, sortCols));
  }

  @Override
  public Dataset<T> sortWithinPartitions(final String sortCol, final String... sortCols) {
    return from(super.sortWithinPartitions(sortCol, sortCols));
  }

  @Override
  public SparkSession sparkSession() {
    return (SparkSession) super.sparkSession();
  }

  @Override
  public Dataset<Row> toDF() {
    return from(super.toDF());
  }

  @Override
  public Dataset<Row> toDF(final scala.collection.Seq<String> colNames) {
    return from(super.toDF(colNames));
  }

  @Override
  public Dataset<Row> toDF(final String... colNames) {
    return from(super.toDF(colNames));
  }

  @Override
  public Dataset<String> toJSON() {
    return from(super.toJSON());
  }

  @Override
  public <U> Dataset<U> transform(
      final scala.Function1<org.apache.spark.sql.Dataset<T>, org.apache.spark.sql.Dataset<U>> t) {
    return from(super.transform(t));
  }

  @Override
  public Dataset<T> union(final org.apache.spark.sql.Dataset<T> other) {
    return from(super.union(other));
  }

  @Override
  public Dataset<T> unpersist() {
    return from(super.unpersist());
  }

  @Override
  public Dataset<T> unpersist(final boolean blocking) {
    return from(super.unpersist(blocking));
  }

  @Override
  public Dataset<T> where(final Column condition) {
    return from(super.where(condition));
  }

  @Override
  public Dataset<T> where(final String conditionExpr) {
    return from(super.where(conditionExpr));
  }

  @Override
  public Dataset<Row> withColumn(final String colName, final Column col) {
    return from(super.withColumn(colName, col));
  }

  @Override
  public Dataset<Row> withColumnRenamed(final String existingName, final String newName) {
    return from(super.withColumnRenamed(existingName, newName));
  }
}
