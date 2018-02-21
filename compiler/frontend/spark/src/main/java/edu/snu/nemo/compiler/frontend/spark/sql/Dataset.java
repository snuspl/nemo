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
  private final SparkSession sparkSession;

  /**
   * Constructor.
   * @param sparkSession spark session.
   * @param logicalPlan spark logical plan.
   * @param encoder spark encoder.
   */
  private Dataset(final SparkSession sparkSession, final LogicalPlan logicalPlan, final Encoder<T> encoder) {
    super(sparkSession, logicalPlan, encoder);
    this.sparkSession = sparkSession;
  }

  /**
   * Using the immutable property of datasets, we can downcast spark datasets to our class using this function.
   * @param dataset the Spark dataset.
   * @param <U> type of the dataset.
   * @return our dataset class.
   */
  public static <U> Dataset<U> from(final org.apache.spark.sql.Dataset<U> dataset) {
    return new Dataset<>((SparkSession) dataset.sparkSession(), dataset.logicalPlan(), dataset.exprEnc());
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
    sparkSession.appendCommand("Dataset#agg", expr, exprs);
    return from(super.agg(expr, exprs));
  }

  @Override
  public Dataset<Row> agg(final Column expr, final scala.collection.Seq<Column> exprs) {
    sparkSession.appendCommand("Dataset#agg", expr, exprs);
    return from(super.agg(expr, exprs));
  }

  @Override
  public Dataset<Row> agg(final scala.collection.immutable.Map<String, String> exprs) {
    sparkSession.appendCommand("Dataset#agg", exprs);
    return from(super.agg(exprs));
  }

  @Override
  public Dataset<Row> agg(final java.util.Map<String, String> exprs) {
    sparkSession.appendCommand("Dataset#agg", exprs);
    return from(super.agg(exprs));
  }

  @Override
  public Dataset<Row> agg(final scala.Tuple2<String, String> aggExpr,
                          final scala.collection.Seq<scala.Tuple2<String, String>> aggExprs) {
    sparkSession.appendCommand("Dataset#agg", aggExpr, aggExprs);
    return from(super.agg(aggExpr, aggExprs));
  }

  @Override
  public Dataset<T> alias(final String alias) {
    sparkSession.appendCommand("Dataset#alias", alias);
    return from(super.alias(alias));
  }

  @Override
  public Dataset<T> alias(final scala.Symbol alias) {
    sparkSession.appendCommand("Dataset#alias", alias);
    return from(super.alias(alias));
  }

  @Override
  public Dataset<T> as(final String alias) {
    sparkSession.appendCommand("Dataset#as", alias);
    return from(super.as(alias));
  }

  @Override
  public Dataset<T> as(final scala.Symbol alias) {
    sparkSession.appendCommand("Dataset#as", alias);
    return from(super.as(alias));
  }

  @Override
  public Dataset<T> cache() {
    sparkSession.appendCommand("Dataset#cache");
    return from(super.cache());
  }

  @Override
  public Dataset<T> checkpoint() {
    sparkSession.appendCommand("Dataset#checkpoint");
    return from(super.checkpoint());
  }

  @Override
  public Dataset<T> checkpoint(final boolean eager) {
    sparkSession.appendCommand("Dataset#checkpoint", eager);
    return from(super.checkpoint(eager));
  }

  @Override
  public Dataset<T> coalesce(final int numPartitions) {
    sparkSession.appendCommand("Dataset#coalesce", numPartitions);
    return from(super.coalesce(numPartitions));
  }

  @Override
  public Dataset<Row> crossJoin(final org.apache.spark.sql.Dataset<?> right) {
    sparkSession.appendCommand("Dataset#crossJoin", right);
    return from(super.crossJoin(right));
  }

  @Override
  public Dataset<Row> describe(final scala.collection.Seq<String> cols) {
    sparkSession.appendCommand("Dataset#describe", cols);
    return from(super.describe(cols));
  }

  @Override
  public Dataset<Row> describe(final String... cols) {
    sparkSession.appendCommand("Dataset#describe", cols);
    return from(super.describe(cols));
  }

  @Override
  public Dataset<T> distinct() {
    sparkSession.appendCommand("Dataset#distinct");
    return from(super.distinct());
  }

  @Override
  public Dataset<Row> drop(final Column col) {
    sparkSession.appendCommand("Dataset#drop", col);
    return from(super.drop(col));
  }

  @Override
  public Dataset<Row> drop(final scala.collection.Seq<String> colNames) {
    sparkSession.appendCommand("Dataset#drop", colNames);
    return from(super.drop(colNames));
  }

  @Override
  public Dataset<Row> drop(final String... colNames) {
    sparkSession.appendCommand("Dataset#drop", colNames);
    return from(super.drop(colNames));
  }

  @Override
  public Dataset<Row> drop(final String colName) {
    sparkSession.appendCommand("Dataset#drop", colName);
    return from(super.drop(colName));
  }

  @Override
  public Dataset<T> dropDuplicates() {
    sparkSession.appendCommand("Dataset#dropDuplicates");
    return from(super.dropDuplicates());
  }

  @Override
  public Dataset<T> dropDuplicates(final scala.collection.Seq<String> colNames) {
    sparkSession.appendCommand("Dataset#dropDuplicates", colNames);
    return from(super.dropDuplicates(colNames));
  }

  @Override
  public Dataset<T> dropDuplicates(final String[] colNames) {
    sparkSession.appendCommand("Dataset#dropDuplicates", colNames);
    return from(super.dropDuplicates(colNames));
  }

  @Override
  public Dataset<T> dropDuplicates(final String col1, final scala.collection.Seq<String> cols) {
    sparkSession.appendCommand("Dataset#dropDuplicates", col1, cols);
    return from(super.dropDuplicates(col1, cols));
  }

  @Override
  public Dataset<T> dropDuplicates(final String col1, final String... cols) {
    sparkSession.appendCommand("Dataset#dropDuplicates", col1, cols);
    return from(super.dropDuplicates(col1, cols));
  }

  @Override
  public Dataset<T> except(final org.apache.spark.sql.Dataset<T> other) {
    sparkSession.appendCommand("Dataset#except", other);
    return from(super.except(other));
  }

  @Override
  public Dataset<T> filter(final Column condition) {
    sparkSession.appendCommand("Dataset#filter", condition);
    return from(super.filter(condition));
  }

  @Override
  public Dataset<T> filter(final String conditionExpr) {
    sparkSession.appendCommand("Dataset#filter", conditionExpr);
    return from(super.filter(conditionExpr));
  }

  @Override
  public Dataset<T> hint(final String name, final Object... parameters) {
    sparkSession.appendCommand("Dataset#hint", name, parameters);
    return from(super.hint(name, parameters));
  }

  @Override
  public Dataset<T> hint(final String name, final scala.collection.Seq<Object> parameters) {
    sparkSession.appendCommand("Dataset#hint", name, parameters);
    return from(super.hint(name, parameters));
  }

  @Override
  public Dataset<T> intersect(final org.apache.spark.sql.Dataset<T> other) {
    sparkSession.appendCommand("Dataset#intersect", other);
    return from(super.intersect(other));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right) {
    sparkSession.appendCommand("Dataset#join", right);
    return from(super.join(right));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final Column joinExprs) {
    sparkSession.appendCommand("Dataset#join", right, joinExprs);
    return from(super.join(right, joinExprs));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final Column joinExprs, final String joinType) {
    sparkSession.appendCommand("Dataset#join", right, joinExprs, joinType);
    return from(super.join(right, joinExprs, joinType));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right,
                           final scala.collection.Seq<String> usingColumns) {
    sparkSession.appendCommand("Dataset#join", right, usingColumns);
    return from(super.join(right, usingColumns));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final scala.collection.Seq<String> usingColumns,
                           final String joinType) {
    sparkSession.appendCommand("Dataset#join", right, usingColumns, joinType);
    return from(super.join(right, usingColumns, joinType));
  }

  @Override
  public Dataset<Row> join(final org.apache.spark.sql.Dataset<?> right, final String usingColumn) {
    sparkSession.appendCommand("Dataset#join", right, usingColumn);
    return from(super.join(right, usingColumn));
  }

  @Override
  public Dataset<T> limit(final int n) {
    sparkSession.appendCommand("Dataset#limit", n);
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
    sparkSession.appendCommand("Dataset#orderBy", sortExprs);
    return from(super.orderBy(sortExprs));
  }

  @Override
  public Dataset<T> orderBy(final scala.collection.Seq<Column> sortExprs) {
    sparkSession.appendCommand("Dataset#orderBy", sortExprs);
    return from(super.orderBy(sortExprs));
  }

  @Override
  public Dataset<T> orderBy(final String sortCol, final scala.collection.Seq<String> sortCols) {
    sparkSession.appendCommand("Dataset#orderBy", sortCol, sortCols);
    return from(super.orderBy(sortCol, sortCols));
  }

  @Override
  public Dataset<T> orderBy(final String sortCol, final String... sortCols) {
    sparkSession.appendCommand("Dataset#orderBy", sortCol, sortCols);
    return from(super.orderBy(sortCol, sortCols));
  }

  @Override
  public Dataset<T> persist() {
    sparkSession.appendCommand("Dataset#persist");
    return from(super.persist());
  }

  @Override
  public Dataset<T> persist(final StorageLevel newLevel) {
    sparkSession.appendCommand("Dataset#persist", newLevel);
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
    sparkSession.appendCommand("Dataset#repartition", partitionExprs);
    return from(super.repartition(partitionExprs));
  }

  @Override
  public Dataset<T> repartition(final int numPartitions) {
    sparkSession.appendCommand("Dataset#repartition", numPartitions);
    return from(super.repartition(numPartitions));
  }

  @Override
  public Dataset<T> repartition(final int numPartitions, final Column... partitionExprs) {
    sparkSession.appendCommand("Dataset#repartition", numPartitions, partitionExprs);
    return from(super.repartition(numPartitions, partitionExprs));
  }

  @Override
  public Dataset<T> repartition(final int numPartitions, final scala.collection.Seq<Column> partitionExprs) {
    sparkSession.appendCommand("Dataset#repartition", numPartitions, partitionExprs);
    return from(super.repartition(numPartitions, partitionExprs));
  }

  @Override
  public Dataset<T> repartition(final scala.collection.Seq<Column> partitionExprs) {
    sparkSession.appendCommand("Dataset#repartition", partitionExprs);
    return from(super.repartition(partitionExprs));
  }

  @Override
  public Dataset<T> sample(final boolean withReplacement, final double fraction) {
    sparkSession.appendCommand("Dataset#sample", withReplacement, fraction);
    return from(super.sample(withReplacement, fraction));
  }

  @Override
  public Dataset<T> sample(final boolean withReplacement, final double fraction, final long seed) {
    sparkSession.appendCommand("Dataset#sample", withReplacement, fraction, seed);
    return from(super.sample(withReplacement, fraction, seed));
  }

  @Override
  public Dataset<Row> select(final Column... cols) {
    sparkSession.appendCommand("Dataset#select", cols);
    return from(super.select(cols));
  }

  @Override
  public Dataset<Row> select(final scala.collection.Seq<Column> cols) {
    sparkSession.appendCommand("Dataset#select", cols);
    return from(super.select(cols));
  }

  @Override
  public Dataset<Row> select(final String col, final scala.collection.Seq<String> cols) {
    sparkSession.appendCommand("Dataset#select", col, cols);
    return from(super.select(col, cols));
  }

  @Override
  public Dataset<Row> select(final String col, final String... cols) {
    sparkSession.appendCommand("Dataset#select", col, cols);
    return from(super.select(col, cols));
  }

  @Override
  public Dataset<Row> selectExpr(final scala.collection.Seq<String> exprs) {
    sparkSession.appendCommand("Dataset#selectExpr", exprs);
    return from(super.selectExpr(exprs));
  }

  @Override
  public Dataset<Row> selectExpr(final String... exprs) {
    sparkSession.appendCommand("Dataset#selectExpr", exprs);
    return from(super.selectExpr(exprs));
  }

  @Override
  public Dataset<T> sort(final Column... sortExprs) {
    sparkSession.appendCommand("Dataset#sort", sortExprs);
    return from(super.sort(sortExprs));
  }

  @Override
  public Dataset<T> sort(final scala.collection.Seq<Column> sortExprs) {
    sparkSession.appendCommand("Dataset#sort", sortExprs);
    return from(super.sort(sortExprs));
  }

  @Override
  public Dataset<T> sort(final String sortCol, final scala.collection.Seq<String> sortCols) {
    sparkSession.appendCommand("Dataset#sort", sortCol, sortCols);
    return from(super.sort(sortCol, sortCols));
  }

  @Override
  public Dataset<T> sort(final String sortCol, final String... sortCols) {
    sparkSession.appendCommand("Dataset#sort", sortCol, sortCols);
    return from(super.sort(sortCol, sortCols));
  }

  @Override
  public Dataset<T> sortWithinPartitions(final Column... sortExprs) {
    sparkSession.appendCommand("Dataset#sortWithinPartitions", sortExprs);
    return from(super.sortWithinPartitions(sortExprs));
  }

  @Override
  public Dataset<T> sortWithinPartitions(final scala.collection.Seq<Column> sortExprs) {
    sparkSession.appendCommand("Dataset#sortWithinPartitions", sortExprs);
    return from(super.sortWithinPartitions(sortExprs));
  }

  @Override
  public Dataset<T> sortWithinPartitions(final String sortCol, final scala.collection.Seq<String> sortCols) {
    sparkSession.appendCommand("Dataset#sortWithinPartitions", sortCol, sortCols);
    return from(super.sortWithinPartitions(sortCol, sortCols));
  }

  @Override
  public Dataset<T> sortWithinPartitions(final String sortCol, final String... sortCols) {
    sparkSession.appendCommand("Dataset#sortWithinPartitions", sortCol, sortCols);
    return from(super.sortWithinPartitions(sortCol, sortCols));
  }

  @Override
  public SparkSession sparkSession() {
    return (SparkSession) super.sparkSession();
  }

  @Override
  public Dataset<Row> toDF() {
    sparkSession.appendCommand("Dataset#toDF");
    return from(super.toDF());
  }

  @Override
  public Dataset<Row> toDF(final scala.collection.Seq<String> colNames) {
    sparkSession.appendCommand("Dataset#toDF", colNames);
    return from(super.toDF(colNames));
  }

  @Override
  public Dataset<Row> toDF(final String... colNames) {
    sparkSession.appendCommand("Dataset#toDF", colNames);
    return from(super.toDF(colNames));
  }

  @Override
  public Dataset<String> toJSON() {
    sparkSession.appendCommand("Dataset#toJSON");
    return from(super.toJSON());
  }

  @Override
  public <U> Dataset<U> transform(
      final scala.Function1<org.apache.spark.sql.Dataset<T>, org.apache.spark.sql.Dataset<U>> t) {
    sparkSession.appendCommand("Dataset#transform", t);
    return from(super.transform(t));
  }

  @Override
  public Dataset<T> union(final org.apache.spark.sql.Dataset<T> other) {
    sparkSession.appendCommand("Dataset#union", other);
    return from(super.union(other));
  }

  @Override
  public Dataset<T> unpersist() {
    sparkSession.appendCommand("Dataset#unpersist");
    return from(super.unpersist());
  }

  @Override
  public Dataset<T> unpersist(final boolean blocking) {
    sparkSession.appendCommand("Dataset#unpersist", blocking);
    return from(super.unpersist(blocking));
  }

  @Override
  public Dataset<T> where(final Column condition) {
    sparkSession.appendCommand("Dataset#where", condition);
    return from(super.where(condition));
  }

  @Override
  public Dataset<T> where(final String conditionExpr) {
    sparkSession.appendCommand("Dataset#where", conditionExpr);
    return from(super.where(conditionExpr));
  }

  @Override
  public Dataset<Row> withColumn(final String colName, final Column col) {
    sparkSession.appendCommand("Dataset#withColumn", colName, col);
    return from(super.withColumn(colName, col));
  }

  @Override
  public Dataset<Row> withColumnRenamed(final String existingName, final String newName) {
    sparkSession.appendCommand("Dataset#withColumnRenamed", existingName, newName);
    return from(super.withColumnRenamed(existingName, newName));
  }
}
