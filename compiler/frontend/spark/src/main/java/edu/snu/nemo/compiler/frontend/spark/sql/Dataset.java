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
  public Dataset<Row>	agg(Column expr, Column... exprs) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	agg(Column expr, scala.collection.Seq<Column> exprs) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	agg(scala.collection.immutable.Map<String,String> exprs) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	agg(java.util.Map<String,String> exprs) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	agg(scala.Tuple2<String,String> aggExpr, scala.collection.Seq<scala.Tuple2<String,String>> aggExprs) {
    return from(super.);
  }

  @Override
  public Dataset<T>	alias(String alias) {
    return from(super.);
  }

  @Override
  public Dataset<T>	alias(scala.Symbol alias) {
    return from(super.);
  }

  @Override
  public Dataset<T>	as(String alias) {
    return from(super.);
  }

  @Override
  public Dataset<T>	as(scala.Symbol alias) {
    return from(super.);
  }

  @Override
  public Dataset<T>	cache() {
    return from(super.);
  }

  @Override
  public Dataset<T>	checkpoint() {
    return from(super.);
  }

  @Override
  public Dataset<T>	checkpoint(boolean eager) {
    return from(super.);
  }

  @Override
  public Dataset<T>	coalesce(int numPartitions) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	crossJoin(Dataset<?> right) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	describe(scala.collection.Seq<String> cols) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	describe(String... cols) {
    return from(super.);
  }

  @Override
  public Dataset<T>	distinct() {
    return from(super.);
  }

  @Override
  public Dataset<Row>	drop(Column col) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	drop(scala.collection.Seq<String> colNames) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	drop(String... colNames) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	drop(String colName) {
    return from(super.);
  }

  @Override
  public Dataset<T>	dropDuplicates() {
    return from(super.);
  }

  @Override
  public Dataset<T>	dropDuplicates(scala.collection.Seq<String> colNames) {
    return from(super.);
  }

  @Override
  public Dataset<T>	dropDuplicates(String[] colNames) {
    return from(super.);
  }

  @Override
  public Dataset<T>	dropDuplicates(String col1, scala.collection.Seq<String> cols) {
    return from(super.);
  }

  @Override
  public Dataset<T>	dropDuplicates(String col1, String... cols) {
    return from(super.);
  }

  @Override
  public Dataset<T>	except(Dataset<T> other) {
    return from(super.);
  }

  @Override
  public Dataset<T>	filter(Column condition) {
    return from(super.);
  }

  @Override
  public Dataset<T>	filter(String conditionExpr) {
    return from(super.);
  }

  @Override
  public Dataset<T>	hint(String name, Object... parameters) {
    return from(super.);
  }

  @Override
  public Dataset<T>	hint(String name, scala.collection.Seq<Object> parameters) {
    return from(super.);
  }

  @Override
  public Dataset<T>	intersect(Dataset<T> other) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	join(Dataset<?> right) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	join(Dataset<?> right, Column joinExprs) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	join(Dataset<?> right, Column joinExprs, String joinType) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	join(Dataset<?> right, scala.collection.Seq<String> usingColumns) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	join(Dataset<?> right, scala.collection.Seq<String> usingColumns, String joinType) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	join(Dataset<?> right, String usingColumn) {
    return from(super.);
  }

  @Override
  public Dataset<T>	limit(int n) {
    return from(super.);
  }

  @Override
  public static Dataset<Row>	ofRows(SparkSession sparkSession, org.apache.spark.sql.catalyst.plans.logical.LogicalPlan logicalPlan) {
    return from(super.);
  }

  @Override
  public Dataset<T>	orderBy(Column... sortExprs) {
    return from(super.);
  }

  @Override
  public Dataset<T>	orderBy(scala.collection.Seq<Column> sortExprs) {
    return from(super.);
  }

  @Override
  public Dataset<T>	orderBy(String sortCol, scala.collection.Seq<String> sortCols) {
    return from(super.);
  }

  @Override
  public Dataset<T>	orderBy(String sortCol, String... sortCols) {
    return from(super.);
  }

  @Override
  public Dataset<T>	persist() {
    return from(super.);
  }

  @Override
  public Dataset<T>	persist(StorageLevel newLevel) {
    return from(super.);
  }

  @Override
  public Dataset<T>[]	randomSplit(double[] weights) {
    return from(super.);
  }

  @Override
  public Dataset<T>[]	randomSplit(double[] weights, long seed) {
    return from(super.);
  }

  @Override
  public java.util.List<Dataset<T>>	randomSplitAsList(double[] weights, long seed) {
    return from(super.);
  }

  @Override
  public Dataset<T>	repartition(Column... partitionExprs) {
    return from(super.);
  }

  @Override
  public Dataset<T>	repartition(int numPartitions) {
    return from(super.);
  }

  @Override
  public Dataset<T>	repartition(int numPartitions, Column... partitionExprs) {
    return from(super.);
  }

  @Override
  public Dataset<T>	repartition(int numPartitions, scala.collection.Seq<Column> partitionExprs) {
    return from(super.);
  }

  @Override
  public Dataset<T>	repartition(scala.collection.Seq<Column> partitionExprs) {
    return from(super.);
  }

  @Override
  public Dataset<T>	sample(boolean withReplacement, double fraction) {
    return from(super.);
  }

  @Override
  public Dataset<T>	sample(boolean withReplacement, double fraction, long seed) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	select(Column... cols) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	select(scala.collection.Seq<Column> cols) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	select(String col, scala.collection.Seq<String> cols) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	select(String col, String... cols) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	selectExpr(scala.collection.Seq<String> exprs) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	selectExpr(String... exprs) {
    return from(super.);
  }

  @Override
  public Dataset<T>	sort(Column... sortExprs) {
    return from(super.);
  }

  @Override
  public Dataset<T>	sort(scala.collection.Seq<Column> sortExprs) {
    return from(super.);
  }

  @Override
  public Dataset<T>	sort(String sortCol, scala.collection.Seq<String> sortCols) {
    return from(super.);
  }

  @Override
  public Dataset<T>	sort(String sortCol, String... sortCols) {
    return from(super.);
  }

  @Override
  public Dataset<T>	sortWithinPartitions(Column... sortExprs) {
    return from(super.);
  }

  @Override
  public Dataset<T>	sortWithinPartitions(scala.collection.Seq<Column> sortExprs) {
    return from(super.);
  }

  @Override
  public Dataset<T>	sortWithinPartitions(String sortCol, scala.collection.Seq<String> sortCols) {
    return from(super.);
  }

  @Override
  public Dataset<T>	sortWithinPartitions(String sortCol, String... sortCols) {
    return from(super.);
  }

  @Override
  public SparkSession	sparkSession() {
    return (SparkSession) super.sparkSession();
  }

  @Override
  public Dataset<Row>	toDF() {
    return from(super.);
  }

  @Override
  public Dataset<Row>	toDF(scala.collection.Seq<String> colNames) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	toDF(String... colNames) {
    return from(super.);
  }

  @Override
  public Dataset<String>	toJSON() {
    return from(super.);
  }

  @Override
  public <U> Dataset<U>	transform(scala.Function1<Dataset<T>,Dataset<U>> t) {
    return from(super.);
  }

  @Override
  public Dataset<T>	union(Dataset<T> other) {
    return from(super.);
  }

  @Override
  public Dataset<T>	unpersist() {
    return from(super.);
  }

  @Override
  public Dataset<T>	unpersist(boolean blocking) {
    return from(super.);
  }

  @Override
  public Dataset<T>	where(Column condition) {
    return from(super.);
  }

  @Override
  public Dataset<T>	where(String conditionExpr) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	withColumn(String colName, Column col) {
    return from(super.);
  }

  @Override
  public Dataset<Row>	withColumnRenamed(String existingName, String newName) {
    return from(super.);
  }
}
