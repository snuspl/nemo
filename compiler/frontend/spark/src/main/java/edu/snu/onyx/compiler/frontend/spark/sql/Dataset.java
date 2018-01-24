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

import edu.snu.onyx.compiler.frontend.spark.core.java.JavaRDD;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext$;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
   * Note that we have to first create our iterators here and supply them to our source vertex.
   * @return the new javaRDD component.
   */
  @Override
  public JavaRDD<T> javaRDD() {
    final List<Iterator<T>> l = new ArrayList<>();
    final Integer parallelism = super.rdd().getNumPartitions();
    for (final Partition partition: super.rdd().getPartitions()) {
      final Iterator<T> data = JavaConversions.seqAsJavaList(
          super.rdd().compute(partition, TaskContext$.MODULE$.empty()).toSeq()
      ).iterator();
      l.add(data);
    }

    ////////////////////////////OPTION2///////////////////////////
//    final LogicalPlan logicalPlan = dataset.logicalPlan();
//
//    dataset.sparkSession().sessionState().planner().strategies();
//
//    final Seq<SparkPlan> fileSourceStrategy = FileSourceStrategy$.MODULE$.apply(logicalPlan);
//
//    JavaConversions.seqAsJavaList(fileSourceStrategy).forEach(sparkPlan -> {
//      if (sparkPlan instanceof DataSourceScanExec) {
//        final FileScanRDD fileScanRDD = (FileScanRDD) sparkPlan.execute();
//        for (Partition partition: fileScanRDD.getPartitions()) {
//          final List<InternalRow> internalRows = JavaConversions.seqAsJavaList(
//              fileScanRDD.compute(partition, TaskContext$.MODULE$.empty()).toSeq());
//          final Iterator<T> data = internalRows.stream()
//              .map(row -> (T) row.get(0, dataset.exprEnc().deserializer().dataType()))
//              .iterator();
//          readers.add(new SparkBoundedSourceReader<>(data));
//        }
//      }
//    });
    //////////////////////////OPTION3////////////////////////////

//    final Seq<Expression> filters = PhysicalOperation.unapply(logicalPlan).get()._2();
//
//    final ExpressionSet filterSet = new ExpressionSet(new HashSet<>(), new ArrayBuffer<>());
//    JavaConversions.seqAsJavaList(filters).forEach(filterSet::add);
//
//    final Seq<Attribute> partitionColumns = logicalRelation.resolve(
//        fsRelation.partitionSchema(),
//        fsRelation.sparkSession().sessionState().analyzer().resolver());
//    final AttributeSet partitionSet = AttributeSet.apply(partitionColumns);
//    final ExpressionSet partitionKeyFilters = new ExpressionSet(new HashSet<>(), new ArrayBuffer<>());
//    JavaConversions.seqAsJavaList(filters).stream()
//        .filter(e -> e.references().subsetOf(partitionSet)).collect(Collectors.toList())
//        .forEach(partitionKeyFilters::add);
//
//    final Seq<Attribute> dataColumns = logicalRelation
//        .resolve(fsRelation.dataSchema(), fsRelation.sparkSession().sessionState().analyzer().resolver());
//
//    final Traversable<Expression> dataFilters = filters.filter(e -> e.references().intersect(partitionSet).isEmpty());
//
//    final Set<Expression> afterScanFilters = filterSet.filterNot(e ->
//        partitionKeyFilters.filter(f -> f.references().nonEmpty()).contains(e));
//
//    final AttributeSet filterAttributes = AttributeSet.apply(afterScanFilters);
//    final Seq<Attribute> requiredExpressions = filterAttributes.toSeq();
//    final AttributeSet requiredAttributes = AttributeSet.apply(requiredExpressions);
//
//    final Traversable<Attribute> readDataColumns = dataColumns.filter(requiredAttributes::contains)
//        .filterNot(partitionColumns::contains);
//    final StructType outputSchema = readDataColumns.map((Function1) (a -> new StructField(((Attribute) a).name(), ((Attribute) a).dataType(), ((Attribute) a).nullable(), ((Attribute) a).metadata())))
//
//    final Traversable<Expression> dataFilters = filters.filter(e -> e.references().intersect(partitionSet).isEmpty());
//
//    final Seq<Filter> pushedDownFilters = dataFilters.flatMap(e -> DataSourceStrategy.translateFilter(e).get());
//
//    final Function1<PartitionedFile, scala.collection.Iterator<InternalRow>> readerWithPartitionValues =
//        fsRelation.fileFormat().buildReaderWithPartitionValues(
//            fsRelation.sparkSession(),
//            fsRelation.dataSchema(),
//            fsRelation.partitionSchema(),
//            outputSchema,
//            pushedDownFilters,
//            fsRelation.options(),
//            fsRelation.sparkSession().sessionState().newHadoopConfWithOptions(fsRelation.options()));


    return JavaRDD.of(super.sparkSession().sparkContext(), l, parallelism);
  }
}
