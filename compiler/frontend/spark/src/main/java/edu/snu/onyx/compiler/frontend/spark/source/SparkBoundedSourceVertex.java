package edu.snu.onyx.compiler.frontend.spark.source;

import edu.snu.onyx.common.ir.Reader;
import edu.snu.onyx.common.ir.vertex.SourceVertex;
import edu.snu.onyx.compiler.frontend.spark.sql.Dataset;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext$;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Bounded source vertex for Spark.
 * @param <T> type of data to read.
 */
public final class SparkBoundedSourceVertex<T> extends SourceVertex<T> {
  private final Dataset<T> dataset;

  /**
   * Constructor.
   * @param dataset Dataset to read data from.
   */
  public SparkBoundedSourceVertex(final Dataset<T> dataset) {
    this.dataset = dataset;
  }

  @Override
  public SparkBoundedSourceVertex getClone() {
    final SparkBoundedSourceVertex<T> that = new SparkBoundedSourceVertex<>(this.dataset);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public List<Reader<T>> getReaders(final int desiredNumOfSplits) throws Exception {
    final List<Reader<T>> readers = new ArrayList<>();

    for (final Partition partition: dataset.rdd().getPartitions()) {
      Iterator<T> data = JavaConversions.seqAsJavaList(
          dataset.rdd().compute(partition, TaskContext$.MODULE$.empty()).toSeq()).iterator();
      readers.add(new SparkBoundedSourceReader<>(data));
    }

    ///////////////////////////////////////////////////////
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
    //////////////////////////////////////////////////////

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

    return readers;
  }

  /**
   * SparkBoundedSourceReader class.
   * @param <T> type of data.
   */
  public class SparkBoundedSourceReader<T> implements Reader<T> {
    private final Iterator<T> data;

    /**
     * Constructor of SparkBoundedSourceReader.
     * @param iterator Iterator of data.
     */
    SparkBoundedSourceReader(final Iterator<T> iterator) {
      this.data = iterator;
    }

    @Override
    public final Iterator<T> read() {
      return this.data;
    }
  }
}
