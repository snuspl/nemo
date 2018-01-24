package edu.snu.onyx.compiler.frontend.spark.source;

import edu.snu.onyx.common.ir.Reader;
import edu.snu.onyx.common.ir.vertex.InitializedSourceVertex;
import edu.snu.onyx.common.ir.vertex.SourceVertex;
import edu.snu.onyx.compiler.frontend.spark.sql.Dataset;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext$;
import scala.collection.JavaConversions;

import java.util.*;

/**
 * Bounded source vertex for Spark.
 * @param <T> type of data to read.
 */
public final class SparkBoundedSourceVertex<T> extends SourceVertex<T> {
  private final List<Reader<T>> readers;

  /**
   * Constructor.
   * Note that we have to first create our iterators here and supply them to our readers.
   * @param dataset Dataset to read data from.
   */
  public SparkBoundedSourceVertex(final Dataset<T> dataset) {
    this.readers = new ArrayList<>();

    for (final Partition partition: dataset.rdd().getPartitions()) {
      final Iterator<T> data = JavaConversions.seqAsJavaList(
          dataset.rdd().compute(partition, TaskContext$.MODULE$.empty()).toSeq()
      ).iterator();
      final List<T> dataForReader = new ArrayList<>();
      data.forEachRemaining(dataForReader::add);
      readers.add(new InitializedSourceVertex.InitializedSourceReader<>(dataForReader));
    }
  }

  public SparkBoundedSourceVertex(final List<Reader<T>> readers) {
    this.readers = readers;
  }

  @Override
  public SparkBoundedSourceVertex getClone() {
    final SparkBoundedSourceVertex<T> that = new SparkBoundedSourceVertex<>(this.readers);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public List<Reader<T>> getReaders(final int desiredNumOfSplits) throws Exception {
    return this.readers;
  }
}
