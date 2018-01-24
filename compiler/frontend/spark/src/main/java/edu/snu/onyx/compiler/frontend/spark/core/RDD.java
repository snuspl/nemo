package edu.snu.onyx.compiler.frontend.spark.core;

import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.LoopVertex;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import scala.collection.Iterator;
import scala.reflect.ClassTag$;

import javax.annotation.Nullable;
import java.util.Stack;

/**
 * RDD for Onyx.
 * @param <T> type of data.
 */
public final class RDD<T> extends org.apache.spark.rdd.RDD<T> {
  private final Stack<LoopVertex> loopVertexStack;
  private final DAG<IRVertex, IREdge> dag;
  @Nullable private final IRVertex lastVertex;
  private final Serializer serializer;

  /**
   * Static method to create a RDD object.
   * @param sparkContext spark context containing configurations.
   * @param <T> type of the resulting object.
   * @return the new JavaRDD object.
   */
  public static <T> RDD<T> of(final SparkContext sparkContext) {
    return new RDD<>(sparkContext, new DAGBuilder<IRVertex, IREdge>().buildWithoutSourceSinkCheck(), null);
  }

  /**
   * Constructor.
   * @param sparkContext spark context containing configurations.
   * @param dag the current DAG.
   * @param lastVertex last vertex added to the builder.
   */
  private RDD(final SparkContext sparkContext, final DAG<IRVertex, IREdge> dag, @Nullable final IRVertex lastVertex) {
    super(sparkContext, null, ClassTag$.MODULE$.apply((Class<T>) Object.class));

    this.loopVertexStack = new Stack<>();
    this.dag = dag;
    this.lastVertex = lastVertex;
    if (sparkContext.conf().get("spark.serializer", "")
        .equals("org.apache.spark.serializer.KryoSerializer")) {
      this.serializer = new KryoSerializer(sparkContext.conf());
    } else {
      this.serializer = new JavaSerializer(sparkContext.conf());
    }
  }

  @Override
  public Iterator<T> compute(final Partition partition, final TaskContext taskContext) {
    throw new UnsupportedOperationException("Operation unsupported.");
  }

  @Override
  public Partition[] getPartitions() {
    throw new UnsupportedOperationException("Operation unsupported.");
  }
}
