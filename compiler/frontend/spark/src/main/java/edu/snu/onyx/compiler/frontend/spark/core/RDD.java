package edu.snu.onyx.compiler.frontend.spark.core;

import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.LoopVertex;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import scala.collection.Iterator;
import scala.reflect.ClassTag$;

import javax.annotation.Nullable;
import java.util.Stack;

public class RDD<T> extends org.apache.spark.rdd.RDD<T> {
  private final Integer parallelism;
  private final Stack<LoopVertex> loopVertexStack;
  private final DAG<IRVertex, IREdge> dag;
  @Nullable private final IRVertex lastVertex;
  private final Serializer serializer;

  /**
   * Static method to create a RDD object.
   * @param sparkContext spark context containing configurations.
   * @param parallelism parallelism information.
   * @param <T> type of the resulting object.
   * @return the new JavaRDD object.
   */
  public static <T> RDD<T> of(final SparkContext sparkContext, final Integer parallelism) {
    return new RDD<>(sparkContext, parallelism,
        new DAGBuilder<IRVertex, IREdge>().buildWithoutSourceSinkCheck(), null);
  }

  /**
   * Constructor.
   * @param sparkContext spark context containing configurations.
   * @param parallelism parallelism information.
   * @param dag the current DAG.
   * @param lastVertex last vertex added to the builder.
   */
  RDD(final SparkContext sparkContext, final Integer parallelism,
      final DAG<IRVertex, IREdge> dag, @Nullable final IRVertex lastVertex) {
    super(sparkContext, null, ClassTag$.MODULE$.apply((Class<T>) Object.class));

    this.loopVertexStack = new Stack<>();
    this.parallelism = parallelism;
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
//
//  /////////////// TRANSFORMATIONS ///////////////
//
//  /**
//   * Set initialized source.
//   * @param initialData initial data.
//   * @return the RDD with the initialized source vertex.
//   */
//  public RDD<T> setSource(final Iterable<T> initialData) {
//    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);
//
//    final IRVertex initializedSourceVertex = new InitializedSourceVertex<>(initialData);
//    initializedSourceVertex.setProperty(ParallelismProperty.of(parallelism));
//    builder.addVertex(initializedSourceVertex, loopVertexStack);
//
//    return new RDD<>((SparkContext) this.sparkContext(), this.parallelism,
//        builder.buildWithoutSourceSinkCheck(), initializedSourceVertex);
//  }
//
//  /**
//   * Set source.
//   * @param rdd RDD to read data from.
//   * @return the RDD with the bounded source vertex.
//   */
//  public RDD<T> setSource(final org.apache.spark.rdd.RDD<T> rdd) {
//    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);
//
//    final IRVertex sparkBoundedSourceVertex = new SparkBoundedSourceVertex<>(rdd);
//    sparkBoundedSourceVertex.setProperty(ParallelismProperty.of(parallelism));
//    builder.addVertex(sparkBoundedSourceVertex, loopVertexStack);
//
//    return new RDD<>((SparkContext) this.sparkContext(), this.parallelism,
//        builder.buildWithoutSourceSinkCheck(), sparkBoundedSourceVertex);
//  }
//
//  /**
//   * Map transform.
//   * @param func function to apply.
//   * @param <U> output type.
//   * @return the JavaRDD with the DAG.
//   */
//  @Override
//  public <U> RDD<U> map(scala.Function1<T, U> func, scala.reflect.ClassTag<U> evidence$3) {
//    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);
//
//    final IRVertex mapVertex = new OperatorVertex(new MapTransform<>(func));
//    mapVertex.setProperty(ParallelismProperty.of(parallelism));
//    builder.addVertex(mapVertex, loopVertexStack);
//
//    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, mapVertex),
//        lastVertex, mapVertex, new SparkCoder(serializer));
//    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
//    builder.connectVertices(newEdge);
//
//    return new RDD<>((SparkContext) this.sparkContext(), this.parallelism,
//        builder.buildWithoutSourceSinkCheck(), mapVertex);
//  }
//
//  /////////////// MISC ///////////////
//
//  /**
//   * Retrieve communication pattern of the edge.
//   * @param src source vertex.
//   * @param dst destination vertex.
//   * @return the communication pattern.
//   */
//  static DataCommunicationPatternProperty.Value getEdgeCommunicationPattern(final IRVertex src,
//                                                                            final IRVertex dst) {
//    if (dst instanceof OperatorVertex
//        && (((OperatorVertex) dst).getTransform() instanceof ReduceByKeyTransform
//        || ((OperatorVertex) dst).getTransform() instanceof GroupByKeyTransform)) {
//      return DataCommunicationPatternProperty.Value.Shuffle;
//    } else {
//      return DataCommunicationPatternProperty.Value.OneToOne;
//    }
//  }
}
