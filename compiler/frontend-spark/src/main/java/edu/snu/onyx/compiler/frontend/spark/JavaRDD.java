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
package edu.snu.onyx.compiler.frontend.spark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.KeyExtractorProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.InitializedSourceVertex;
import edu.snu.onyx.common.ir.vertex.LoopVertex;
import edu.snu.onyx.common.ir.vertex.OperatorVertex;
import edu.snu.onyx.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.onyx.compiler.frontend.spark.coder.SparkCoder;
import edu.snu.onyx.compiler.frontend.spark.transform.MapTransform;
import edu.snu.onyx.compiler.frontend.spark.transform.ReduceTransform;
import edu.snu.onyx.compiler.frontend.spark.transform.SerializableBinaryOperator;
import edu.snu.onyx.compiler.frontend.spark.transform.SerializableFunction;
import org.apache.spark.serializer.KryoSerializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Stack;

/**
 * Java RDD.
 * @param <T> type of the final element.
 */
public final class JavaRDD<T extends Serializable> {
  private final SparkContext sparkContext;
  private final Integer parallelism;
  private final Stack<LoopVertex> loopVertexStack;
  private DAGBuilder<IRVertex, IREdge> builder;
  private final IRVertex lastVertex;
  private final KryoSerializer kryoSerializer;

  /**
   * Constructor to start with.
   * @param sparkContext spark context containing configurations.
   * @param parallelism parallelism information.
   */
  JavaRDD(final SparkContext sparkContext, final Integer parallelism) {
    this(sparkContext, parallelism, new DAGBuilder<>(), null);
  }

  /**
   * Constructor.
   * @param sparkContext spark context containing configurations.
   * @param parallelism parallelism information.
   * @param builder the builder for the DAG.
   * @param lastVertex last vertex added to the builder.
   */
  private JavaRDD(final SparkContext sparkContext, final Integer parallelism,
                  final DAGBuilder<IRVertex, IREdge> builder, final IRVertex lastVertex) {
    this.loopVertexStack = new Stack<>();
    this.sparkContext = sparkContext;
    this.parallelism = parallelism;
    this.builder = builder;
    this.lastVertex = lastVertex;
    this.kryoSerializer = new KryoSerializer(sparkContext.conf());
  }

  /////////////// TRANSFORMATIONS ///////////////

  /**
   * Set initialized source.
   * @param initialData initial data.
   * @return the Java RDD with the initialized source vertex.
   */
  JavaRDD<T> setSource(final Iterable<T> initialData) {
    final IRVertex initializedSourceVertex = new InitializedSourceVertex<>(initialData);
    initializedSourceVertex.setProperty(ParallelismProperty.of(parallelism));
    builder.addVertex(initializedSourceVertex, loopVertexStack);

    return new JavaRDD<>(this.sparkContext, this.parallelism, this.builder, initializedSourceVertex);
  }

  /**
   * Map transform.
   * @param func function to apply.
   * @param <O> output type.
   * @return the JavaRDD with the DAG.
   */
  public <O extends Serializable> JavaRDD<O> map(final SerializableFunction<T, O> func) {
    final IRVertex mapVertex = new OperatorVertex(new MapTransform<>(func));
    mapVertex.setProperty(ParallelismProperty.of(parallelism));
    builder.addVertex(mapVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, mapVertex),
        lastVertex, mapVertex, new SparkCoder(kryoSerializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new JavaRDD<>(this.sparkContext, this.parallelism, this.builder, mapVertex);
  }


  /////////////// ACTIONS ///////////////

  /**
   * Reduce action.
   * @param func function (binary operator) to apply.
   * @return the result of the reduce action.
   */
  public T reduce(final SerializableBinaryOperator<T> func) {
    // save result in a temporary file
    final String resultFile = System.getProperty("user.dir") + "/reduceresult.bin";

    final IRVertex reduceVertex = new OperatorVertex(new ReduceTransform<>(func, resultFile));
    reduceVertex.setProperty(ParallelismProperty.of(parallelism));
    builder.addVertex(reduceVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, reduceVertex),
        lastVertex, reduceVertex, new SparkCoder(kryoSerializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    // launch DAG
    final DAG<IRVertex, IREdge> dag = this.builder.build();
    this.builder = new DAGBuilder<>();
    JobLauncher.launchDAG(dag);

    // Retrieve result data.
    try {
      final Kryo kryo = new Kryo();
      final Input input = new Input(new FileInputStream(resultFile));
      final T result = (T) kryo.readClassAndObject(input);
      input.close();
      // Delete temporary file
      new File(resultFile).delete();
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /////////////// MISC ///////////////

  /**
   * Retrieve communication pattern of the edge.
   * @param src source vertex.
   * @param dst destination vertex.
   * @return the communication pattern.
   */
  private static DataCommunicationPatternProperty.Value getEdgeCommunicationPattern(final IRVertex src,
                                                                                    final IRVertex dst) {
    if (dst instanceof OperatorVertex && ((OperatorVertex) dst).getTransform() instanceof ReduceTransform) {
      return DataCommunicationPatternProperty.Value.Shuffle;
    } else {
      return DataCommunicationPatternProperty.Value.OneToOne;
    }
  }
}
