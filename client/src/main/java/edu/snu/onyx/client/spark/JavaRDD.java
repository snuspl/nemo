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
package edu.snu.onyx.client.spark;

import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.common.coder.BytesCoder;
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
import edu.snu.onyx.compiler.frontend.spark.transform.MapTransform;
import edu.snu.onyx.compiler.frontend.spark.transform.ReduceTransform;
import edu.snu.onyx.compiler.frontend.spark.transform.SerializableBinaryOperator;
import edu.snu.onyx.compiler.frontend.spark.transform.SerializableFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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

  /**
   * Constructor to start with.
   * @param sparkContext spark context.
   * @param parallelism parallelism information.
   */
  JavaRDD(final SparkContext sparkContext, final Integer parallelism) {
    this(sparkContext, parallelism, new DAGBuilder<>(), null);
  }

  /**
   * Constructor.
   * @param sparkContext spark context.
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
  }

  ///////////// TRANSFORMATIONS ////////////////

  /**
   * Set initialized source.
   * @param initialData initial data.
   * @return the Java RDD with the initialized source vertex.
   */
  JavaRDD<T> setSource(final List<T> initialData) {
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
        lastVertex, mapVertex, new BytesCoder());
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new JavaRDD<>(this.sparkContext, this.parallelism, this.builder, mapVertex);
  }


  ////////////// ACTIONS ////////////////

  /**
   * Reduce action.
   * @param func function to apply.
   * @return the result of the reduce action.
   */
  public T reduce(final SerializableBinaryOperator<T> func) {
    final List<T> result = new ArrayList<>();

    final IRVertex reduceVertex = new OperatorVertex(new ReduceTransform<>(func, result));
    reduceVertex.setProperty(ParallelismProperty.of(parallelism));
    builder.addVertex(reduceVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, reduceVertex),
        lastVertex, reduceVertex, new BytesCoder());
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    final DAG<IRVertex, IREdge> dag = this.builder.build();
    this.builder = new DAGBuilder<>();
    JobLauncher.launchDAG(dag);

    return result.iterator().next();
  }

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
