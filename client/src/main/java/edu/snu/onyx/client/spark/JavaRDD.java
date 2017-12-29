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

import edu.snu.onyx.common.coder.BytesCoder;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.LoopVertex;
import edu.snu.onyx.common.ir.vertex.OperatorVertex;
import edu.snu.onyx.compiler.frontend.spark.transform.MapTransform;
import edu.snu.onyx.compiler.frontend.spark.transform.ReduceTransform;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public class JavaRDD<T> {
  final SparkContext sparkContext;
  final Integer parallelism;
  private List initialData;
  private final Stack<LoopVertex> loopVertexStack;
  final DAGBuilder<IRVertex, IREdge> builder;
  private final IRVertex lastVertex;

  JavaRDD(final SparkContext sparkContext, final Integer parallelism, final List initialData) {
    this(sparkContext, parallelism, initialData, new DAGBuilder<>(), null);
  }

  JavaRDD(final SparkContext sparkContext, final Integer parallelism, final List initialData,
          final DAGBuilder<IRVertex, IREdge> builder, final IRVertex lastVertex) {
    this.loopVertexStack = new Stack<>();
    this.sparkContext = sparkContext;
    this.parallelism = parallelism;
    this.initialData = initialData;
    this.builder = builder;
    this.lastVertex = lastVertex;
  }

  // TRANSFORMATIONS
  public <O> JavaRDD<O> map(final Function<T, O> func) {
    final IRVertex mapVertex = new OperatorVertex(new MapTransform<>(func));
    builder.addVertex(mapVertex, loopVertexStack);
    if (lastVertex != null) {
      final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, mapVertex),
          lastVertex, mapVertex, new BytesCoder());
      builder.connectVertices(newEdge);
    }
    return new JavaRDD<>(this.sparkContext, this.parallelism, this.initialData, this.builder, mapVertex);
  }


  // ACTIONS
  public T reduce(final BinaryOperator<T> func) {
    final List<T> result = new ArrayList<>();
    final IRVertex reduceVertex = new OperatorVertex(new ReduceTransform<>(func, result));
    builder.addVertex(reduceVertex, loopVertexStack);
    if (lastVertex != null) {
      final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, reduceVertex),
          lastVertex, reduceVertex, new BytesCoder());
      builder.connectVertices(newEdge);
    }
    return result.iterator().next();
  }

  private static DataCommunicationPatternProperty.Value getEdgeCommunicationPattern(final IRVertex src,
                                                                                    final IRVertex dst) {
    if (dst instanceof OperatorVertex && ((OperatorVertex) dst).getTransform() instanceof ReduceTransform) {
      return DataCommunicationPatternProperty.Value.Shuffle;
    } else {
      return DataCommunicationPatternProperty.Value.OneToOne;
    }
  }
}
