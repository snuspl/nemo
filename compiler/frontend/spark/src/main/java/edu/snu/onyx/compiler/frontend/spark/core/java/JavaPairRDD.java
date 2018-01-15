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
package edu.snu.onyx.compiler.frontend.spark.core.java;

import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.LoopVertex;
import edu.snu.onyx.compiler.frontend.spark.core.SparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Stack;

public class JavaPairRDD<K, V> extends org.apache.spark.api.java.JavaPairRDD<K, V> {
  private final SparkContext sparkContext;
  private final Integer parallelism;
  private final Stack<LoopVertex> loopVertexStack;
  private final DAG<IRVertex, IREdge> dag;
  @Nullable private final IRVertex lastVertex;
  private final Serializer serializer;

  /**
   * Constructor.
   * @param sparkContext spark context containing configurations.
   * @param parallelism parallelism information.
   * @param dag the current DAG.
   * @param lastVertex last vertex added to the builder.
   */
  JavaPairRDD(final SparkContext sparkContext, final Integer parallelism,
              final DAG<IRVertex, IREdge> dag, @Nullable final IRVertex lastVertex) {
    // TODO #366: resolve while implementing scala RDD.
    super(null, null, null);

    this.loopVertexStack = new Stack<>();
    this.sparkContext = sparkContext;
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

  /**
   * @return the spark context.
   */
  public SparkContext getSparkContext() {
    return sparkContext;
  }

  /////////////// TRANSFORMATIONS ///////////////

  @Override
  public JavaPairRDD<K, V> reduceByKey(Function2<V, V, V> func) {
    // TODO
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

  /////////////// ACTIONS ///////////////

  @Override
  public List<Tuple2<K, V>> collect() {
    // TODO
    throw new UnsupportedOperationException("Operation not yet implemented.");
  }

}
