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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.KeyExtractorProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.LoopVertex;
import edu.snu.onyx.common.ir.vertex.OperatorVertex;
import edu.snu.onyx.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.onyx.compiler.frontend.spark.SparkKeyExtractor;
import edu.snu.onyx.compiler.frontend.spark.coder.SparkCoder;
import edu.snu.onyx.compiler.frontend.spark.core.RDD;
import edu.snu.onyx.compiler.frontend.spark.core.SparkContext;
import edu.snu.onyx.compiler.frontend.spark.transform.CollectTransform;
import edu.snu.onyx.compiler.frontend.spark.transform.ReduceByKeyTransform;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import static edu.snu.onyx.compiler.frontend.spark.core.java.JavaRDD.getEdgeCommunicationPattern;

/**
 * Java RDD for pairs.
 * @param <K> key type.
 * @param <V> value type.
 */
public final class JavaPairRDD<K, V> extends org.apache.spark.api.java.JavaPairRDD<K, V> {
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
    super(RDD.<Tuple2<K, V>>of(sparkContext, parallelism),
        ClassTag$.MODULE$.apply((Class<K>) Object.class), ClassTag$.MODULE$.apply((Class<V>) Object.class));

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
  public JavaPairRDD<K, V> reduceByKey(final Function2<V, V, V> func) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    final IRVertex reduceByKeyVertex = new OperatorVertex(new ReduceByKeyTransform<K, V>(func));
    reduceByKeyVertex.setProperty(ParallelismProperty.of(parallelism));
    builder.addVertex(reduceByKeyVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, reduceByKeyVertex),
        lastVertex, reduceByKeyVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    return new JavaPairRDD<>(this.sparkContext, this.parallelism,
        builder.buildWithoutSourceSinkCheck(), reduceByKeyVertex);
  }

  /////////////// ACTIONS ///////////////

  @Override
  public List<Tuple2<K, V>> collect() {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    // save result in a temporary file
    // TODO #740: remove this part, and make it properly transfer with executor.
    final String resultFile = System.getProperty("user.dir") + "/collectresult";

    final IRVertex collectVertex = new OperatorVertex(new CollectTransform<>(resultFile));
    collectVertex.setProperty(ParallelismProperty.of(parallelism));
    builder.addVertex(collectVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, collectVertex),
        lastVertex, collectVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    // launch DAG
    JobLauncher.launchDAG(builder.build());

    // Retrieve result data from file.
    // TODO #740: remove this part, and make it properly transfer with executor.
    try {
      final Kryo kryo = new Kryo();
      final List<Tuple2<K, V>> result = new ArrayList<>();
      Integer i = 0;
      // TODO #740: remove this part, and make it properly transfer with executor.
      File file = new File(resultFile + i);
      while (file.exists()) {
        final Input input = new Input(new FileInputStream(resultFile + i));
        result.add((Tuple2<K, V>) kryo.readClassAndObject(input));
        input.close();

        // Delete temporary file
        file.delete();
        file = new File(resultFile + ++i);
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
