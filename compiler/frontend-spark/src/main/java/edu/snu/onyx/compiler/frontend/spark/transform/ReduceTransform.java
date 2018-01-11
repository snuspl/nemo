/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.onyx.compiler.frontend.spark.transform;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import edu.snu.onyx.common.ir.OutputCollector;
import edu.snu.onyx.common.ir.vertex.transform.Transform;
import edu.snu.onyx.compiler.frontend.spark.JavaRDD;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.stream.StreamSupport;

/**
 * Reduce Transform for Spark.
 * @param <T> element type.
 */
public final class ReduceTransform<T extends Serializable> implements Transform<T, T> {
  private final SerializableBinaryOperator<T> func;
  private OutputCollector<T> oc;
  private String filename;

  /**
   * Constructor.
   * @param func function to run for the reduce transform.
   * @param filename file to keep the result in.
   */
  public ReduceTransform(final SerializableBinaryOperator<T> func, final String filename) {
    this.func = func;
    this.filename = filename;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<T> outputCollector) {
    this.oc = outputCollector;
    this.filename = filename + JavaRDD.getResultId();
  }

  @Override
  public void onData(final Iterator<T> elements, final String srcVertexId) {
    final Iterable<T> iterable = () -> elements;
    final T res = StreamSupport.stream(iterable.spliterator(), true)
        .reduce(func)
        .orElseThrow(() -> new RuntimeException("Something wrong with the provided reduce operator"));
    oc.emit(res);

    // Write result to a temporary file.
    try {
      final Kryo kryo = new Kryo();
      final Output output = new Output(new FileOutputStream(filename));
      kryo.writeClassAndObject(output, res);
      output.close();
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
  }
}
