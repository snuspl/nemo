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
package edu.snu.onyx.compiler.frontend.spark.coder;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.snu.onyx.common.coder.Coder;
import org.apache.spark.serializer.KryoSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Kryo Spark Coder for serialization.
 * @param <T> type of the object to (de)serialize.
 */
public final class SparkCoder<T> implements Coder<T> {
  private final KryoSerializer kryoSerializer;

  /**
   * Default constructor.
   */
  public SparkCoder(final KryoSerializer kryoSerializer) {
    this.kryoSerializer = kryoSerializer;
  }

  @Override
  public void encode(final T element, final OutputStream outStream) throws IOException {
    final Output output = new Output(outStream);
    kryoSerializer.newKryo().writeClassAndObject(output, element);
    output.close();
  }

  @Override
  public T decode(final InputStream inStream) throws IOException {
    final Input input = new Input(inStream);
    final T obj = (T) kryoSerializer.newKryo().readClassAndObject(input);
    input.close();
    return obj;
  }
}
