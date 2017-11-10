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
package edu.snu.onyx.common;

import edu.snu.onyx.common.dag.DAG;

import java.io.*;
import java.util.Base64;

/**
 * Encodes {@link DAG} into {@link String} or vice versa.
 */
public final class DAGCodec {
  /**
   * Private constructor.
   */
  private DAGCodec() {
  }

  /**
   * Encodes a {@link DAG} into a {@link String}.
   * @param dag {@link DAG} to encode
   * @return Encoded {@link String}
   * @throws IOException on {@link IOException} from object stream
   */
  public static String encode(final DAG dag) throws IOException {
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
    objectOutputStream.writeObject(dag);
    objectOutputStream.close();
    final String base64 = Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());
    return base64;
  }

  /**
   * Decodes a {@link String} into a {@link DAG}.
   * @param string {@link String} to decode
   * @return Decoded {@link DAG}
   * @throws IOException on {@link IOException} from object stream
   * @throws ClassNotFoundException when it's not able to find the class definition
   */
  public static DAG decode(final String string) throws IOException, ClassNotFoundException {
    final byte[] bytes = Base64.getDecoder().decode(string);
    final ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
    final DAG dag = (DAG) objectInputStream.readObject();
    objectInputStream.close();
    return dag;
  }
}
