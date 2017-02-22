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
package edu.snu.vortex.runtime.common;


import java.io.IOException;

/**
 * An object deserializer interface.
 * All deserializer implementations used in {@link edu.snu.vortex.runtime.common.channel.Channel}
 * should minimally support.
 */
public interface ObjectDeserializer {

  /**
   * Deserialize an object from the internal source (maybe set by the constructor).
   * @return A deserialized object.
   * @throws IOException If an I/O error has occurred.
   * @throws ClassNotFoundException If the class of the serialized object is not recognizable.
   */
  Object readObject() throws IOException, ClassNotFoundException;

  /**
   * Release all internal resources and close the deserializer.
   * @throws IOException If an I/O error has occurred.
   */
  void close() throws IOException;
}
