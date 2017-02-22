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
 * An object serializer interface.
 * All object serializer implementations used in {@link edu.snu.vortex.runtime.common.channel.Channel}
 * should minimally support.
 */
public interface ObjectSerializer {

    /**
   * Serialize an object into the given storage (maybe set by the constructor).
   * @param obj The object to be serialized.
   * @throws IOException If an I/O error has occurred.
   */
  void writeObject(Object obj) throws IOException;

  /**
   * Release all internal resources and close the serializer.
   * @throws IOException If an I/O error has occurred.
   */
  void close() throws IOException;
}
