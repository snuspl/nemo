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
package edu.snu.vortex.compiler.ir.attribute.edge;

import edu.snu.vortex.compiler.ir.attribute.ExecutionFactor;

/**
 * DataStore ExecutionFactor.
 */
public final class DataStore extends ExecutionFactor<String> {
  private DataStore(final String attribute) {
    super(Type.DataStore, attribute);
  }

  public static DataStore of(final String dataStore) {
    return new DataStore(dataStore);
  }

  // List of default pre-configured attributes.
  public static final String MEMORY = "Memory";
  public static final String LOCAL_FILE = "LocalFile";
  public static final String REMOTE_FILE = "RemoteFile";
}
