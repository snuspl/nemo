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
package edu.snu.vortex.compiler.ir.execution_property.edge;

import edu.snu.vortex.compiler.ir.execution_property.ExecutionProperty;

/**
 * DataStore ExecutionProperty.
 */
public final class DataStore extends ExecutionProperty<String> {
  private DataStore(final String value) {
    super(Key.DataStore, value);
  }

  public static DataStore of(final String value) {
    return new DataStore(value);
  }

  // List of default pre-configured values. TODO #479: Remove static values.
  public static final String MEMORY = "Memory";
  public static final String LOCAL_FILE = "LocalFile";
  public static final String REMOTE_FILE = "RemoteFile";
}
