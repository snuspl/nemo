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
 * DataFlowModel ExecutionFactor.
 */
public final class DataFlowModel extends ExecutionFactor<String> {
  private DataFlowModel(final String attribute) {
    super(Type.DataFlowModel, attribute, String.class);
  }

  public static  DataFlowModel of(final String dataFlowModel) {
    return new DataFlowModel(dataFlowModel);
  }

  // List of default pre-configured attributes.
  public static final String PULL = "Pull";
  public static final String PUSH = "Push";
}
