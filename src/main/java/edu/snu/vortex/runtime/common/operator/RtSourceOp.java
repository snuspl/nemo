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
package edu.snu.vortex.runtime.common.operator;

import edu.snu.vortex.runtime.common.execplan.RtOperator;
import edu.snu.vortex.runtime.common.execplan.RuntimeAttributes;

import java.util.List;
import java.util.Map;

/**
 * RtSourceOp operator.
 * @param <O> output type.
 */
public abstract class RtSourceOp<O> extends RtOperator<Void, O> {
  public RtSourceOp(final String irOpId, final Map<RuntimeAttributes.OperatorAttribute, Object> rtOpAttr) {
    super(irOpId, rtOpAttr);
  }

  /**
   * Getter for readers.
   * @return List of readers.
   */
  public abstract List<Reader<O>> getReaders();

  /**
   * Interface for reader.
   * @param <O> output type.
   */
  public interface Reader<O> {
    Iterable<O> read() throws Exception;
  }
}
