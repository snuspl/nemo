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
package edu.snu.vortex.compiler.frontend.beam.operator;

import edu.snu.vortex.compiler.ir.operator.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;

/**
 * BoundedWindow operator implementation.
 * @param <T> type.
 */
public class BoundedWindowImpl<T> extends BoundedWindow<T> {
  private final WindowFn windowFn;

  public BoundedWindowImpl(final WindowFn windowFn) {
    this.windowFn = windowFn;
  }
}
