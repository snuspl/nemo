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
package edu.snu.vortex.compiler.frontend.beam;

import edu.snu.vortex.compiler.ir.Element;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

/**
 * Element implementation for Beam.
 * @param <T> Type of the WindowedValue.
 */
public final class BeamElement<T> implements Element<WindowedValue<T>, Object> {
  private final WindowedValue windowedValue;

  public BeamElement(final WindowedValue<T> wv) {
    this.windowedValue = wv;
  }

  @Override
  public WindowedValue<T> getData() {
    return windowedValue;
  }

  @Override
  public Object getKey() {
    return ((KV) windowedValue.getValue()).getKey();
  }

  @Override
  public String toString() {
    return windowedValue.toString();
  }
}
