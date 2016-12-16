/*
 * Copyright (C) 2016 Seoul National University
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
package dag.examples;

import dag.node.Do;

import java.util.Map;

public class EmptyDo<I, O, T> extends Do<I, O, T> {
  private final String name;

  public EmptyDo(final String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(", name: ");
    sb.append(name);
    return sb.toString();
  }

  @Override
  public Iterable<O> compute(Iterable<I> input, Map<T, Object> broadcasted) {
    return null;
  }
}
