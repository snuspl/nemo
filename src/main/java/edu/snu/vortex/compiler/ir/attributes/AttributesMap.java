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
package edu.snu.vortex.compiler.ir.attributes;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * AttributesMap Class, which uses HashMap for keeping track of attributes for operators and edges.
 */
public final class AttributesMap {
  private final Map<Attributes.Key, Attributes> attributes;
  private final Map<Attributes.IntegerKey, Integer> intAttributes;

  public AttributesMap() {
    attributes = new HashMap<>();
    intAttributes = new HashMap<>();
  }

  public Attributes put(final Attributes.Key key, final Attributes val) {
    if (!val.hasKey(key)) {
      throw new RuntimeException("Attribute " + val + " is not a member of Key " + key);
    }
    return attributes.put(key, val);
  }

  public Integer put(final Attributes.IntegerKey key, final Integer integer) {
    return intAttributes.put(key, integer);
  }

  public Attributes get(final Attributes.Key key) {
    return attributes.get(key);
  }

  public Integer get(final Attributes.IntegerKey key) {
    return intAttributes.get(key);
  }

  public Attributes remove(final Attributes.Key key) {
    return attributes.remove(key);
  }

  public void forEachAttr(final BiConsumer<? super Attributes.Key, ? super Attributes> action) {
    attributes.forEach(action);
  }

  public void forEachIntAttr(final BiConsumer<? super Attributes.IntegerKey, ? super Integer> action) {
    intAttributes.forEach(action);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(attributes);
    return sb.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AttributesMap that = (AttributesMap) o;

    if (!attributes.equals(that.attributes)) {
      return false;
    }
    return intAttributes.equals(that.intAttributes);
  }

  @Override
  public int hashCode() {
    int result = attributes.hashCode();
    result = 31 * result + intAttributes.hashCode();
    return result;
  }
}
