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
package edu.snu.vortex.compiler.ir.attribute;

import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.Vertex;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * AttributeMap Class, which uses HashMap for keeping track of attributes for operators and edges.
 */
public final class AttributeMap {
  private final String id;
  private final Map<Attribute.Key, Attribute> attributes;
  private final Map<Attribute.IntegerKey, Integer> intAttributes;

  private AttributeMap(final String id) {
    this.id = id;
    attributes = new HashMap<>();
    intAttributes = new HashMap<>();
  }

  public static AttributeMap of(final Edge edge) {
    final AttributeMap map = new AttributeMap(edge.getId());
    map.setDefaultEdgeValues();
    return map;
  }
  public static AttributeMap of(final Vertex vertex) {
    final AttributeMap map = new AttributeMap(vertex.getId());
    map.setDefaultVertexValues();
    return map;
  }

  private void setDefaultEdgeValues() {
    this.attributes.put(Attribute.Key.Partitioning, Attribute.Hash);
  }
  private void setDefaultVertexValues() {
    this.intAttributes.put(Attribute.IntegerKey.Parallelism, 1);
  }

  public Attribute put(final Attribute.Key key, final Attribute val) {
    if (!val.hasKey(key)) {
      throw new RuntimeException("Attribute " + val + " is not a member of Key " + key);
    }
    return attributes.put(key, val);
  }

  public Integer put(final Attribute.IntegerKey key, final Integer integer) {
    return intAttributes.put(key, integer);
  }

  public Attribute get(final Attribute.Key key) {
    return attributes.get(key);
  }

  public Integer get(final Attribute.IntegerKey key) {
    return intAttributes.get(key);
  }

  public Attribute remove(final Attribute.Key key) {
    return attributes.remove(key);
  }

  public void forEachAttr(final BiConsumer<? super Attribute.Key, ? super Attribute> action) {
    attributes.forEach(action);
  }

  public void forEachIntAttr(final BiConsumer<? super Attribute.IntegerKey, ? super Integer> action) {
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

    AttributeMap that = (AttributeMap) o;

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
