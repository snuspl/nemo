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
package dag;

import dag.node.Node;

import java.util.HashMap;

public class Edge<I, O> {
  public enum Type {
    M2M,
    O2M,
    O2O,
    N2N
  }

  private final String id;
  private final HashMap<String, Object> attributes;
  private final Type type;
  private final Node<?, I> src;
  private final Node<O, ?> dst;

  Edge(final Type type,
       final Node<?, I> src,
       final Node<O, ?> dst) {
    this.id = IdManager.newEdgeId();
    attributes = new HashMap<>(0);
    this.type = type;
    this.src = src;
    this.dst = dst;
  }

  public String getId() {
    return id;
  }

  Edge setAttr(final String key, final Object val) {
    attributes.put(key, val);
    return this;
  }

  Object getAttr(final String key) {
    return attributes.get(key);
  }

  public Type getType() {
    return type;
  }

  public Node<?, I> getSrc() {
    return src;
  }

  public Node<O, ?> getDst() {
    return dst;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("id: ");
    sb.append(id);
    sb.append(", src: ");
    sb.append(src.getId());
    sb.append(", dst: ");
    sb.append(dst.getId());
    sb.append(", attributes: ");
    sb.append(attributes);
    sb.append(", type: ");
    sb.append(type);
    return sb.toString();
  }
}
