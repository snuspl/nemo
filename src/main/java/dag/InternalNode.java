package dag;/*
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class InternalNode<I, O> implements Node<I, O> {
  private final String id;
  private final HashMap<String, Object> attributes;
  private final Operator<I, O> operator;
  private final List<Edge<?, I>> inEdges = new ArrayList<>(0);
  private final List<Edge<O, ?>> outEdges = new ArrayList<>(0);

  enum Type {
    Source,
    Internal,
    Sink
  }

  public InternalNode(final Operator<I, O> operator) {
    id = IdManager.newNodeId();
    attributes = new HashMap<>();
    this.operator = operator;
  }

  public Type getType() {
    if (inEdges.size() == 0 && outEdges.size() == 0)
      throw new IllegalStateException();

    if (inEdges.size() == 0)
      return Type.Source;
    else if (outEdges.size() == 0)
      return Type.Sink;
    else
      return Type.Internal;
  }

  public String getId() {
    return id;
  }

  public HashMap<String, Object> getAttributes() {
    return attributes;
  }

  public List<Edge<?, I>> getInEdges() {
    return inEdges;
  }

  public List<Edge<O, ?>> getOutEdges() {
    return outEdges;
  }

  public void addInEdge(final Edge<?, I> edge) {
    // Checking
    inEdges.add(edge);
  }

  public void addOutEdge(final Edge<O, ?> edge) {
    // Checking
    outEdges.add(edge);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("id: ");
    sb.append(id);
    sb.append(", attributes: ");
    sb.append(attributes);
    sb.append(", operator: ");
    sb.append(operator);
    sb.append(", inEdges: ");
    sb.append(inEdges);
    sb.append(", outEdges: ");
    sb.append(outEdges);
    return sb.toString();
  }

}
