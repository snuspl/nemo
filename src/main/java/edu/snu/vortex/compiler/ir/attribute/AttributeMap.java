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

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.attribute.edge.DataFlowModel;
import edu.snu.vortex.compiler.ir.attribute.edge.DataStore;
import edu.snu.vortex.compiler.ir.attribute.edge.Partitioning;
import edu.snu.vortex.compiler.ir.attribute.vertex.ExecutorPlacement;
import edu.snu.vortex.compiler.ir.attribute.vertex.Parallelism;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * AttributeMap Class, which uses HashMap for keeping track of attributes for vertices and edges.
 */
public final class AttributeMap implements Serializable {
  private final String id;
  private final Map<ExecutionFactor.Type, ExecutionFactor<String>> stringAttributes;
  private final Map<ExecutionFactor.Type, ExecutionFactor<Integer>> intAttributes;
  private final Map<ExecutionFactor.Type, ExecutionFactor<Boolean>> boolAttributes;

  /**
   * Constructor for AttributeMap class.
   * @param id ID of the vertex / edge to keep the attribute of.
   */
  private AttributeMap(final String id) {
    this.id = id;
    stringAttributes = new HashMap<>();
    intAttributes = new HashMap<>();
    boolAttributes = new HashMap<>();
  }

  /**
   * Static initializer for irEdges.
   * @param irEdge irEdge to keep the attributes of.
   * @return The corresponding AttributeMap.
   */
  public static AttributeMap of(final IREdge irEdge) {
    final AttributeMap map = new AttributeMap(irEdge.getId());
    map.setDefaultEdgeValues(irEdge.getType());
    return map;
  }
  /**
   * Static initializer for irVertex.
   * @param irVertex irVertex to keep the attributes of.
   * @return The corresponding AttributeMap.
   */
  public static AttributeMap of(final IRVertex irVertex) {
    final AttributeMap map = new AttributeMap(irVertex.getId());
    map.setDefaultVertexValues();
    return map;
  }

  /**
   * Putting default attributes for edges.
   * @param type type of the edge.
   */
  private void setDefaultEdgeValues(final IREdge.Type type) {
    this.put(Partitioning.of(Partitioning.HASH));
    this.put(DataFlowModel.of(DataFlowModel.PULL));
    switch (type) {
      case OneToOne:
        this.put(DataStore.of(DataStore.MEMORY));
        break;
      default:
        this.put(DataStore.of(DataStore.LOCAL_FILE));
        break;
    }
  }
  /**
   * Putting default attributes for vertices.
   */
  private void setDefaultVertexValues() {
    this.put(ExecutorPlacement.of(ExecutorPlacement.NONE));
    this.put(Parallelism.of(1));
  }

  /**
   * ID of the item this AttributeMap class is keeping track of.
   * @return the ID of the item this AttributeMap class is keeping track of.
   */
  public String getId() {
    return id;
  }

  /**
   * Put the given execution factor in the AttributeMap.
   * @param executionFactor execution factor to insert.
   * @return the inserted execution factor.
   */
  public ExecutionFactor put(final ExecutionFactor<?> executionFactor) {
    if (executionFactor.getAttribute() instanceof String) {
      return stringAttributes.put(executionFactor.getType(), (ExecutionFactor<String>) executionFactor);
    } else if (executionFactor.getAttribute() instanceof Integer) {
      return intAttributes.put(executionFactor.getType(), (ExecutionFactor<Integer>) executionFactor);
    } else if (executionFactor.getAttribute() instanceof Boolean) {
      return boolAttributes.put(executionFactor.getType(), (ExecutionFactor<Boolean>) executionFactor);
    } else {
      throw new RuntimeException(AttributeMap.class.getSimpleName()
          + " doesn't yet support the attribute with the given object type: " + executionFactor.getAttribute());
    }
  }

  public Object get(final ExecutionFactor.Type executionFactorType) {
    final ExecutionFactor<?> executionFactor;
    if (stringAttributes.containsKey(executionFactorType)) {
      executionFactor = stringAttributes.get(executionFactorType);
    } else if (intAttributes.containsKey(executionFactorType)) {
      executionFactor = intAttributes.get(executionFactorType);
    } else if (boolAttributes.containsKey(executionFactorType)) {
      executionFactor = boolAttributes.get(executionFactorType);
    } else {
      executionFactor = null;
    }
    return executionFactor == null ? null : executionFactor.getAttribute();
  }
  /**
   * Get the value of the given execution factor type.
   * @param executionFactorType the execution factor type to find the value of.
   * @return the value of the given execution factor.
   */
  public String getStringAttr(final ExecutionFactor.Type executionFactorType) {
    final ExecutionFactor<String> executionFactor = stringAttributes.get(executionFactorType);
    return executionFactor == null ? null : executionFactor.getAttribute();
  }

  /**
   * Get the value of the given execution factor type.
   * @param executionFactorType the execution factor type to find the value of.
   * @return the value of the given execution factor.
   */
  public Integer getIntegerAttr(final ExecutionFactor.Type executionFactorType) {
    final ExecutionFactor<Integer> executionFactor = intAttributes.get(executionFactorType);
    return executionFactor == null ? null : executionFactor.getAttribute();
  }

  /**
   * Get the value of the given execution factor type.
   * @param executionFactorType the execution factor type to find the value of.
   * @return the value of the given execution factor.
   */
  public Boolean getBooleanAttr(final ExecutionFactor.Type executionFactorType) {
    final ExecutionFactor<Boolean> executionFactor = boolAttributes.get(executionFactorType);
    return executionFactor == null ? null : executionFactor.getAttribute();
  }

  /**
   * remove the attribute.
   * @param key key of the attribute to remove.
   * @return the removed attribute.
   */
  public ExecutionFactor remove(final ExecutionFactor.Type key) {
    if (stringAttributes.containsKey(key)) {
      return stringAttributes.remove(key);
    } else if (intAttributes.containsKey(key)) {
      return intAttributes.remove(key);
    } else if (boolAttributes.containsKey(key)) {
      return boolAttributes.remove(key);
    } else {
      return null;
    }
  }

  /**
   * @param key key to look for
   * @return whether or not the attribute map contains the key.
   */
  public boolean containsKey(final ExecutionFactor.Type key) {
    return stringAttributes.containsKey(key) || intAttributes.containsKey(key) || boolAttributes.containsKey(key);
  }

  /**
   * Same as forEach function in Java 8, but for attributes.
   * @param action action to apply to each attributes.
   */
  public void forEachAttr(final Consumer<? super ExecutionFactor> action) {
    stringAttributes.values().forEach(action);
    intAttributes.values().forEach(action);
    boolAttributes.values().forEach(action);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    boolean isFirstPair = true;
    for (final Map.Entry<ExecutionFactor.Type, ExecutionFactor<String>> pair : stringAttributes.entrySet()) {
      if (!isFirstPair) {
        sb.append(", ");
      }
      isFirstPair = false;
      sb.append("\"");
      sb.append(pair.getKey());
      sb.append("\": \"");
      sb.append(pair.getValue().getAttribute());
      sb.append("\"");
    }
    for (final Map.Entry<ExecutionFactor.Type, ExecutionFactor<Integer>> pair : intAttributes.entrySet()) {
      if (!isFirstPair) {
        sb.append(", ");
      }
      isFirstPair = false;
      sb.append("\"");
      sb.append(pair.getKey());
      sb.append("\": \"");
      sb.append(pair.getValue().getAttribute());
      sb.append("\"");
    }
    for (final Map.Entry<ExecutionFactor.Type, ExecutionFactor<Boolean>> pair : boolAttributes.entrySet()) {
      if (!isFirstPair) {
        sb.append(", ");
      }
      isFirstPair = false;
      sb.append("\"");
      sb.append(pair.getKey());
      sb.append("\": \"");
      sb.append(pair.getValue().getAttribute());
      sb.append("\"");
    }
    sb.append("}");
    return sb.toString();
  }

  // Apache commons-lang 3 Equals/HashCodeBuilder template.
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    AttributeMap that = (AttributeMap) obj;

    return new EqualsBuilder()
        .append(stringAttributes.values().stream().map(ExecutionFactor::getAttribute).collect(Collectors.toSet()),
            that.stringAttributes.values().stream().map(ExecutionFactor::getAttribute).collect(Collectors.toSet()))
        .append(intAttributes.values().stream().map(ExecutionFactor::getAttribute).collect(Collectors.toSet()),
            that.intAttributes.values().stream().map(ExecutionFactor::getAttribute).collect(Collectors.toSet()))
        .append(boolAttributes.values().stream().map(ExecutionFactor::getAttribute).collect(Collectors.toSet()),
            that.boolAttributes.values().stream().map(ExecutionFactor::getAttribute).collect(Collectors.toSet()))
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(stringAttributes.values().stream().map(ExecutionFactor::getAttribute).collect(Collectors.toSet()))
        .append(intAttributes.values().stream().map(ExecutionFactor::getAttribute).collect(Collectors.toSet()))
        .append(boolAttributes.values().stream().map(ExecutionFactor::getAttribute).collect(Collectors.toSet()))
        .toHashCode();
  }
}
