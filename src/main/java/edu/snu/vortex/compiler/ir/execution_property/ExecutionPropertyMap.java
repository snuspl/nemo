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
package edu.snu.vortex.compiler.ir.execution_property;

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.execution_property.edge.DataFlowModelProperty;
import edu.snu.vortex.compiler.ir.execution_property.edge.DataStoreProperty;
import edu.snu.vortex.compiler.ir.execution_property.edge.PartitioningProperty;
import edu.snu.vortex.compiler.ir.execution_property.vertex.ExecutorPlacement;
import edu.snu.vortex.compiler.ir.execution_property.vertex.Parallelism;
import edu.snu.vortex.runtime.executor.data.LocalFileStore;
import edu.snu.vortex.runtime.executor.data.MemoryStore;
import edu.snu.vortex.runtime.executor.datatransfer.partitioning.Hash;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * ExecutionPropertyMap Class, which uses HashMap for keeping track of ExecutionProperties for vertices and edges.
 */
public final class ExecutionPropertyMap implements Serializable {
  private final String id;
  private final Map<ExecutionProperty.Key, ExecutionProperty<?>> properties;
  private final Map<ExecutionProperty.Key, ExecutionProperty<Class>> classProperties;
  private final Map<ExecutionProperty.Key, ExecutionProperty<Integer>> intProperties;
  private final Map<ExecutionProperty.Key, ExecutionProperty<Boolean>> boolProperties;

  /**
   * Constructor for ExecutionPropertyMap class.
   * @param id ID of the vertex / edge to keep the execution property of.
   */
  private ExecutionPropertyMap(final String id) {
    this.id = id;
    properties = new HashMap<>();
    classProperties = new HashMap<>();
    intProperties = new HashMap<>();
    boolProperties = new HashMap<>();
  }

  /**
   * Static initializer for irEdges.
   * @param irEdge irEdge to keep the execution property of.
   * @return The corresponding ExecutionPropertyMap.
   */
  public static ExecutionPropertyMap of(final IREdge irEdge) {
    final ExecutionPropertyMap map = new ExecutionPropertyMap(irEdge.getId());
    map.setDefaultEdgeValues(irEdge.getType());
    return map;
  }
  /**
   * Static initializer for irVertex.
   * @param irVertex irVertex to keep the execution property of.
   * @return The corresponding ExecutionPropertyMap.
   */
  public static ExecutionPropertyMap of(final IRVertex irVertex) {
    final ExecutionPropertyMap map = new ExecutionPropertyMap(irVertex.getId());
    map.setDefaultVertexValues();
    return map;
  }

  /**
   * Putting default execution property for edges.
   * @param type type of the edge.
   */
  private void setDefaultEdgeValues(final IREdge.Type type) {
    this.put(PartitioningProperty.of(Hash.class));
    this.put(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));
    switch (type) {
      case OneToOne:
        this.put(DataStoreProperty.of(MemoryStore.class));
        break;
      default:
        this.put(DataStoreProperty.of(LocalFileStore.class));
        break;
    }
  }
  /**
   * Putting default execution property for vertices.
   */
  private void setDefaultVertexValues() {
    this.put(ExecutorPlacement.of(ExecutorPlacement.NONE));
    this.put(Parallelism.of(1));
  }

  /**
   * ID of the item this ExecutionPropertyMap class is keeping track of.
   * @return the ID of the item this ExecutionPropertyMap class is keeping track of.
   */
  public String getId() {
    return id;
  }

  /**
   * Put the given execution property  in the ExecutionPropertyMap.
   * @param executionProperty execution property to insert.
   * @return the inserted execution property.
   */
  public ExecutionProperty put(final ExecutionProperty<?> executionProperty) {
    if (executionProperty.getValue() instanceof Integer) {
      return intProperties.put(executionProperty.getKey(), (ExecutionProperty<Integer>) executionProperty);
    } else if (executionProperty.getValue() instanceof Boolean) {
      return boolProperties.put(executionProperty.getKey(), (ExecutionProperty<Boolean>) executionProperty);
    } else if (executionProperty.getValue() instanceof Class) {
      return classProperties.put(executionProperty.getKey(), (ExecutionProperty<Class>) executionProperty);
    } else {
      return properties.put(executionProperty.getKey(), executionProperty);
    }
  }

  public Object get(final ExecutionProperty.Key executionPropertyKey) {
    final ExecutionProperty<?> executionProperty;
    if (intProperties.containsKey(executionPropertyKey)) {
      executionProperty = intProperties.get(executionPropertyKey);
    } else if (boolProperties.containsKey(executionPropertyKey)) {
      executionProperty = boolProperties.get(executionPropertyKey);
    } else if (classProperties.containsKey(executionPropertyKey)) {
      executionProperty = classProperties.get(executionPropertyKey);
    } else {
      executionProperty = properties.get(executionPropertyKey);
    }
    return executionProperty == null ? null : executionProperty.getValue();
  }
  /**
   * Get the value of the given execution property type.
   * @param executionPropertyKey the execution property type to find the value of.
   * @return the value of the given execution property.
   */
  public Class<?> getClassProperty(final ExecutionProperty.Key executionPropertyKey) {
    final ExecutionProperty<Class> executionProperty = classProperties.get(executionPropertyKey);
    return executionProperty == null ? null : executionProperty.getValue();
  }

  /**
   * Get the value of the given execution property type.
   * @param executionPropertyKey the execution property type to find the value of.
   * @return the value of the given execution property.
   */
  public Integer getIntegerProperty(final ExecutionProperty.Key executionPropertyKey) {
    final ExecutionProperty<Integer> executionProperty = intProperties.get(executionPropertyKey);
    return executionProperty == null ? null : executionProperty.getValue();
  }

  /**
   * Get the value of the given execution property type.
   * @param executionPropertyKey the execution property type to find the value of.
   * @return the value of the given execution property.
   */
  public Boolean getBooleanProperty(final ExecutionProperty.Key executionPropertyKey) {
    final ExecutionProperty<Boolean> executionProperty = boolProperties.get(executionPropertyKey);
    return executionProperty == null ? null : executionProperty.getValue();
  }

  /**
   * remove the execution property.
   * @param key key of the execution property to remove.
   * @return the removed execution property
   */
  public ExecutionProperty remove(final ExecutionProperty.Key key) {
    if (intProperties.containsKey(key)) {
      return intProperties.remove(key);
    } else if (boolProperties.containsKey(key)) {
      return boolProperties.remove(key);
    } else {
      return properties.remove(key);
    }
  }

  /**
   * @param key key to look for.
   * @return whether or not the execution property map contains the key.
   */
  public boolean containsKey(final ExecutionProperty.Key key) {
    return properties.containsKey(key) || intProperties.containsKey(key) || boolProperties.containsKey(key);
  }

  /**
   * Same as forEach function in Java 8, but for execution properties.
   * @param action action to apply to each of the execution properties.
   */
  public void forEachProperties(final Consumer<? super ExecutionProperty> action) {
    properties.values().forEach(action);
    intProperties.values().forEach(action);
    boolProperties.values().forEach(action);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    boolean isFirstPair = true;
    for (final Map.Entry<ExecutionProperty.Key, ExecutionProperty<?>> entry : properties.entrySet()) {
      if (!isFirstPair) {
        sb.append(", ");
      }
      isFirstPair = false;
      sb.append("\"");
      sb.append(entry.getKey());
      sb.append("\": \"");
      sb.append(entry.getValue().getValue());
      sb.append("\"");
    }
    for (final Map.Entry<ExecutionProperty.Key, ExecutionProperty<Integer>> entry : intProperties.entrySet()) {
      if (!isFirstPair) {
        sb.append(", ");
      }
      isFirstPair = false;
      sb.append("\"");
      sb.append(entry.getKey());
      sb.append("\": \"");
      sb.append(entry.getValue().getValue());
      sb.append("\"");
    }
    for (final Map.Entry<ExecutionProperty.Key, ExecutionProperty<Boolean>> entry : boolProperties.entrySet()) {
      if (!isFirstPair) {
        sb.append(", ");
      }
      isFirstPair = false;
      sb.append("\"");
      sb.append(entry.getKey());
      sb.append("\": \"");
      sb.append(entry.getValue().getValue());
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

    ExecutionPropertyMap that = (ExecutionPropertyMap) obj;

    return new EqualsBuilder()
        .append(properties.values().stream().map(ExecutionProperty::getValue).collect(Collectors.toSet()),
            that.properties.values().stream().map(ExecutionProperty::getValue).collect(Collectors.toSet()))
        .append(intProperties.values().stream().map(ExecutionProperty::getValue).collect(Collectors.toSet()),
            that.intProperties.values().stream().map(ExecutionProperty::getValue).collect(Collectors.toSet()))
        .append(boolProperties.values().stream().map(ExecutionProperty::getValue).collect(Collectors.toSet()),
            that.boolProperties.values().stream().map(ExecutionProperty::getValue).collect(Collectors.toSet()))
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(properties.values().stream().map(ExecutionProperty::getValue).collect(Collectors.toSet()))
        .append(intProperties.values().stream().map(ExecutionProperty::getValue).collect(Collectors.toSet()))
        .append(boolProperties.values().stream().map(ExecutionProperty::getValue).collect(Collectors.toSet()))
        .toHashCode();
  }
}
