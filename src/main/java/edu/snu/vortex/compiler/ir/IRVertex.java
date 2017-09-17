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
package edu.snu.vortex.compiler.ir;

import edu.snu.vortex.compiler.ir.attribute.AttributeMap;
import edu.snu.vortex.common.dag.Vertex;
import edu.snu.vortex.compiler.ir.attribute.ExecutionFactor;

/**
 * The top-most wrapper for a user operation in the Vortex IR.
 */
public abstract class IRVertex extends Vertex {
  private final AttributeMap attributes;

  /**
   * Constructor of IRVertex.
   */
  public IRVertex() {
    super(IdManager.newVertexId());
    this.attributes = AttributeMap.of(this);
  }

  /**
   * @return a clone elemnt of the IRVertex.
   */
  public abstract IRVertex getClone();

  /**
   * Static function to copy attributes from a vertex to the other.
   * @param fromVertex the edge to copy attributes from.
   * @param toVertex the edge to copy attributes to.
   */
  public static void copyAttributes(final IRVertex fromVertex, final IRVertex toVertex) {
    fromVertex.getAttributes().forEachAttr(toVertex::setAttr);
  }

  /**
   * Set an attribute to the IRVertex.
   * @param executionFactor new execution factor.
   * @return the IRVertex with the attribute applied.
   */
  public final IRVertex setAttr(final ExecutionFactor<?> executionFactor) {
    attributes.put(executionFactor);
    return this;
  }

  /**
   * Get the attribute of the IRVertex.
   * @param executionFactorType type of the execution factor.
   * @return the attribute.
   */
  public final Object getAttr(final ExecutionFactor.Type executionFactorType) {
    return attributes.get(executionFactorType);
  }
  public final String getStringAttr(final ExecutionFactor.Type executionFactorType) {
    return attributes.getStringAttr(executionFactorType);
  }
  public final Integer getIntegerAttr(final ExecutionFactor.Type executionFactorType) {
    return attributes.getIntegerAttr(executionFactorType);
  }
  public final Boolean getBooleanAttr(final ExecutionFactor.Type executionFactorType) {
    return attributes.getBooleanAttr(executionFactorType);
  }

  /**
   * @return the AttributeMap of the IRVertex.
   */
  public final AttributeMap getAttributes() {
    return attributes;
  }

  /**
   * @return IRVertex properties in String form.
   */
  protected final String irVertexPropertiesToString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("\"class\": \"").append(this.getClass().getSimpleName());
    sb.append("\", \"attributes\": ").append(attributes);
    return sb.toString();
  }
}
