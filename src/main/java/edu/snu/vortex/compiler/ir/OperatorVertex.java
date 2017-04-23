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


import javax.annotation.Nullable;

/**
 * IRVertex that transforms input data.
 * It is to be constructed in the compiler frontend with language-specific data transform logic.
 */
public final class OperatorVertex extends IRVertex {
  private final Transform transform;
  private final LoopVertex assignedLoopVertex; // nullable

  public OperatorVertex(final Transform t) {
    this(t, null);
  }

  public OperatorVertex(final Transform t, @Nullable final LoopVertex assignedLoopVertex) {
    super();
    this.transform = t;
    this.assignedLoopVertex = assignedLoopVertex;
    if (this.assignedLoopVertex != null) {
      this.assignedLoopVertex.getBuilder().addVertex(this);
    }
  }

  public Boolean isComposite() {
    return assignedLoopVertex != null;
  }

  @Nullable
  public LoopVertex getAssignedLoopVertex() {
    return assignedLoopVertex;
  }

  public Transform getTransform() {
    return transform;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append(", transform: ");
    sb.append(transform);
    return sb.toString();
  }
}
