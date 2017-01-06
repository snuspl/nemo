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
package edu.snu.vortex.compiler.ir.component;

import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.IdManager;

import java.io.Serializable;

public final class Stage implements Serializable {
  private final String id;
  private final DAG dag;

  public Stage(final DAG dag) {
    this.id = IdManager.newStageId();
    this.dag = dag;
  }

  public String getId() {
    return id;
  }

  public DAG getDAG() {
    return dag;
  }

  public boolean contains(Operator operator) {
    return this.getDAG().contains(operator);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("STAGE " + this.getId() + "\n");
    sb.append(dag.toString());
    sb.append("END OF STAGE " + this.getId() + "\n");
    return sb.toString();
  }
}
