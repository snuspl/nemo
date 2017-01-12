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
package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.ir.Attributes;
import edu.snu.vortex.compiler.ir.operator.Operator;
import edu.snu.vortex.runtime.common.RAttributes;
import edu.snu.vortex.runtime.common.ROperator;

import java.util.HashMap;
import java.util.Map;

public class OperatorCompiler {
  public ROperator convert(final Operator irOp) {
    final Map<Attributes.Key, Attributes.Val> irOpAttributes = irOp.getAttributes();

    final Map<RAttributes.ROpAttribute, Object> rOpAttributes = new HashMap<>();
    for (Map.Entry<Attributes.Key, Attributes.Val> attr : irOpAttributes.entrySet()) {
      switch (attr.getKey()) {
      case Placement:
        if (attr.getValue() == Attributes.Placement.Transient) {
          rOpAttributes.put(RAttributes.ROpAttribute.RESOURCE_TYPE, RAttributes.Resource_Type.TRANSIENT);
        } else if (attr.getValue() == Attributes.Placement.Reserved) {
          rOpAttributes.put(RAttributes.ROpAttribute.RESOURCE_TYPE, RAttributes.Resource_Type.RESERVED);
        } else if (attr.getValue() == Attributes.Placement.Compute) {
          rOpAttributes.put(RAttributes.ROpAttribute.RESOURCE_TYPE, RAttributes.Resource_Type.COMPUTE);
        } else if (attr.getValue() == Attributes.Placement.Storage) {
          rOpAttributes.put(RAttributes.ROpAttribute.RESOURCE_TYPE, RAttributes.Resource_Type.STORAGE);
        }
        break;
      case EdgePartitioning:
        if (attr.getValue() == Attributes.EdgePartitioning.Hash) {
          rOpAttributes.put(RAttributes.ROpAttribute.PARTITION, RAttributes.Partition.HASH);
        } else if (attr.getValue() == Attributes.EdgePartitioning.Range) {
          rOpAttributes.put(RAttributes.ROpAttribute.PARTITION, RAttributes.Partition.RANGE);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported operator attribute");
      }
    }
    final ROperator rOp = new ROperator(irOp.getId(), rOpAttributes);
    return rOp;
  }

  public String convertId(final String irOpId) {
    return irOpId;
  }
}
