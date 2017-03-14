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
package edu.snu.vortex.runtime.common.example;

import edu.snu.vortex.attributes.Attributes;
import edu.snu.vortex.attributes.AttributesMap;
import edu.snu.vortex.runtime.common.*;
import edu.snu.vortex.runtime.exception.NoSuchRtStageException;

/**
 * Simple Execution Plan.
 */
public final class SimpleExecutionPlan {
  private SimpleExecutionPlan() {
  }

  public static void main(final String[] args) {
    final ExecutionPlan simplePlan = new ExecutionPlan();

    /** A simple Execution Plan composed of 3 stages, Stage A and B independent of each other,
     * while Stage C depends on both A and B.
     * Operator a2 is connected to Operator b1 and Operator c1.
     */

    // Make Stage A
    final AttributesMap stageAattr = new AttributesMap();
    stageAattr.put(Attributes.IntegerKey.Parallelism, 3);
    final RtStage a = new RtStage(stageAattr);

    final String mockIrOpIdA1 = "a1";
    final AttributesMap rtOpA1attr = new AttributesMap();
    rtOpA1attr.put(Attributes.Key.EdgePartitioning, Attributes.Hash);
    rtOpA1attr.put(Attributes.Key.Placement, Attributes.Transient);
    final RtOperator a1 = new RtOperator(mockIrOpIdA1, rtOpA1attr);

    final String mockIrOpIdA2 = "a2";
    final AttributesMap rtOpA2attr = new AttributesMap();
    rtOpA2attr.put(Attributes.Key.EdgePartitioning, Attributes.Range);
    rtOpA2attr.put(Attributes.Key.Placement, Attributes.Reserved);
    final RtOperator a2 = new RtOperator(mockIrOpIdA2, rtOpA2attr);

    a.addRtOp(a1);
    a.addRtOp(a2);

    final AttributesMap rtOpLinkA12attr = new AttributesMap();
    rtOpLinkA12attr.put(Attributes.Key.EdgeChannel, Attributes.Memory);
    rtOpLinkA12attr.put(Attributes.Key.CommunicationPattern, Attributes.OneToOne);
    final RtOpLink a1Toa2 = new RtOpLink(a1, a2, rtOpLinkA12attr);
    a.connectRtOps(a1.getId(), a2.getId(), a1Toa2);

    // Make Stage B
    final AttributesMap stageBattr = new AttributesMap();
    stageBattr.put(Attributes.IntegerKey.Parallelism, 2);
    final RtStage b = new RtStage(stageBattr);

    final String mockIrOpIdB1 = "b1";
    final AttributesMap rtOpB1attr = new AttributesMap();
    rtOpB1attr.put(Attributes.Key.EdgePartitioning, Attributes.Hash);
    rtOpB1attr.put(Attributes.Key.Placement, Attributes.Transient);
    final RtOperator b1 = new RtOperator(mockIrOpIdB1, rtOpB1attr);

    b.addRtOp(b1);

    // Make Stage C
    final AttributesMap stageCattr = new AttributesMap();
    stageCattr.put(Attributes.IntegerKey.Parallelism, 4);
    final RtStage c = new RtStage(stageCattr);

    final String mockIrOpIdC1 = "c1";
    final AttributesMap rtOpC1attr = new AttributesMap();
    rtOpC1attr.put(Attributes.Key.EdgePartitioning, Attributes.Hash);
    rtOpC1attr.put(Attributes.Key.Placement, Attributes.Transient);
    final RtOperator c1 = new RtOperator(mockIrOpIdC1, rtOpC1attr);

    c.addRtOp(c1);

    final AttributesMap rtOpLinkA2B1attr = new AttributesMap();
    rtOpLinkA2B1attr.put(Attributes.Key.EdgeChannel, Attributes.File);
    rtOpLinkA2B1attr.put(Attributes.Key.CommunicationPattern, Attributes.ScatterGather);
    final RtOpLink a2Toc1 = new RtOpLink(a2, c1, rtOpLinkA2B1attr);

    final AttributesMap rtOpLinkA2C1attr = new AttributesMap();
    rtOpLinkA2C1attr.put(Attributes.Key.EdgeChannel, Attributes.File);
    rtOpLinkA2C1attr.put(Attributes.Key.CommunicationPattern, Attributes.ScatterGather);
    final RtOpLink b1Toc1 = new RtOpLink(b1, c1, rtOpLinkA2C1attr);

    // Add stages to the execution plan
    simplePlan.addRtStage(a);
    simplePlan.addRtStage(b);
    simplePlan.addRtStage(c);

    // Connect the stages with links a2_c1 and b1_c1
    try {
      simplePlan.connectRtStages(a, c, a2Toc1);
      simplePlan.connectRtStages(b, c, b1Toc1);
    } catch (final NoSuchRtStageException ex) {
    }
  }
}
