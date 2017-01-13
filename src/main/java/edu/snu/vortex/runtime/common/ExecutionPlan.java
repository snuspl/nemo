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
package edu.snu.vortex.runtime.common;

import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.operator.Operator;
import edu.snu.vortex.runtime.exception.NoSuchRStageException;

import java.util.*;
import java.util.function.Consumer;

public class ExecutionPlan {
  /**
   * A list of {@link RtStage} to be executed in this plan, sorted in topological order.
   */
  private final List<RtStage> rtStages;

  /**
   * Map of <ID, {@link RtStageLink}> connecting the {@link ExecutionPlan#rtStages} contained in this plan.
   */
  private final Map<String, RtStageLink> rtStageLinks;

  /**
   * An execution plan for Vortex runtime.
   */
  public ExecutionPlan() {
    this.rtStages = new LinkedList<>();
    this.rtStageLinks = new HashMap<>();
  }

  /**
   * Adds a {@link RtStage} to this plan. Important! The {@param rtStage} must be added in the order of execution.
   * @param rtStage to be added
   */
  public void addRStage(final RtStage rtStage) {
    rtStages.add(rtStage);
  }

  /**
   * Connects two {@link RtStage} in the plan.
   * There can be multiple {@link RtOpLink} in a unique {@link RtStageLink} connecting the two stages.
   * @param srcRtStage
   * @param dstRtStage
   * @param rtOpLink that connects two {@link RtOperator} each in {@param srcRtStage} and {@param dstRtStage}.
   * @throws NoSuchRStageException when any of the {@param srcRtStage} and {@param dstRtStage} are not yet in the plan.
   */
  public void connectRStages(final RtStage srcRtStage,
                             final RtStage dstRtStage,
                             final RtOpLink rtOpLink) throws NoSuchRStageException {
    if (!rtStages.contains(srcRtStage) || !rtStages.contains(dstRtStage)) {
      throw new NoSuchRStageException("The requested RtStage does not exist in this ExecutionPlan");
    }

    final String rStageLinkId = IdGenerator.generateRtStageLinkId(srcRtStage.getId(), dstRtStage.getId());
    RtStageLink rtStageLink = rtStageLinks.get(rStageLinkId);

    if (rtStageLink == null) {
      rtStageLink = new RtStageLink(rStageLinkId, srcRtStage, dstRtStage);
      rtStageLinks.put(rStageLinkId, rtStageLink);
    }
    rtStageLink.addROpLink(rtOpLink);

    srcRtStage.addOutputRtStageLink(rtStageLink);
    dstRtStage.addInputRtStageLink(rtStageLink);
  }

  public List<RtStage> getRtStages() {
    return rtStages;
  }

  public Map<String, RtStageLink> getRtStageLinks() {
    return rtStageLinks;
  }

  public void print() {
    rtStages.forEach(rtStage -> rtStage.print());
  }
}
