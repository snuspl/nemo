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

import edu.snu.vortex.runtime.exception.NoSuchRtStageException;
import edu.snu.vortex.utils.DAG;
import edu.snu.vortex.utils.DAGImpl;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Execution Plan.
 */
public final class ExecutionPlan {
  private static final Logger LOG = Logger.getLogger(ExecutionPlan.class.getName());

  /**
   * A list of {@link RuntimeStage} to be executed in this plan, sorted in topological order.
   */
  private final DAG<RuntimeStage> rtStages;

  /**
   * Map of <ID, {@link RtStageLink}> connecting the {@link ExecutionPlan#rtStages} contained in this plan.
   */
  private final Map<String, RtStageLink> rtStageLinks;

  /**
   * An execution plan for Vortex runtime.
   */
  public ExecutionPlan() {
    this.rtStages = new DAGImpl<>();
    this.rtStageLinks = new HashMap<>();
  }

  /**
   * Adds a {@link RuntimeStage} to this plan.
   * @param runtimeStage to be added
   */
  public void addRtStage(final RuntimeStage runtimeStage) {
    if (!rtStages.addVertex(runtimeStage)) {
      LOG.log(Level.FINE, "RuntimeStage {0} already exists", runtimeStage.getId());
    }
  }

  /**
   * Connects two {@link RuntimeStage} in the plan.
   * There can be multiple {@link RtOpLink} in a unique {@link RtStageLink} connecting the two stages.
   * @param srcRuntimeStage .
   * @param dstRuntimeStage .
   * @param rtOpLink that connects two {@link RtOperator} each in {@param srcRuntimeStage} and {@param dstRuntimeStage}.
   */
  public void connectRtStages(final RuntimeStage srcRuntimeStage,
                              final RuntimeStage dstRuntimeStage,
                              final RtOpLink rtOpLink) {
    try {
      rtStages.addEdge(srcRuntimeStage, dstRuntimeStage);
    } catch (final NoSuchElementException e) {
      throw new NoSuchRtStageException("The requested RuntimeStage does not exist in this ExecutionPlan");
    }

    final String rtStageLinkId = IdGenerator.generateRtStageLinkId(srcRuntimeStage.getId(), dstRuntimeStage.getId());
    RtStageLink rtStageLink = rtStageLinks.get(rtStageLinkId);

    if (rtStageLink == null) {
      rtStageLink = new RtStageLink(rtStageLinkId, srcRuntimeStage, dstRuntimeStage);
      rtStageLinks.put(rtStageLinkId, rtStageLink);
    }
    rtStageLink.addRtOpLink(rtOpLink);

    srcRuntimeStage.addOutputRtStageLink(rtStageLink);
    dstRuntimeStage.addInputRtStageLink(rtStageLink);
  }

  public Set<RuntimeStage> getNextRtStagesToExecute() {
    return rtStages.getRootVertices();
  }

  public boolean removeCompleteStage(final RuntimeStage runtimeStageToRemove) {
    return rtStages.removeVertex(runtimeStageToRemove);
  }

  public Map<String, RtStageLink> getRtStageLinks() {
    return rtStageLinks;
  }

  @Override
  public String toString() {
    return "RtStages: " + rtStages + " / RtStageLinks: " + rtStageLinks;
  }
}
