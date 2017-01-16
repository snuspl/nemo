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
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExecutionPlan {
  private static final Logger LOG = Logger.getLogger(ExecutionPlan.class.getName());

  /**
   * A list of {@link RtStage} to be executed in this plan, sorted in topological order.
   */
  private final DAG<RtStage> rtStages;

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
   * Adds a {@link RtStage} to this plan. Important! The {@param rtStage} must be added in the order of execution.
   * @param rtStage to be added
   */
  public void addRStage(final RtStage rtStage) {
    if (!rtStages.addVertex(rtStage)) {
      LOG.log(Level.FINE, "RtStage {0} already exists", rtStage.getId());
    }
  }

  /**
   * Connects two {@link RtStage} in the plan.
   * There can be multiple {@link RtOpLink} in a unique {@link RtStageLink} connecting the two stages.
   * @param srcRtStage
   * @param dstRtStage
   * @param rtOpLink that connects two {@link RtOperator} each in {@param srcRtStage} and {@param dstRtStage}.
   * @throws NoSuchRtStageException when any of the {@param srcRtStage} and {@param dstRtStage} are not yet in the plan.
   */
  public void connectRtStages(final RtStage srcRtStage,
                              final RtStage dstRtStage,
                              final RtOpLink rtOpLink) throws NoSuchRtStageException {
    try {
      rtStages.addEdge(srcRtStage, dstRtStage);
    } catch (final NoSuchElementException e) {
      throw new NoSuchRtStageException("The requested RtStage does not exist in this ExecutionPlan");
    }

    final String rStageLinkId = IdGenerator.generateRtStageLinkId(srcRtStage.getId(), dstRtStage.getId());
    RtStageLink rtStageLink = rtStageLinks.get(rStageLinkId);

    if (rtStageLink == null) {
      rtStageLink = new RtStageLink(rStageLinkId, srcRtStage, dstRtStage);
      rtStageLinks.put(rStageLinkId, rtStageLink);
    }
    rtStageLink.addRtOpLink(rtOpLink);

    srcRtStage.addOutputRtStageLink(rtStageLink);
    dstRtStage.addInputRtStageLink(rtStageLink);
  }

  public Set<RtStage> getNextRtStagesToExecute() {
    return rtStages.getRootVertices();
  }

  public boolean removeCompleteStage(final RtStage rtStageToRemove) {
    return rtStages.removeVertex(rtStageToRemove);
  }


  public List<RtStage> getRtStages() {
//    return rtStages;
    return null;
  }

  public Map<String, RtStageLink> getRtStageLinks() {
    return rtStageLinks;
  }

  public void print() {
    //TODO: print components of this execution grach in DFS gragh traversal.
//    doDFS(rtStages, (rtStage -> rtStage.print()), VisitOrder.PreOrder);
  }

  ////////// DFS Traversal
  public enum VisitOrder {
    PreOrder,
    PostOrder
  }

  public static void doDFS(final List<RtStage> stages,
                           final Consumer<RtStage> function,
                           final VisitOrder visitOrder) {
    final HashSet<RtStage> visited = new HashSet<>();
    stages.stream().filter(stage -> stage.getInputLinks().size() == 0)
        .filter(source -> !visited.contains(source))
        .forEach(source -> visit(source, function, visitOrder, visited));
  }

  private static void visit(final RtStage rtStage,
                            final Consumer<RtStage> rtStageConsumer,
                            final VisitOrder visitOrder,
                            final HashSet<RtStage> visited) {
    visited.add(rtStage);
    if (visitOrder == VisitOrder.PreOrder) {
      rtStageConsumer.accept(rtStage);
    }
    final Map<String, RtStageLink> outRtStageLinkMap = rtStage.getOutputLinks();
    final List<RtStageLink> outRtStageLinks = new ArrayList<>();
    outRtStageLinkMap.forEach((id, rtStagelink) -> outRtStageLinks.add(rtStagelink));

    if (outRtStageLinks.size() != 0) {
      outRtStageLinks.stream()
          .map(outRtStageLink -> outRtStageLink.getDstStage())
          .filter(dstRtStage -> !visited.contains(dstRtStage))
          .forEach(outOperator -> visit(outOperator, rtStageConsumer, visitOrder, visited));
    }

    if (visitOrder == VisitOrder.PostOrder) {
      rtStageConsumer.accept(rtStage);
    }
  }
}
